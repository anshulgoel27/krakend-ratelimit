package gin

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/luraproject/lura/v2/config"
	"github.com/luraproject/lura/v2/logging"
	"github.com/luraproject/lura/v2/proxy"
	krakendgin "github.com/luraproject/lura/v2/router/gin"

	krakendrate "github.com/krakendio/krakend-ratelimit/v3"
	"github.com/krakendio/krakend-ratelimit/v3/router"
)

// HandlerFactory is the out-of-the-box basic ratelimit handler factory using the default krakend endpoint
// handler for the gin router
var HandlerFactory = NewRateLimiterMw(logging.NoOp, krakendgin.EndpointHandler)

// NewRateLimiterMw builds a rate limiting wrapper over the received handler factory.
func NewRateLimiterMw(logger logging.Logger, next krakendgin.HandlerFactory) krakendgin.HandlerFactory {
	return func(remote *config.EndpointConfig, p proxy.Proxy) gin.HandlerFunc {
		logPrefix := "[ENDPOINT: " + remote.Endpoint + "][Ratelimit]"
		handlerFunc := next(remote, p)

		cfg, err := router.RateLimtingConfigGetter(remote.ExtraConfig)
		if err != nil {
			if err != router.ErrNoExtraCfg {
				logger.Error(logPrefix, err)
			}
			return handlerFunc
		}

		return RateLimiterWrapperFromCfg(logger, logPrefix, cfg, handlerFunc)
	}
}

func NewTriredRateLimiterMw(logger logging.Logger, next krakendgin.HandlerFactory) krakendgin.HandlerFactory {
	return func(remote *config.EndpointConfig, p proxy.Proxy) gin.HandlerFunc {
		logPrefix := "[ENDPOINT: " + remote.Endpoint + "][TieredRatelimit]"
		handlerFunc := next(remote, p)

		cfg, err := router.TieredRateLimtingConfigGetter(remote.ExtraConfig)
		if err != nil {
			if err != router.ErrNoExtraCfg {
				logger.Error(logPrefix, err)
			}
			return handlerFunc
		}

		return TieredRateLimiterWrapperFromCfg(logger, logPrefix, cfg, handlerFunc)
	}
}

// User struct to hold rate limiter
type TieredLimiter struct {
	Store          krakendrate.LimiterStore
	TokenExtractor TokenExtractor
}

func TieredRateLimiterWrapperFromCfg(logger logging.Logger, logPrefix string, cfg router.TieredRateLimitConfig,
	handler gin.HandlerFunc,
) gin.HandlerFunc {
	if len(cfg.Tiers) <= 0 {
		return handler
	}

	if cfg.TierKey == "" {
		return handler
	}

	limiters := map[string]*TieredLimiter{}
	for _, tier := range cfg.Tiers {
		if tier.TierValue != "" && tier.RateLimit.ClientMaxRate > 0 {
			if tier.RateLimit.ClientCapacity == 0 {
				if tier.RateLimit.MaxRate < 1 {
					tier.RateLimit.ClientCapacity = 1
				} else {
					tier.RateLimit.ClientCapacity = uint64(tier.RateLimit.ClientMaxRate)
				}
			}

			tokenExtractor, err := TokenExtractorFromCfg(tier.RateLimit)
			if err != nil {
				logger.Warning(logPrefix, "Unknown strategy", tier.RateLimit.Strategy)
				return handler
			}
			logger.Debug(logPrefix,
				fmt.Sprintf("Rate limit enabled. Strategy: %s (key: %s), MaxRate: %f, Capacity: %d",
					tier.RateLimit.Strategy, tier.RateLimit.Key, tier.RateLimit.ClientMaxRate, tier.RateLimit.ClientCapacity))

			store := router.StoreFromCfg(tier.RateLimit)
			limiters[strings.ToLower(tier.TierValue)] = &TieredLimiter{Store: store, TokenExtractor: tokenExtractor}
		}
	}

	return NewTieredTokenLimiterMw(cfg.TierKey, limiters)(handler)
}

func RateLimiterWrapperFromCfg(logger logging.Logger, logPrefix string, cfg router.RateLimitingConfig,
	handler gin.HandlerFunc,
) gin.HandlerFunc {
	return applyClientRateLimit(logger, logPrefix, cfg,
		applyGlobalRateLimit(logger, logPrefix, cfg, handler))
}

func applyGlobalRateLimit(logger logging.Logger, logPrefix string, cfg router.RateLimitingConfig,
	handler gin.HandlerFunc,
) gin.HandlerFunc {
	if cfg.MaxRate <= 0 {
		return handler
	}

	if cfg.Capacity == 0 {
		if cfg.MaxRate < 1 {
			cfg.Capacity = 1
		} else {
			cfg.Capacity = uint64(cfg.MaxRate)
		}
	}

	logger.Debug(logPrefix, fmt.Sprintf("Rate limit enabled. MaxRate: %f, Capacity: %d", cfg.MaxRate, cfg.Capacity))
	return NewEndpointRateLimiterMw(krakendrate.NewTokenBucket(cfg.MaxRate, cfg.Capacity))(handler)
}

func applyClientRateLimit(logger logging.Logger, logPrefix string, cfg router.RateLimitingConfig,
	handler gin.HandlerFunc,
) gin.HandlerFunc {
	if cfg.ClientMaxRate <= 0 {
		return handler
	}
	if cfg.ClientCapacity == 0 {
		if cfg.MaxRate < 1 {
			cfg.ClientCapacity = 1
		} else {
			cfg.ClientCapacity = uint64(cfg.ClientMaxRate)
		}
	}

	tokenExtractor, err := TokenExtractorFromCfg(cfg)
	if err != nil {
		logger.Warning(logPrefix, "Unknown strategy", cfg.Strategy)
		return handler
	}
	logger.Debug(logPrefix,
		fmt.Sprintf("Rate limit enabled. Strategy: %s (key: %s), MaxRate: %f, Capacity: %d",
			cfg.Strategy, cfg.Key, cfg.ClientMaxRate, cfg.ClientCapacity))
	store := router.StoreFromCfg(cfg)

	return NewTokenLimiterMw(tokenExtractor, store)(handler)
}

// EndpointMw is a function that decorates the received handlerFunc with some rateliming logic
type EndpointMw func(gin.HandlerFunc) gin.HandlerFunc

// NewEndpointRateLimiterMw creates a simple ratelimiter for a given handlerFunc
func NewEndpointRateLimiterMw(tb *krakendrate.TokenBucket) EndpointMw {
	return func(next gin.HandlerFunc) gin.HandlerFunc {
		return func(c *gin.Context) {
			if !tb.Allow() {
				c.AbortWithError(503, krakendrate.ErrLimited)
				return
			}
			next(c)
		}
	}
}

// NewHeaderLimiterMw creates a token ratelimiter using the value of a header as a token
//
// Deprecated: Use NewHeaderLimiterMwFromCfg instead
func NewHeaderLimiterMw(header string, maxRate float64, capacity uint64) EndpointMw {
	return NewTokenLimiterMw(HeaderTokenExtractor(header), krakendrate.NewLimiterStore(maxRate, int(capacity),
		krakendrate.DefaultShardedMemoryBackend(context.Background())))
}

// NewHeaderLimiterMwFromCfg creates a token ratelimiter using the value of a header as a token
func NewHeaderLimiterMwFromCfg(cfg router.RateLimitingConfig) EndpointMw {
	store := router.StoreFromCfg(cfg)
	tokenExtractor := HeaderTokenExtractor(cfg.Key)
	return NewTokenLimiterMw(tokenExtractor, store)
}

// NewIpLimiterMw creates a token ratelimiter using the IP of the request as a token
func NewIpLimiterMw(maxRate float64, capacity uint64) EndpointMw {
	return NewTokenLimiterMw(IPTokenExtractor, krakendrate.NewLimiterStore(maxRate, int(capacity),
		krakendrate.DefaultShardedMemoryBackend(context.Background())))
}

// NewIpLimiterWithKeyMw creates a token ratelimiter using the IP of the request as a token
//
// Deprecated: Use NewIpLimiterWithKeyMwFromCfg instead
func NewIpLimiterWithKeyMw(header string, maxRate float64, capacity uint64) EndpointMw {
	tokenExtractor := NewIPTokenExtractor(header)
	return NewTokenLimiterMw(tokenExtractor, krakendrate.NewLimiterStore(maxRate, int(capacity),
		krakendrate.DefaultShardedMemoryBackend(context.Background())))
}

// NewIpLimiterWithKeyMwFromCfg creates a token ratelimiter using the IP of the request as a token
func NewIpLimiterWithKeyMwFromCfg(cfg router.RateLimitingConfig) EndpointMw {
	store := router.StoreFromCfg(cfg)
	tokenExtractor := NewIPTokenExtractor(cfg.Key)
	return NewTokenLimiterMw(tokenExtractor, store)
}

// NewTokenLimiterMw returns a token based ratelimiting endpoint middleware with the received TokenExtractor and LimiterStore
func NewTokenLimiterMw(tokenExtractor TokenExtractor, limiterStore krakendrate.LimiterStore) EndpointMw {
	return func(next gin.HandlerFunc) gin.HandlerFunc {
		return func(c *gin.Context) {
			tokenKey := tokenExtractor(c)
			if tokenKey == "" {
				c.AbortWithError(http.StatusTooManyRequests, krakendrate.ErrLimited)
				return
			}
			if !limiterStore(tokenKey).Allow() {
				c.AbortWithError(http.StatusTooManyRequests, krakendrate.ErrLimited)
				return
			}
			next(c)
		}
	}
}

func NewTieredTokenLimiterMw(tier_key_header string, limiters map[string]*TieredLimiter) EndpointMw {
	return func(next gin.HandlerFunc) gin.HandlerFunc {
		return func(c *gin.Context) {
			tierValue := c.Request.Header.Get(tier_key_header)
			if tierValue == "" {
				c.AbortWithError(http.StatusBadRequest, fmt.Errorf("missing tier header: %s", tier_key_header))
				return
			}

			limiter, exists := limiters[tierValue]
			if !exists {
				c.AbortWithError(http.StatusBadRequest, fmt.Errorf("missing tier definition: %s", tierValue))
				return
			}

			tokenKey := limiter.TokenExtractor(c)
			if tokenKey == "" {
				c.AbortWithError(http.StatusTooManyRequests, krakendrate.ErrLimited)
				return
			}

			if !limiter.Store(tokenKey).Allow() {
				c.AbortWithError(http.StatusTooManyRequests, krakendrate.ErrLimited)
				return
			}
			next(c)
		}
	}
}
