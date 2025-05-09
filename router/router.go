/*
Package router provides several rate-limit routers.

The ratelimit package provides an efficient token bucket implementation. See http://en.wikipedia.org/wiki/Token_bucket for more details.
*/
package router

import (
	"errors"
	"fmt"
	"math/rand"
	"time"

	krakendrate "github.com/anshulgoel27/krakend-ratelimit/v3"
	"github.com/luraproject/lura/v2/config"
)

// Namespace is the key to use to store and access the custom config data for the router
const Namespace = "github_com/anshulgoel27/krakend-ratelimit/router"

// RateLimitingConfig is the custom config struct containing the params for the router middlewares
type RateLimitingConfig struct {
	MaxRate             float64       `json:"max_rate"`
	Capacity            uint64        `json:"capacity"`
	Strategy            string        `json:"strategy"`
	ClientMaxRate       float64       `json:"client_max_rate"`
	ClientCapacity      uint64        `json:"client_capacity"`
	Key                 string        `json:"key"`
	TTL                 time.Duration `json:"every"`
	NumShards           uint64        `json:"num_shards"`
	CleanUpPeriod       time.Duration `json:"cleanup_period"`
	CleanUpThreads      uint64        `json:"cleanup_threads"`
	RedisConnectionName string        `json:"redis_connection_name"`
}

const Namespace_Triered = "github_com/anshulgoel27/krakend-ratelimit/tiered"

type TieredRateLimitConfig struct {
	TierKey string `json:"tier_key"` // The header name containing the tier name
	Tiers   []Tier `json:"tiers"`    // The list of all tier definitions and limits
}

type Tier struct {
	RateLimits []RateLimitingConfig `json:"ratelimits"` // The rate limit definition
	TierValue  string               `json:"tier_value"` // The tier name
}

// RateLimitingZeroCfg is the zero value for the Config struct
var RateLimitingZeroCfg = RateLimitingConfig{}
var TieredRateLimitingZeroCfg = TieredRateLimitConfig{}

var (
	ErrNoExtraCfg    = errors.New("no extra config")
	ErrWrongExtraCfg = errors.New("wrong extra config")
)

func TieredRateLimtingConfigGetter(e config.ExtraConfig) (TieredRateLimitConfig, error) {
	v, ok := e[Namespace_Triered]
	if !ok {
		return TieredRateLimitingZeroCfg, ErrNoExtraCfg
	}
	tmp, ok := v.(map[string]interface{})
	if !ok {
		return TieredRateLimitingZeroCfg, ErrWrongExtraCfg
	}

	return ConstructTieredRateLimitConfig(tmp), nil
}

// ConstructTieredRateLimitConfig builds the complete tiered rate limiting configuration
func ConstructTieredRateLimitConfig(config map[string]interface{}) TieredRateLimitConfig {
	cfg := TieredRateLimitConfig{}

	// TierKey: Header name that contains the tier name
	if v, ok := config["tier_key"]; ok {
		cfg.TierKey = fmt.Sprintf("%v", v)
	}

	// Tiers: List of all tier definitions
	if v, ok := config["tiers"]; ok {
		tiersConfig, ok := v.([]interface{})
		if ok {
			for _, tierCfg := range tiersConfig {
				// Convert each tier config into a Tier struct
				tierConfig := tierCfg.(map[string]interface{})
				cfg.Tiers = append(cfg.Tiers, ConstructTierRateLimitingConfig(tierConfig))
			}
		}
	}

	// Return the constructed tiered rate limiting config
	return cfg
}

func ConstructTierRateLimitingConfig(config map[string]interface{}) Tier {
	tier := Tier{}

	// RateLimit Configuration for the Tier
	if v, ok := config["ratelimits"]; ok {
		if arr, ok := v.([]interface{}); ok {
			var rateLimitConfigs []RateLimitingConfig
			for _, item := range arr {
				if itemMap, ok := item.(map[string]interface{}); ok {
					rateLimitConfigs = append(rateLimitConfigs, ConstructRateLimtingConfig(itemMap))
				}
			}
			tier.RateLimits = rateLimitConfigs
		}
	}

	// TierValue: can be a literal or policy expression
	if v, ok := config["tier_value"]; ok {
		tier.TierValue = fmt.Sprintf("%v", v)
	}

	// Return the constructed tier object
	return tier
}

func ConstructRateLimtingConfig(config map[string]interface{}) RateLimitingConfig {
	cfg := RateLimitingConfig{}
	if v, ok := config["max_rate"]; ok {
		switch val := v.(type) {
		case int64:
			cfg.MaxRate = float64(val)
		case int:
			cfg.MaxRate = float64(val)
		case float64:
			cfg.MaxRate = val
		}
	}
	if v, ok := config["capacity"]; ok {
		switch val := v.(type) {
		case int64:
			cfg.Capacity = uint64(val)
		case int:
			cfg.Capacity = uint64(val)
		case float64:
			cfg.Capacity = uint64(val)
		}
	}
	if v, ok := config["strategy"]; ok {
		cfg.Strategy = fmt.Sprintf("%v", v)
	}
	if v, ok := config["client_max_rate"]; ok {
		switch val := v.(type) {
		case int64:
			cfg.ClientMaxRate = float64(val)
		case int:
			cfg.ClientMaxRate = float64(val)
		case float64:
			cfg.ClientMaxRate = val
		}
	}
	if v, ok := config["client_capacity"]; ok {
		switch val := v.(type) {
		case int64:
			cfg.ClientCapacity = uint64(val)
		case int:
			cfg.ClientCapacity = uint64(val)
		case float64:
			cfg.ClientCapacity = uint64(val)
		}
	}
	if v, ok := config["key"]; ok {
		cfg.Key = fmt.Sprintf("%v", v)
	}

	if v, ok := config["redis_connection_name"]; ok {
		cfg.RedisConnectionName = fmt.Sprintf("%v", v)
	}

	cfg.TTL = krakendrate.DataTTL
	if v, ok := config["every"]; ok {
		every, err := time.ParseDuration(fmt.Sprintf("%v", v))
		if err != nil || every < time.Second {
			every = time.Second
		}
		factor := float64(time.Second) / float64(every)
		cfg.MaxRate = cfg.MaxRate * factor
		cfg.ClientMaxRate = cfg.ClientMaxRate * factor

		if every == 24*time.Hour {
			cfg.TTL = every
		}
		if every > cfg.TTL {
			// we do not need crypto strength random number to generate some
			// jitter in the duration, so we mark it to skipcq the check:
			cfg.TTL = time.Duration(int64((1 + 0.25*rand.Float64()) * float64(every))) // skipcq: GSC-G404
		}
	}
	cfg.NumShards = krakendrate.DefaultShards
	if v, ok := config["num_shards"]; ok {
		switch val := v.(type) {
		case int64:
			cfg.NumShards = uint64(val)
		case int:
			cfg.NumShards = uint64(val)
		case float64:
			cfg.NumShards = uint64(val)
		}
	}
	cfg.CleanUpPeriod = time.Minute
	if v, ok := config["cleanup_period"]; ok {
		cr, err := time.ParseDuration(fmt.Sprintf("%v", v))
		if err != nil {
			cr = time.Minute
		}
		// we hardcode a minimum time
		if cr < time.Second {
			cr = time.Second
		}
		cfg.CleanUpPeriod = cr
	}
	cfg.CleanUpThreads = 1
	if v, ok := config["cleanup_threads"]; ok {
		switch val := v.(type) {
		case int64:
			cfg.CleanUpThreads = uint64(val)
		case int:
			cfg.CleanUpThreads = uint64(val)
		case float64:
			cfg.CleanUpThreads = uint64(val)
		}
	}

	return cfg
}

// RateLimtingConfigGetter parses the extra config for the rate adapter and
// returns a ZeroCfg and an error if something goes wrong.
func RateLimtingConfigGetter(e config.ExtraConfig) (RateLimitingConfig, error) {
	v, ok := e[Namespace]
	if !ok {
		return RateLimitingZeroCfg, ErrNoExtraCfg
	}
	tmp, ok := v.(map[string]interface{})
	if !ok {
		return RateLimitingZeroCfg, ErrWrongExtraCfg
	}
	return ConstructRateLimtingConfig(tmp), nil
}
