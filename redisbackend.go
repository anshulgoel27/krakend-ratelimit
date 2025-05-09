package krakendrate

import (
	"context"
	"errors"
	"time"

	"github.com/go-redis/redis_rate/v10"
	"github.com/luraproject/lura/v2/config"
	"github.com/redis/go-redis/v9"
)

type RedisConfig struct {
	Clusters        []RedisCluster  `json:"clusters,omitempty"`
	ConnectionPools []RedisConnPool `json:"connection_pools,omitempty"`
}

type RedisCluster struct {
	Addresses       []string      `json:"addresses"`
	ClientName      string        `json:"client_name,omitempty"`
	ConnMaxIdleTime time.Duration `json:"conn_max_idle_time,omitempty"`
	ConnMaxLifeTime time.Duration `json:"conn_max_life_time,omitempty"`
	DialTimeout     time.Duration `json:"dial_timeout,omitempty"`
	MaxActiveConns  int           `json:"max_active_conns,omitempty"`
	MaxIdleConns    int           `json:"max_idle_conns,omitempty"`
	MaxRedirects    int           `json:"max_redirects,omitempty"`
	MaxRetries      int           `json:"max_retries,omitempty"`
	MaxRetryBackoff time.Duration `json:"max_retry_backoff,omitempty"`
	MinIdleConns    int           `json:"min_idle_conns,omitempty"`
	MinRetryBackoff time.Duration `json:"min_retry_backoff,omitempty"`
	Name            string        `json:"name"`
	Password        string        `json:"password,omitempty"`
	PoolSize        int           `json:"pool_size,omitempty"`
	PoolTimeout     time.Duration `json:"pool_timeout,omitempty"`
	UserName        string        `json:"user_name,omitempty"`
}

type RedisConnPool struct {
	Address         string        `json:"address"`
	ClientName      string        `json:"client_name,omitempty"`
	ConnMaxIdleTime time.Duration `json:"conn_max_idle_time,omitempty"`
	ConnMaxLifeTime time.Duration `json:"conn_max_life_time,omitempty"`
	DB              int           `json:"db,omitempty"`
	DialTimeout     time.Duration `json:"dial_timeout,omitempty"`
	MaxActiveConns  int           `json:"max_active_conns,omitempty"`
	MaxIdleConns    int           `json:"max_idle_conns,omitempty"`
	MaxRetries      int           `json:"max_retries,omitempty"`
	MaxRetryBackoff time.Duration `json:"max_retry_backoff,omitempty"`
	MinIdleConns    int           `json:"min_idle_conns,omitempty"`
	MinRetryBackoff time.Duration `json:"min_retry_backoff,omitempty"`
	Name            string        `json:"name"`
	Password        string        `json:"password,omitempty"`
	PoolSize        int           `json:"pool_size,omitempty"`
	PoolTimeout     time.Duration `json:"pool_timeout,omitempty"`
	UserName        string        `json:"user_name,omitempty"`
}

const Namespace_Redis = "github_com/anshulgoel27/krakend-ratelimit/redis"

var RedisZeroCfg = RedisConfig{}

var (
	ErrNoExtraCfg    = errors.New("no extra config")
	ErrWrongExtraCfg = errors.New("wrong extra config")
)

func RedisConfigGetter(e config.ExtraConfig) (*RedisConfig, error) {
	// Get the config under the Redis namespace
	v, ok := e[Namespace_Redis]
	if !ok {
		return &RedisZeroCfg, ErrNoExtraCfg
	}

	tmp, ok := v.(map[string]interface{})
	if !ok {
		return &RedisZeroCfg, ErrWrongExtraCfg
	}

	// Parse and construct the configuration
	config := constructRedisConfig(tmp)

	// Set minimum required values
	for i := range config.Clusters {
		cluster := &config.Clusters[i]

		// Set default values if they are missing
		if cluster.MaxActiveConns == 0 {
			cluster.MaxActiveConns = 10 // Minimum value
		}
		if cluster.MaxIdleConns == 0 {
			cluster.MaxIdleConns = 10 // Minimum value
		}
		if cluster.ConnMaxIdleTime == 0 {
			cluster.ConnMaxIdleTime = 30 * time.Minute // Default to 30 minutes
		}
		if cluster.ConnMaxLifeTime == 0 {
			cluster.ConnMaxLifeTime = 0 // No limit by default
		}
		if cluster.DialTimeout == 0 {
			cluster.DialTimeout = 5 * time.Second // Default to 5 seconds
		}
		if cluster.MaxRetries == 0 {
			cluster.MaxRetries = 3 // Default to 3 retries
		}
		if cluster.MaxRetryBackoff == 0 {
			cluster.MaxRetryBackoff = 512 * time.Millisecond // Default to 512ms
		}
		if cluster.MinRetryBackoff == 0 {
			cluster.MinRetryBackoff = 8 * time.Millisecond // Default to 8ms
		}
		if cluster.PoolTimeout == 0 {
			cluster.PoolTimeout = 30 * time.Second // Default to 30 seconds
		}
		if cluster.PoolSize == 0 {
			cluster.PoolSize = 10 // Default pool size
		}
	}

	for i := range config.ConnectionPools {
		connPool := &config.ConnectionPools[i]

		// Set default values for connection pools
		if connPool.MaxActiveConns == 0 {
			connPool.MaxActiveConns = 10 // Minimum value
		}
		if connPool.MaxIdleConns == 0 {
			connPool.MaxIdleConns = 10 // Minimum value
		}
		if connPool.ConnMaxIdleTime == 0 {
			connPool.ConnMaxIdleTime = 30 * time.Minute // Default to 30 minutes
		}
		if connPool.ConnMaxLifeTime == 0 {
			connPool.ConnMaxLifeTime = 0 // No limit by default
		}
		if connPool.DialTimeout == 0 {
			connPool.DialTimeout = 5 * time.Second // Default to 5 seconds
		}
		if connPool.MaxRetries == 0 {
			connPool.MaxRetries = 3 // Default to 3 retries
		}
		if connPool.MaxRetryBackoff == 0 {
			connPool.MaxRetryBackoff = 512 * time.Millisecond // Default to 512ms
		}
		if connPool.MinRetryBackoff == 0 {
			connPool.MinRetryBackoff = 8 * time.Millisecond // Default to 8ms
		}
		if connPool.PoolTimeout == 0 {
			connPool.PoolTimeout = 30 * time.Second // Default to 30 seconds
		}
		if connPool.PoolSize == 0 {
			connPool.PoolSize = 10 // Default pool size
		}
	}

	return config, nil
}

func constructRedisConfig(configMap map[string]interface{}) *RedisConfig {
	var config RedisConfig

	// Parse the clusters section
	if clusters, ok := configMap["clusters"].([]interface{}); ok {
		for _, cluster := range clusters {
			clusterMap, ok := cluster.(map[string]interface{})
			if !ok {
				continue // Skip if the cluster is not a valid map
			}

			var redisCluster RedisCluster
			// Parse the fields for each cluster
			if addresses, ok := clusterMap["addresses"].([]interface{}); ok {
				for _, addr := range addresses {
					if address, ok := addr.(string); ok {
						redisCluster.Addresses = append(redisCluster.Addresses, address)
					}
				}
			}
			if clientName, ok := clusterMap["client_name"].(string); ok {
				redisCluster.ClientName = clientName
			}
			if connMaxIdleTime, ok := clusterMap["conn_max_idle_time"].(string); ok {
				// Convert string to time.Duration
				if d, err := time.ParseDuration(connMaxIdleTime); err == nil {
					redisCluster.ConnMaxIdleTime = d
				}
			}
			if connMaxLifeTime, ok := clusterMap["conn_max_life_time"].(string); ok {
				if d, err := time.ParseDuration(connMaxLifeTime); err == nil {
					redisCluster.ConnMaxLifeTime = d
				}
			}
			if dialTimeout, ok := clusterMap["dial_timeout"].(string); ok {
				if d, err := time.ParseDuration(dialTimeout); err == nil {
					redisCluster.DialTimeout = d
				}
			}
			if maxActiveConns, ok := clusterMap["max_active_conns"].(float64); ok {
				redisCluster.MaxActiveConns = int(maxActiveConns)
			}
			if maxIdleConns, ok := clusterMap["max_idle_conns"].(float64); ok {
				redisCluster.MaxIdleConns = int(maxIdleConns)
			}
			if maxRetries, ok := clusterMap["max_retries"].(float64); ok {
				redisCluster.MaxRetries = int(maxRetries)
			}
			if maxRetryBackoff, ok := clusterMap["max_retry_backoff"].(string); ok {
				if d, err := time.ParseDuration(maxRetryBackoff); err == nil {
					redisCluster.MaxRetryBackoff = d
				}
			}
			if minRetryBackoff, ok := clusterMap["min_retry_backoff"].(string); ok {
				if d, err := time.ParseDuration(minRetryBackoff); err == nil {
					redisCluster.MinRetryBackoff = d
				}
			}
			if poolTimeout, ok := clusterMap["pool_timeout"].(string); ok {
				if d, err := time.ParseDuration(poolTimeout); err == nil {
					redisCluster.PoolTimeout = d
				}
			}
			if poolSize, ok := clusterMap["pool_size"].(float64); ok {
				redisCluster.PoolSize = int(poolSize)
			}
			if name, ok := clusterMap["name"].(string); ok {
				redisCluster.Name = name
			}
			if password, ok := clusterMap["password"].(string); ok {
				redisCluster.Password = password
			}
			if userName, ok := clusterMap["user_name"].(string); ok {
				redisCluster.UserName = userName
			}

			// Append to the config clusters slice
			config.Clusters = append(config.Clusters, redisCluster)
		}
	}

	// Parse the connection pools section
	if pools, ok := configMap["connection_pools"].([]interface{}); ok {
		for _, pool := range pools {
			poolMap, ok := pool.(map[string]interface{})
			if !ok {
				continue // Skip if the pool is not a valid map
			}

			var redisPool RedisConnPool
			// Parse the fields for each connection pool
			if address, ok := poolMap["address"].(string); ok {
				redisPool.Address = address
			}
			if clientName, ok := poolMap["client_name"].(string); ok {
				redisPool.ClientName = clientName
			}
			if connMaxIdleTime, ok := poolMap["conn_max_idle_time"].(string); ok {
				if d, err := time.ParseDuration(connMaxIdleTime); err == nil {
					redisPool.ConnMaxIdleTime = d
				}
			}
			if connMaxLifeTime, ok := poolMap["conn_max_life_time"].(string); ok {
				if d, err := time.ParseDuration(connMaxLifeTime); err == nil {
					redisPool.ConnMaxLifeTime = d
				}
			}
			if dialTimeout, ok := poolMap["dial_timeout"].(string); ok {
				if d, err := time.ParseDuration(dialTimeout); err == nil {
					redisPool.DialTimeout = d
				}
			}
			if maxActiveConns, ok := poolMap["max_active_conns"].(float64); ok {
				redisPool.MaxActiveConns = int(maxActiveConns)
			}
			if maxIdleConns, ok := poolMap["max_idle_conns"].(float64); ok {
				redisPool.MaxIdleConns = int(maxIdleConns)
			}
			if maxRetries, ok := poolMap["max_retries"].(float64); ok {
				redisPool.MaxRetries = int(maxRetries)
			}
			if maxRetryBackoff, ok := poolMap["max_retry_backoff"].(string); ok {
				if d, err := time.ParseDuration(maxRetryBackoff); err == nil {
					redisPool.MaxRetryBackoff = d
				}
			}
			if minRetryBackoff, ok := poolMap["min_retry_backoff"].(string); ok {
				if d, err := time.ParseDuration(minRetryBackoff); err == nil {
					redisPool.MinRetryBackoff = d
				}
			}
			if poolTimeout, ok := poolMap["pool_timeout"].(string); ok {
				if d, err := time.ParseDuration(poolTimeout); err == nil {
					redisPool.PoolTimeout = d
				}
			}
			if poolSize, ok := poolMap["pool_size"].(float64); ok {
				redisPool.PoolSize = int(poolSize)
			}
			if name, ok := poolMap["name"].(string); ok {
				redisPool.Name = name
			}
			if password, ok := poolMap["password"].(string); ok {
				redisPool.Password = password
			}
			if userName, ok := poolMap["user_name"].(string); ok {
				redisPool.UserName = userName
			}

			// Append to the config connection pools slice
			config.ConnectionPools = append(config.ConnectionPools, redisPool)
		}
	}

	// Return the constructed config
	return &config
}

// redisRateBackend implements the Backend interface using redis-rate.
type redisRateBackend struct {
	limiter *redis_rate.Limiter
	rate    int           // max events
	period  time.Duration // time window
}

type redisLimiter struct {
	key     string
	limiter *redis_rate.Limiter
	rate    int           // max events
	period  time.Duration // time window
}

func (b *redisRateBackend) Load(key string, _ func() interface{}) interface{} {
	return &redisLimiter{
		key:     key,
		limiter: b.limiter,
		rate:    b.rate,
		period:  b.period,
	}
}

func (b *redisRateBackend) Store(key string, _ interface{}) error {
	// No-op: redis-rate already updates the counter in Load
	return nil
}

func (l *redisLimiter) Allow() bool {
	if l.period == 24*time.Hour {
		now := time.Now()
		midnight := time.Date(now.Year(), now.Month(), now.Day()+1, 0, 0, 0, 0, now.Location())
		secondsRemaining := int(midnight.Sub(now).Seconds())
		rate := redis_rate.Limit{
			Rate:   l.rate, // total allowed
			Period: time.Duration(secondsRemaining) * time.Second,
			Burst:  l.rate, // max burst
		}
		res, err := l.limiter.Allow(context.Background(), l.key, rate)
		if err == nil {
			if res.Allowed == 0 && res.Remaining == 0 {
				return false
			}
		}

	} else {
		var rate redis_rate.Limit
		switch l.period {
		case time.Second:
			rate = redis_rate.PerSecond(l.rate)
		case time.Minute:
			rate = redis_rate.PerMinute(l.rate)
		case time.Hour:
			rate = redis_rate.PerHour(l.rate)
		default:
			rate = redis_rate.PerMinute(l.rate)
		}

		res, err := l.limiter.Allow(context.Background(), l.key, rate)
		if err == nil {
			if res.Allowed == 0 && res.Remaining == 0 {
				return false
			}
		}
	}
	return true
}

func RedisBackendBuilder(client *redis.Client, rate int, period time.Duration) Backend {
	return &redisRateBackend{
		limiter: redis_rate.NewLimiter(client),
		rate:    rate,
		period:  period,
	}
}
