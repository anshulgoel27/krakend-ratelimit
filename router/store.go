package router

import (
	"context"
	"fmt"
	"log"
	"sync"

	krakendrate "github.com/anshulgoel27/krakend-ratelimit/v3"
	"github.com/redis/go-redis/v9"
)

// Assuming a global variable to store Redis clients keyed by connectionName.
var redisClients = make(map[string]*redis.Client)
var mu sync.Mutex // To avoid race conditions when accessing redisClients

func StoreFromCfg(cfg RateLimitingConfig, redisConfig *krakendrate.RedisConfig) krakendrate.LimiterStore {
	ctx := context.Background()

	if redisConfig != nil && cfg.RedisConnectionName != "" {
		client, err := connectToRedis(redisConfig, cfg.RedisConnectionName)
		if err != nil {
			log.Printf("Redis connection failed for %s: %v. Falling back to memory backend.", cfg.RedisConnectionName, err)
		} else {
			redisBackend := krakendrate.RedisBackendBuilder(client, int(cfg.ClientCapacity), cfg.TTL)
			return krakendrate.NewLimiterFromBackend(redisBackend)
		}
	}

	var storeBackend krakendrate.Backend
	if cfg.NumShards > 1 {
		storeBackend = krakendrate.NewShardedBackend(
			ctx,
			cfg.NumShards,
			cfg.TTL,
			cfg.CleanUpPeriod,
			1,
			krakendrate.PseudoFNV64a,
			krakendrate.MemoryBackendBuilder,
		)
	} else {
		storeBackend = krakendrate.MemoryBackendBuilder(ctx, cfg.TTL, cfg.CleanUpPeriod, 1, 1)[0]
	}

	return krakendrate.NewLimiterStore(cfg.ClientMaxRate, int(cfg.ClientCapacity), storeBackend)
}

// connectToRedis establishes a Redis connection using a connection pool or cluster name.
// It reuses an existing client if the connection name has been used before.
func connectToRedis(config *krakendrate.RedisConfig, connectionName string) (*redis.Client, error) {
	// Check if the client already exists for the given connectionName
	mu.Lock()
	if client, exists := redisClients[connectionName]; exists {
		mu.Unlock()
		// Test the connection to make sure it's still valid
		if _, err := client.Ping(context.Background()).Result(); err != nil {
			return nil, fmt.Errorf("failed to ping existing Redis client for connection pool '%s': %v", connectionName, err)
		}
		log.Printf("Reusing existing Redis connection pool '%s'", connectionName)
		return client, nil
	}
	mu.Unlock()

	// If not found in the pool, create a new connection.
	for _, pool := range config.ConnectionPools {
		if pool.Name == connectionName {
			client := redis.NewClient(&redis.Options{
				Addr:         pool.Address,
				Username:     pool.UserName,
				Password:     pool.Password,
				DB:           pool.DB,
				DialTimeout:  pool.DialTimeout,
				PoolSize:     pool.PoolSize,
				PoolTimeout:  pool.PoolTimeout,
				MinIdleConns: pool.MinIdleConns,
			})

			// Test the connection
			if _, err := client.Ping(context.Background()).Result(); err != nil {
				return nil, fmt.Errorf("failed to connect to Redis connection pool '%s': %v", connectionName, err)
			}

			// Store the client in the global map for future reuse
			mu.Lock()
			redisClients[connectionName] = client
			mu.Unlock()

			log.Printf("Connected to Redis connection pool '%s'", connectionName)
			return client, nil
		}
	}

	return nil, fmt.Errorf("no Redis connection (cluster or pool) found with name '%s'", connectionName)
}
