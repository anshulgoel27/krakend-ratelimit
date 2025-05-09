package router

import (
	"context"
	"fmt"
	"log"

	krakendrate "github.com/anshulgoel27/krakend-ratelimit/v3"
	"github.com/redis/go-redis/v9"
)

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

func connectToRedis(config *krakendrate.RedisConfig, connectionName string) (*redis.Client, error) {
	// Try to match the name in clusters first
	// for _, cluster := range config.Clusters {
	// 	if cluster.Name == connectionName {
	// 		client := redis.NewClusterClient(&redis.ClusterOptions{
	// 			Addrs:           cluster.Addresses,
	// 			Password:        cluster.Password,
	// 			MaxRedirects:    cluster.MaxRedirects,
	// 			MaxRetries:      cluster.MaxRetries,
	// 			MinRetryBackoff: cluster.MinRetryBackoff,
	// 			MaxRetryBackoff: cluster.MaxRetryBackoff,
	// 			DialTimeout:     cluster.DialTimeout,
	// 			ReadTimeout:     3 * time.Second,
	// 			WriteTimeout:    3 * time.Second,
	// 		})

	// 		// Test the connection
	// 		if _, err := client.Ping(context.Background()).Result(); err != nil {
	// 			return nil, fmt.Errorf("failed to connect to Redis cluster '%s': %v", connectionName, err)
	// 		}

	// 		log.Printf("Connected to Redis cluster '%s'", connectionName)
	// 		return client, nil
	// 	}
	// }

	// If not found in clusters, check connection pools
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

			log.Printf("Connected to Redis connection pool '%s'", connectionName)
			return client, nil
		}
	}

	return nil, fmt.Errorf("no Redis connection (cluster or pool) found with name '%s'", connectionName)
}
