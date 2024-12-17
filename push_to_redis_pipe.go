package main

import (
	"bufio"
	"compress/gzip"
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	redis "github.com/go-redis/redis/v8"
)

func ConnectToRedis(redis_host string, redis_port int) (*redis.Client, context.Context) {
	rdb := redis.NewClient(&redis.Options{
		Addr:         fmt.Sprintf("%s:%d", redis_host, redis_port),
		DB:           0, // use default DB
		PoolSize:     1000,
		DialTimeout:  10 * time.Minute, // Timeout for connection establishment
		ReadTimeout:  10 * time.Minute, // Timeout for reading data
		WriteTimeout: 10 * time.Minute, // Timeout for writing data
	})

	ctx := context.Background()
	_, err := rdb.Ping(ctx).Result()
	if err != nil {
		log.Fatalf("[Event:Error] [Msg:Failed to connect to redis] [Err:%s]", err)
		return nil, nil
	}

	return rdb, ctx
}

func ReadFile(ch chan string, wg *sync.WaitGroup, fileName string) {
	defer wg.Done()

	file, err := os.Open(fileName)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		ch <- scanner.Text()
	}

	close(ch)

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
}

func ReadFileGzip(ch chan string, wg *sync.WaitGroup, fileName string) {
	defer wg.Done()

	gzipFile, err := os.Open(fileName)
	if err != nil {
		log.Fatal(err)
	}
	defer gzipFile.Close()

	reader, err := gzip.NewReader(gzipFile)
	if err != nil {
		log.Fatal(err)
	}
	defer reader.Close()

	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		ch <- scanner.Text()
	}

	close(ch)

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
}

func push_to_redis(batchSize, redisPort int, ch chan string, wg *sync.WaitGroup, redisQueueName, redisHost string) {
	defer wg.Done()

	channelBuffer := 1000

	redisClient, ctx := ConnectToRedis(redisHost, redisPort)
	pipe := redisClient.Pipeline()
	uids := make([]string, 0, channelBuffer)

	for line := range ch {
		uid := strings.TrimSpace(line)
		uids = append(uids, uid)

		if len(uids) >= batchSize {
			for _, item := range uids {
				pipe.RPush(ctx, redisQueueName, item)
			}

			_, err := pipe.Exec(ctx)
			if err != nil {
				log.Fatalf("Error pushing to Redis: %v", err)
			}

			uids = uids[:0]
		}
	}

	if len(uids) >= batchSize {
		for _, item := range uids {
			pipe.RPush(ctx, redisQueueName, item)
		}

		_, err := pipe.Exec(ctx)
		if err != nil {
			log.Fatalf("Error pushing to Redis: %v", err)
		}
	}
}

func main() {
	fileName := flag.String("fileName", "test.txt.gz", "input file name with values to push to the redis queue")
	redisQueueName := flag.String("redisQueueName", "test", "redis queue name")
	redisHostName := flag.String("redisHostName", "localhost", "redis queue name")
	batchSize := flag.Int("batchSize", 1000, "redis pipe batch size of rpush")
	redisPort := flag.Int("redisPort", 6379, "")
	isGzip := flag.Bool("isGzip", true, "")
	flag.Parse()

	channelBuffer := 1000

	var wg sync.WaitGroup
	ch := make(chan string, channelBuffer)

	wg.Add(1)
	if *isGzip {
		go ReadFileGzip(ch, &wg, *fileName)
	} else {
		go ReadFile(ch, &wg, *fileName)
	}

	wg.Add(1)
	go push_to_redis(*batchSize, *redisPort, ch, &wg, *redisQueueName, *redisHostName)

	wg.Wait()
}
