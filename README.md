### Requirements
Go version: `go1.22.1`

### Steps to run
1. Download or clone the repository\
    `git clone https://github.com/vishal-s-patil/go-push-to-redis-queue.git`
2. Enter into the directory\
`cd go-push-to-redis-queue` 
3. Download the required packages\
    `go mod tidy`
4. Run the module\
    `go run push_to_redis_pipe.go <flags>`

### Flags
| FlagName       | isRequired   | Default value            |
|----------------|--------------|--------------------------|
| fileName       | true         | empty string             |
| redisQueueName | true         | empty string             |
| redisHostName  | false        | localhost                |
| redisPort      | false        | default redis port(6379) |
| batchSize      | false        | 1000                     | 
| isGzip         | false        | true                     |