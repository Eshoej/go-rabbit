# go-rabbit

A Go client for RabbitMQ with support for events and tasks.

> [!WARNING]
> This is a **WORK IN PROGRESS**. It is an encoded, debugged, and AI-converted version of the original Node.js library. While it is functional, it may still have rough edges.

## Credits

This library is based on and inspired by [CoinifySoftware/node-rabbitmq](https://github.com/CoinifySoftware/node-rabbitmq). 
We aim to maintain a similar API and feature set where appropriate for Go.

## Installation

```bash
go get github.com/Eshoej/go-rabbit
```

## Usage

### Initialization

```go
package main

import (
	"log"
	gorabbit "github.com/Eshoej/go-rabbit"
)

func main() {
	config := gorabbit.DefaultConfiguration()
	config.ServiceName = "my-service"
	config.Connection.Host = "localhost"
	// ... configure other options

	// You can pass a logger that implements the Logger interface
	// or nil to use a no-op logger.
	rabbit, err := gorabbit.NewGoRabbit(config, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer rabbit.Shutdown(0)

	// Ensure connection is established
	if err := rabbit.CheckConnection(); err != nil {
		log.Println("RabbitMQ not connected yet")
	}
}
```

### Events

Events are published to a topic exchange.

#### Emitting an Event

```go
context := map[string]interface{}{
	"userID": "123",
}
options := gorabbit.EmitEventOptions{} // Use defaults

_, err := rabbit.EmitEvent("user.created", context, options)
if err != nil {
	log.Printf("Failed to emit event: %v", err)
}
```

#### Consuming an Event

```go
_, err := rabbit.RegisterEventConsumer("my-service.user.created", func(ctx any, msg any) error {
	log.Printf("Received event: %v with context: %v", msg, ctx)
	return nil
}, nil)
```

### Tasks

Tasks are operations that need to be performed, potentially with retries or delays.

#### Enqueuing a Task

```go
context := map[string]interface{}{
	"orderID": "456",
}
options := map[string]any{
    "delayMillis": 5000, // Optional delay
}

_, err := rabbit.EnqueueTask("process-order", context, options)
if err != nil {
	log.Printf("Failed to enqueue task: %v", err)
}
```

#### Consuming a Task

```go
_, err := rabbit.RegisterTaskConsumer("process-order", func(ctx any, msg any) error {
	log.Printf("Processing task: %v", msg)
	return nil
}, nil)
```

### Failed Messages

You can register a consumer to handle messages that have failed processing (dead-lettered).

```go
_, err := rabbit.RegisterFailedMessageConsumer(func(queueName string, msg any) error {
	log.Printf("Message failed from queue %s: %v", queueName, msg)
	return nil
}, nil)
```

## License

MIT
