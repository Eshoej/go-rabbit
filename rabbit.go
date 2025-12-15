package gorabbit

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"reflect"
	"sync"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

// Exchanges holds the exchange names.
type Exchanges struct {
	EventsTopic string
	TasksTopic  string
	Retry       string
	Delayed     string
	Failed      string
}

// Queues holds the queue names and prefixes.
type Queues struct {
	Failed            string
	RetryPrefix       string
	DelayedTaskPrefix string
}

// ConsumerConfig holds consumer–related configuration.
type ConsumerConfig struct {
	Prefetch int
}

// PublishConfig holds publisher–related configuration.
type PublishConfig struct {
	PersistentMessages bool
}

// ConnectionConfiguration holds the parameters for connecting to GoRabbitMQ.
type ConnectionConfiguration struct {
	Protocol string // "amqp" or "amqps"
	Username string
	Password string
	Host     string
	Port     int
	VHost    string
}

// -----------------------------------------------------------------------------
// Message types
// -----------------------------------------------------------------------------

// Event represents an event message.
type Event struct {
	EventName string    `json:"eventName"`
	Context   any       `json:"context"`
	UUID      uuid.UUID `json:"uuid"`
	Time      int64     `json:"time"`
	Attempts  int       `json:"attempts"`
}

// Task represents a task message.
type Task struct {
	TaskName    string    `json:"taskName"`
	Context     any       `json:"context"`
	UUID        uuid.UUID `json:"uuid"`
	Time        int64     `json:"time"`
	Attempts    int       `json:"attempts"`
	Origin      string    `json:"origin"`
	DelayMillis int       `json:"delayMillis,omitempty"`
}

// -----------------------------------------------------------------------------
// Consumer registration types
// -----------------------------------------------------------------------------

// Consumer holds information about a registered consumer.
type Consumer struct {
	Type        string // "event", "task", "failed"
	Key         string
	ConsumerTag string
	ConsumeFn   func(context any, message any) error
	Options     any
}

// FailedMessageConsumer holds information about a registered failed message consumer.
type FailedMessageConsumer struct {
	Type        string
	Key         string
	ConsumerTag string
	ConsumeFn   func(queueName string, message any) error
	Options     any
}

// GoRabbitConfiguration holds the complete configuration for the GoRabbit client.
type GoRabbitConfiguration struct {
	ServiceName         string
	DefaultLogLevel     string
	UsePublisherConfirm bool

	Exchanges Exchanges
	Queues    Queues

	Consumer ConsumerConfig
	Publish  PublishConfig

	Connection ConnectionConfiguration
}

// DefaultConfiguration returns a configuration with sensible default values.
func DefaultConfiguration() GoRabbitConfiguration {
	return GoRabbitConfiguration{
		ServiceName:         "defaultService",
		DefaultLogLevel:     "info",
		UsePublisherConfirm: false,
		Exchanges: Exchanges{
			EventsTopic: "events.topic",
			TasksTopic:  "tasks.topic",
			Retry:       "retry.exchange",
			Delayed:     "delayed.exchange",
			Failed:      "failed.exchange",
		},
		Queues: Queues{
			Failed:            "failed",
			RetryPrefix:       "retry",
			DelayedTaskPrefix: "delayed",
		},
		Consumer: ConsumerConfig{
			Prefetch: 10,
		},
		Publish: PublishConfig{
			PersistentMessages: true,
		},
		Connection: ConnectionConfiguration{
			Protocol: "amqp",
			Username: "guest",
			Password: "guest",
			Host:     "localhost",
			Port:     5672,
			VHost:    "/",
		},
	}
}

// GoRabbit is the main client struct for interacting with RabbitMQ.
type GoRabbit struct {
	config                    GoRabbitConfiguration
	logger                    Logger
	conn                      *amqp.Connection
	connMutex                 sync.Mutex
	consumers                 []Consumer
	failedMessageConsumer     []FailedMessageConsumer
	activeMessageConsumptions []any
	isShuttingDown            bool
	channelPool               *ChannelPool
}

// GoRabbitConstructorOptions holds options for the constructor (unused in current factory, kept for future).
type GoRabbitConstructorOptions struct {
	Config GoRabbitConfiguration
	Logger Logger
}

// NewGoRabbit creates a new instance of the GoRabbit client.
func NewGoRabbit(config GoRabbitConfiguration, logger Logger) (*GoRabbit, error) {
	if config.ServiceName == "" {
		log.Fatal("ServiceName is required")
	}

	if logger == nil {
		newLogger := nopLogger{}
		logger = newLogger
	}

	rabbit := &GoRabbit{
		config: config,
		logger: logger,
	}

	rabbit.channelPool = NewChannelPool(logger, rabbit.getConnection, rabbit.onChannelOpened, rabbit.onChannelClosed)
	return rabbit, nil
}

// CheckConnection checks if the connection to RabbitMQ is active.
func (rabbit *GoRabbit) CheckConnection() error {
	if rabbit.conn == nil || rabbit.conn.IsClosed() {
		// Logger maybe
		return fmt.Errorf("no active connection to GoRabbitMQ")
	}
	return nil
}

// getConnection returns a singleton connection to GoRabbitMQ.
func (rabbit *GoRabbit) getConnection() (*amqp.Connection, error) {
	rabbit.connMutex.Lock()
	defer rabbit.connMutex.Unlock()
	if rabbit.conn != nil {
		return rabbit.conn, nil
	}
	if rabbit.isShuttingDown {
		return nil, fmt.Errorf("GoRabbitMQ module is shutting down")
	}

	url := generateConnectionURL(rabbit.config.Connection)
	rabbit.logger.Info("Opening connection to RabbitMQ")
	conn, err := amqp.Dial(url)
	if err != nil {
		rabbit.logger.Error("Error connecting to GoRabbitMQ: %v", NewField("error", err))
		return nil, err
	}
	rabbit.conn = conn
	// Set up connection error/close handling.
	go func() {
		<-conn.NotifyClose(make(chan *amqp.Error))
		rabbit.logger.Info("GoRabbitMQ connection closed")
		rabbit.connMutex.Lock()
		rabbit.conn = nil
		rabbit.connMutex.Unlock()
	}()
	go rabbit.onConnectionOpened()
	return rabbit.conn, nil
}

// onConnectionOpened is called when a connection is successfully opened.
func (rabbit *GoRabbit) onConnectionOpened() {
	rabbit.logger.Info("GoRabbitMQ connection opened")
	if len(rabbit.consumers) > 0 {
		if err := rabbit.recreateRegisteredConsumers(); err != nil {
			rabbit.logger.Error("Error recreating registered consumers: %v", NewField("error", err))
		}
	}
}

// recreateRegisteredConsumers reattaches consumers after a reconnect.
func (rabbit *GoRabbit) recreateRegisteredConsumers() error {
	consumersCopy := make([]Consumer, len(rabbit.consumers))
	copy(consumersCopy, rabbit.consumers)
	failedMessageConsumersCopy := make([]FailedMessageConsumer, len(rabbit.failedMessageConsumer))
	copy(failedMessageConsumersCopy, rabbit.failedMessageConsumer)
	rabbit.consumers = nil

	rabbit.logger.Info(fmt.Sprintf("Recreating %d registered consumers", len(consumersCopy)))
	for _, consumer := range consumersCopy {
		switch consumer.Type {
		case "event":
			_, err := rabbit.RegisterEventConsumer(consumer.Key, consumer.ConsumeFn, consumer.Options)
			if err != nil {
				return err
			}
		case "task":
			_, err := rabbit.RegisterTaskConsumer(consumer.Key, consumer.ConsumeFn, consumer.Options)
			if err != nil {
				return err
			}
		default:
			return fmt.Errorf("unknown consumer type: %s", consumer.Type)
		}
	}
	for _, consumer := range failedMessageConsumersCopy {
		_, err := rabbit.RegisterFailedMessageConsumer(consumer.ConsumeFn, consumer.Options)
		if err != nil {
			return err
		}
	}

	return nil
}

// onChannelOpened is a callback when a channel is opened.
func (rabbit *GoRabbit) onChannelOpened(ch *amqp.Channel, channelType string) error {
	rabbit.logger.Info(fmt.Sprintf("GoRabbitMQ %s channel opened", channelType))
	if channelType == "consumer" {
		if err := ch.Qos(rabbit.config.Consumer.Prefetch, 0, false); err != nil {
			rabbit.logger.Error("Error setting QoS", NewField("error", err))
		}
	}
	return nil
}

// onChannelClosed is a callback when a channel is closed.
func (rabbit *GoRabbit) onChannelClosed(channelType string, err error) error {
	if rabbit.isShuttingDown {
		rabbit.logger.Info(fmt.Sprintf("GoRabbitMQ %s channel closed", channelType))
		return nil
	}
	rabbit.logger.Error(fmt.Sprintf("GoRabbitMQ %s channel closed unexpectedly", channelType), NewField("error", err))
	if len(rabbit.consumers) == 0 {
		return nil
	}
	rabbit.connectWithBackoff()
	return nil
}

// connectWithBackoff tries to reconnect with backoff.
func (rabbit *GoRabbit) connectWithBackoff() {
	operation := func() error {
		_, err := rabbit.getConnection()
		if err != nil {
			rabbit.logger.Error("Error reconnecting", NewField("error", err))
		}
		return err
	}
	b := backoff.NewExponentialBackOff()
	b.InitialInterval = 1 * time.Second
	b.MaxInterval = 600 * time.Second
	if err := backoff.Retry(operation, b); err != nil {
		rabbit.logger.Error("Failed to reconnect", NewField("error", err))
	}
}

// publishMessage publishes a message to a given exchange and routing key.
func (rabbit *GoRabbit) publishMessage(ch *amqp.Channel, exchange, routingKey string, pubOpts amqp.Publishing) error {
	// Set persistent delivery if configured.
	if rabbit.config.Publish.PersistentMessages {
		pubOpts.DeliveryMode = amqp.Persistent
	} else {
		pubOpts.DeliveryMode = amqp.Transient
	}
	err := ch.Publish(
		exchange,
		routingKey,
		false, // mandatory
		false, // immediate
		pubOpts,
	)
	if err != nil {
		rabbit.logger.Error("Error publishing message", NewField("error", err))
	}
	return err
}

// -----------------------------------------------------------------------------
// Public methods
// -----------------------------------------------------------------------------
type EmitEventOptions struct {
	Uuid        *uuid.UUID
	Time        *time.Time
	ServiceName *string
}

// EmitEvent emits an event onto the events topic exchange.
func (rabbit *GoRabbit) EmitEvent(eventName string, context any, options EmitEventOptions) (*Event, error) {
	serviceName := rabbit.config.ServiceName
	if options.ServiceName != nil && *options.ServiceName != "" {
		serviceName = *options.ServiceName
	}

	fullEventName := serviceName + "." + eventName
	rabbit.logger.Info(fmt.Sprintf("Emitting event: %s", fullEventName))

	ch, err := rabbit.channelPool.GetPublisherChannel()
	if err != nil {
		return nil, err
	}

	exchangeName := rabbit.config.Exchanges.EventsTopic
	if err = ch.ExchangeDeclare(
		exchangeName,
		"topic",
		true,  // durable
		false, // auto-delete
		false, // internal
		false, // no-wait
		nil,
	); err != nil {
		return nil, err
	}

	time := time.Now()
	if options.Time != nil {
		time = *options.Time
	}

	uuid := uuid.New()
	if options.Uuid != nil {
		uuid = *options.Uuid
	}

	evt := &Event{
		EventName: fullEventName,
		Context:   context,
		UUID:      uuid,
		Time:      time.UnixMilli(),
		Attempts:  0,
	}
	payload, err := json.Marshal(evt)
	if err != nil {
		return nil, err
	}

	if err = rabbit.publishMessage(ch, exchangeName, fullEventName, amqp.Publishing{Body: payload}); err != nil {
		return nil, err
	}
	rabbit.logger.Info(fmt.Sprintf("Event %s emitted", fullEventName))
	return evt, nil
}

// EnqueueTask enqueues a task onto the tasks topic exchange.
func (rabbit *GoRabbit) EnqueueTask(fullTaskName string, context any, options map[string]any) (*Task, error) {
	serviceName := rabbit.config.ServiceName
	if s, ok := options["serviceName"].(string); ok && s != "" {
		serviceName = s
	}
	ch, err := rabbit.channelPool.GetPublisherChannel()
	if err != nil {
		return nil, err
	}

	delayMillis := 0
	if d, ok := options["delayMillis"].(int); ok {
		delayMillis = d
	}

	var exchangeName string
	pubOpts := amqp.Publishing{}
	if delayMillis > 0 {
		// Create a delayed exchange and queue.
		delayedRes, errDelayed := rabbit.assertDelayedTaskExchangeAndQueue(ch, delayMillis)
		if errDelayed != nil {
			return nil, errDelayed
		}
		exchangeName = delayedRes.DelayedExchangeName
		// (Note: In GoRabbitMQ Go client there is no built–in BCC header; you might simulate this via headers.)
		pubOpts.Headers = amqp.Table{"BCC": []any{delayedRes.DelayedQueueName}}
	} else {
		exchangeName = rabbit.config.Exchanges.TasksTopic
		if err = ch.ExchangeDeclare(
			exchangeName,
			"topic",
			true,
			false,
			false,
			false,
			nil,
		); err != nil {
			return nil, err
		}
	}

	rabbit.logger.Info(fmt.Sprintf("Enqueuing task: %s", fullTaskName))
	task := &Task{
		TaskName: fullTaskName,
		Context:  context,
		UUID:     uuid.New(),
		Time:     time.Now().UnixMilli(),
		Attempts: 0,
		Origin:   serviceName,
	}
	if delayMillis > 0 {
		task.DelayMillis = delayMillis
	}
	payload, err := json.Marshal(task)
	if err != nil {
		return nil, err
	}

	pubOpts.Body = payload
	if err = rabbit.publishMessage(ch, exchangeName, fullTaskName, pubOpts); err != nil {
		return nil, err
	}
	rabbit.logger.Info(fmt.Sprintf("Task %s enqueued", fullTaskName))
	return task, nil
}

// RegisterEventConsumer registers a consumer for events.
func (rabbit *GoRabbit) RegisterEventConsumer(eventKey string, consumeFn func(context any, message any) error, options any) (string, error) {
	// Here we assume options is a map[string]any
	opts, _ := options.(map[string]any)
	serviceName := rabbit.config.ServiceName
	if s, ok := opts["serviceName"].(string); ok && s != "" {
		serviceName = s
	}
	uniqueQueue := false
	if uq, ok := opts["uniqueQueue"].(bool); ok {
		uniqueQueue = uq
	}

	exchangeName := rabbit.config.Exchanges.EventsTopic
	eventQueueName := rabbit.getConsumeEventQueueName(eventKey, serviceName, uniqueQueue)
	rabbit.logger.Info(fmt.Sprintf("Registering event consumer for key %s on queue %s", eventKey, eventQueueName))

	ch, err := rabbit.channelPool.GetConsumerChannel()
	if err != nil {
		return "", err
	}

	if err = ch.ExchangeDeclare(
		exchangeName,
		"topic",
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return "", err
	}

	queueType := "classic"
	if !uniqueQueue {
		queueType = "quorum"
	}

	args := amqp.Table{
		"x-queue-type": queueType,
	}
	queue, err := ch.QueueDeclare(
		eventQueueName,
		true,        // durable
		uniqueQueue, // auto-delete if unique
		false,       // exclusive
		false,       // no-wait
		args,        // args
	)
	if err != nil {
		return "", err
	}
	if err = ch.QueueBind(queue.Name, eventKey, exchangeName, false, nil); err != nil {
		return "", err
	}
	if err = ch.Qos(rabbit.config.Consumer.Prefetch, 0, false); err != nil {
		return "", err
	}
	consumerTag := eventQueueName + "-" + rabbit.getInstanceIdentifier()
	msgs, err := ch.Consume(
		queue.Name,
		consumerTag,
		false, // auto-ack false so we can manually ack/nack
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return "", err
	}

	go rabbit.handleConsumeMessages(msgs, "event", eventQueueName, consumeFn, options)
	rabbit.consumers = append(rabbit.consumers, Consumer{
		Type:        "event",
		Key:         eventKey,
		ConsumerTag: consumerTag,
		ConsumeFn:   consumeFn,
		Options:     options,
	})
	return consumerTag, nil
}

// RegisterTaskConsumer registers a consumer for tasks.
func (rabbit *GoRabbit) RegisterTaskConsumer(
	taskName string,
	consumeFn func(context any, message any) error,
	options any) (string, error) {
	opts, _ := options.(map[string]any)
	serviceName := rabbit.config.ServiceName
	if s, ok := opts["serviceName"].(string); ok && s != "" {
		serviceName = s
	}
	uniqueQueue := false
	if uq, ok := opts["uniqueQueue"].(bool); ok {
		uniqueQueue = uq
	}
	fullTaskName := serviceName + "." + taskName
	taskQueueName := rabbit.getTaskConsumerQueueName(taskName, serviceName, uniqueQueue)
	rabbit.logger.Info(fmt.Sprintf("Registering task consumer for %s on queue %s", taskName, taskQueueName))

	ch, err := rabbit.channelPool.GetConsumerChannel()
	if err != nil {
		return "", err
	}
	if err = ch.ExchangeDeclare(
		rabbit.config.Exchanges.TasksTopic,
		"topic",
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return "", err
	}

	queueType := "classic"
	if !uniqueQueue {
		queueType = "quorum"
	}

	args := amqp.Table{
		"x-queue-type": queueType,
	}
	queue, err := ch.QueueDeclare(
		taskQueueName,
		true,
		uniqueQueue,
		false,
		false,
		args,
	)
	if err != nil {
		return "", err
	}
	if err = ch.QueueBind(queue.Name, fullTaskName, rabbit.config.Exchanges.TasksTopic, false, nil); err != nil {
		return "", err
	}
	if err = ch.Qos(rabbit.config.Consumer.Prefetch, 0, false); err != nil {
		return "", err
	}
	consumerTag := taskQueueName + "-" + rabbit.getInstanceIdentifier()
	msgs, err := ch.Consume(
		queue.Name,
		consumerTag,
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return "", err
	}

	go rabbit.handleConsumeMessages(msgs, "task", taskQueueName, consumeFn, options)
	rabbit.consumers = append(rabbit.consumers, Consumer{
		Type:        "task",
		Key:         taskName,
		ConsumerTag: consumerTag,
		ConsumeFn:   consumeFn,
		Options:     options,
	})
	return consumerTag, nil
}

// RegisterFailedMessageConsumer registers a consumer for failed messages.
func (rabbit *GoRabbit) RegisterFailedMessageConsumer(consumeFn func(queueName string, message any) error, options any) (string, error) {
	ch, err := rabbit.channelPool.GetConsumerChannel()
	if err != nil {
		return "", err
	}
	queueName := rabbit.config.Queues.Failed
	rabbit.logger.Info(fmt.Sprintf("Registering failed message consumer on queue %s", queueName))

	args := amqp.Table{
		"x-queue-type": "quorum",
	}
	queue, err := ch.QueueDeclare(
		queueName,
		true,
		false,
		false,
		false,
		args,
	)
	if err != nil {
		return "", err
	}
	if err = ch.Qos(rabbit.config.Consumer.Prefetch, 0, false); err != nil {
		return "", err
	}
	consumerTag := queueName + "-" + rabbit.getInstanceIdentifier()
	msgs, err := ch.Consume(
		queue.Name,
		consumerTag,
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return "", err
	}
	go rabbit.handleFailedMessages(msgs, queueName, consumeFn, options)
	// Wrap the consumeFn to fit our Consumer type.
	rabbit.failedMessageConsumer = append(rabbit.failedMessageConsumer, FailedMessageConsumer{
		Type:        "failed",
		Key:         "failed",
		ConsumerTag: consumerTag,
		ConsumeFn:   consumeFn,
		Options:     options,
	})
	return consumerTag, nil
}

// AssertConnection makes sure the connection is established.
func (rabbit *GoRabbit) AssertConnection() error {
	_, err := rabbit.getConnection()
	return err
}

// EnqueueMessage enqueues a message to a specific queue.
func (rabbit *GoRabbit) EnqueueMessage(queueName string, messageObject any) error {
	ch, err := rabbit.channelPool.GetPublisherChannel()
	if err != nil {
		return err
	}
	payload, err := json.Marshal(messageObject)
	if err != nil {
		return err
	}
	// Using the default direct exchange (empty string)
	if err = rabbit.publishMessage(ch, "", queueName, amqp.Publishing{Body: payload}); err != nil {
		return err
	}
	rabbit.logger.Info(fmt.Sprintf("Enqueued message to queue %s", queueName))
	return nil
}

// Shutdown performs a graceful shutdown.
func (rabbit *GoRabbit) Shutdown(timeout time.Duration) error {
	if rabbit.isShuttingDown {
		return nil
	}
	rabbit.isShuttingDown = true

	rabbit.logger.Info("Shutting down GoRabbitMQ")
	if err := rabbit.cancelAllConsumers(); err != nil {
		rabbit.logger.Error(fmt.Sprintf("Error cancelling consumers: %v", err))
	}

	done := make(chan struct{})
	go func() {
		for {
			if len(rabbit.activeMessageConsumptions) == 0 {
				close(done)
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()
	select {
	case <-done:
	case <-time.After(timeout):
		rabbit.logger.Error("Timeout waiting for consumers to finish")
	}

	// (Optionally: nack any remaining messages)

	rabbit.channelPool.Close()
	if rabbit.conn != nil {
		if err := rabbit.conn.Close(); err != nil {
			rabbit.logger.Error(fmt.Sprintf("Error closing connection: %v", err))
		}
		rabbit.conn = nil
	}
	return nil
}

// cancelAllConsumers cancels all registered consumers.
func (rabbit *GoRabbit) cancelAllConsumers() error {
	ch, err := rabbit.channelPool.GetConsumerChannel()
	if err != nil {
		return err
	}
	for _, consumer := range rabbit.consumers {
		if err := ch.Cancel(consumer.ConsumerTag, false); err != nil {
			rabbit.logger.Error(fmt.Sprintf("Error cancelling consumer %s: %v", consumer.ConsumerTag, err))
		}
	}
	rabbit.consumers = nil
	return nil
}

// -----------------------------------------------------------------------------
// Internal helper methods
// -----------------------------------------------------------------------------

// getInstanceIdentifier returns a random identifier for this instance.
func (rabbit *GoRabbit) getInstanceIdentifier() string {
	b := make([]byte, 10)
	if _, err := rand.Read(b); err != nil {
		return "default"
	}
	return hex.EncodeToString(b)
}

// getConsumeEventQueueName returns the name of the queue for consuming events.
func (rabbit *GoRabbit) getConsumeEventQueueName(eventKey, serviceName string, uniqueQueue bool) string {
	queueName := "events." + serviceName
	if uniqueQueue {
		queueName += ".instance-" + rabbit.getInstanceIdentifier()
	}
	queueName += "." + eventKey
	return queueName
}

// getTaskConsumerQueueName returns the name of the queue for consuming tasks.
func (rabbit *GoRabbit) getTaskConsumerQueueName(taskName, serviceName string, uniqueQueue bool) string {
	queueName := "tasks." + serviceName
	if uniqueQueue {
		queueName += ".instance-" + rabbit.getInstanceIdentifier()
	}
	queueName += "." + taskName
	return queueName
}

// handleConsumeMessages processes messages from event or task consumers.
func (rabbit *GoRabbit) handleConsumeMessages(msgs <-chan amqp.Delivery, messageType, queueName string,
	consumeFn func(context any, message any) error, options any) {
	for msg := range msgs {
		var msgObj map[string]any

		if err := json.Unmarshal(msg.Body, &msgObj); err != nil {
			rabbit.logger.Error(fmt.Sprintf("Error unmarshaling message: %v", err))
			if nackErr := msg.Nack(false, false); nackErr != nil {
				rabbit.logger.Error(fmt.Sprintf("Error NACKing message: %v", nackErr))
			}
			continue
		}

		// Determine name based on type.
		var name string
		if messageType == "event" {
			if v, ok := msgObj["eventName"].(string); ok {
				name = v
			}
		} else {
			if v, ok := msgObj["taskName"].(string); ok {
				name = v
			}
		}
		rabbit.logger.Info(fmt.Sprintf("%s %s ready to be consumed", messageType, name))
		rabbit.activeMessageConsumptions = append(rabbit.activeMessageConsumptions, msgObj)

		startTime := time.Now()
		err := consumeFn(msgObj["context"], msgObj)
		duration := time.Since(startTime)
		if err != nil {
			rabbit.logger.Error(fmt.Sprintf("Error consuming %s %s: %v", messageType, name, err))
			rabbit.handleConsumeRejection(msg, messageType, msgObj, err, queueName, options)
		} else {
			rabbit.logger.Info(fmt.Sprintf("%s %s consumed in %v", messageType, name, duration))
		}

		rabbit.activeMessageConsumptions = removeFromSlice(rabbit.activeMessageConsumptions, msgObj)
		if err := msg.Ack(false); err != nil {
			rabbit.logger.Error(fmt.Sprintf("Error ACKing message: %v", err))
		}
	}
}

// handleConsumeRejection processes a message that failed to be consumed.
func (rabbit *GoRabbit) handleConsumeRejection(msg amqp.Delivery, messageType string,
	msgObj map[string]any, consumeError error, queueName string, options any) {
	// Decide whether to retry.
	currentAttempt := 0
	if a, ok := msgObj["attempts"].(float64); ok {
		currentAttempt = int(a)
	}
	shouldRetry, delaySeconds := decideConsumerRetry(currentAttempt, options)
	// (Optionally, call an onError handler here.)
	ch, err := rabbit.channelPool.GetPublisherChannel()
	if err != nil {
		rabbit.logger.Error(fmt.Sprintf("Error getting publisher channel: %v", err))
		return
	}
	var republishExchange string
	pubOpts := amqp.Publishing{}
	if shouldRetry {
		retryRes, errRetry := rabbit.assertRetryExchangeAndQueue(ch, delaySeconds)
		if errRetry != nil {
			rabbit.logger.Error(fmt.Sprintf("Error asserting retry exchange: %v", errRetry))
			return
		}
		republishExchange = retryRes.RetryExchangeName
		pubOpts.Headers = amqp.Table{"BCC": []any{retryRes.RetryQueueName}}
	} else {
		deadLetterExchange, errDlx := rabbit.assertDeadLetterExchangeAndQueue(ch)
		if errDlx != nil {
			rabbit.logger.Error(fmt.Sprintf("Error asserting dead letter exchange: %v", errDlx))
			return
		}
		republishExchange = deadLetterExchange
	}

	// Update attempts and republish.
	msgObj["attempts"] = currentAttempt + 1
	payload, err := json.Marshal(msgObj)
	if err != nil {
		rabbit.logger.Error(fmt.Sprintf("Error marshaling message: %v", err))
		return
	}
	pubOpts.Body = payload
	if err = rabbit.publishMessage(ch, republishExchange, queueName, pubOpts); err != nil {
		rabbit.logger.Error(fmt.Sprintf("Error republishing message: %v", err))
	}
	rabbit.logger.Info(fmt.Sprintf("Scheduled %s %s for %s (retry: %v, delay: %ds)", messageType, msgObj["uuid"], queueName, shouldRetry, delaySeconds))
}

// handleFailedMessages processes messages from the failed queue.
func (rabbit *GoRabbit) handleFailedMessages(msgs <-chan amqp.Delivery, queueName string,
	consumeFn func(queueName string, message any) error, options any) {
	for msg := range msgs {
		var msgObj map[string]any
		if err := json.Unmarshal(msg.Body, &msgObj); err != nil {
			rabbit.logger.Error(fmt.Sprintf("Error unmarshaling failed message: %v", err))
			if nackErr := msg.Nack(false, false); nackErr != nil {
				rabbit.logger.Error(fmt.Sprintf("Error NACKing message: %v", nackErr))
			}
			continue
		}
		rabbit.logger.Info(fmt.Sprintf("Failed message ready to be consumed: %v", msgObj))
		rabbit.activeMessageConsumptions = append(rabbit.activeMessageConsumptions, msgObj)
		startTime := time.Now()
		err := consumeFn(msg.RoutingKey, msgObj)
		duration := time.Since(startTime)
		if err != nil {
			rabbit.logger.Error(fmt.Sprintf("Error consuming failed message: %v", err))
			if nackErr := msg.Nack(false, true); nackErr != nil {
				rabbit.logger.Error(fmt.Sprintf("Error NACKing failed message: %v", nackErr))
			}
		} else {
			if err = msg.Ack(false); err != nil {
				rabbit.logger.Error(fmt.Sprintf("Error ACKing failed message: %v", err))
			}
			rabbit.logger.Info(fmt.Sprintf("Failed message consumed in %v", duration))
		}
		rabbit.activeMessageConsumptions = removeFromSlice(rabbit.activeMessageConsumptions, msgObj)
	}
}

// assertRetryExchangeAndQueue prepares an exchange and queue for retrying messages.
type RetryExchangeQueue struct {
	RetryExchangeName string
	RetryQueueName    string
}

func (rabbit *GoRabbit) assertRetryExchangeAndQueue(ch *amqp.Channel, delaySeconds int) (*RetryExchangeQueue, error) {
	delayMs := delaySeconds * 1000
	retryExchangeName := rabbit.config.Exchanges.Retry
	retryQueueName := fmt.Sprintf("%s.%dms", rabbit.config.Queues.RetryPrefix, delayMs)
	if err := ch.ExchangeDeclare(retryExchangeName, "direct", true, false, false, false, nil); err != nil {
		return nil, err
	}
	args := amqp.Table{
		"x-dead-letter-exchange": "",
		"x-message-ttl":          delayMs,
	}
	q, err := ch.QueueDeclare(
		retryQueueName,
		true,
		true,
		false,
		false,
		args,
	)
	if err != nil {
		return nil, err
	}
	if err := ch.QueueBind(q.Name, q.Name, retryExchangeName, false, nil); err != nil {
		return nil, err
	}
	return &RetryExchangeQueue{
		RetryExchangeName: retryExchangeName,
		RetryQueueName:    q.Name,
	}, nil
}

// DelayedExchangeQueue holds delayed exchange information.
type DelayedExchangeQueue struct {
	DelayedExchangeName string
	DelayedQueueName    string
}

// assertDelayedTaskExchangeAndQueue prepares an exchange and queue for delayed tasks.
func (rabbit *GoRabbit) assertDelayedTaskExchangeAndQueue(ch *amqp.Channel, delayMillis int) (*DelayedExchangeQueue, error) {
	delayedExchangeName := rabbit.config.Exchanges.Delayed
	delayedQueueName := fmt.Sprintf("%s.%dms", rabbit.config.Queues.DelayedTaskPrefix, delayMillis)
	if err := ch.ExchangeDeclare(delayedExchangeName, "direct", true, false, false, false, nil); err != nil {
		return nil, err
	}
	args := amqp.Table{
		"x-dead-letter-exchange": rabbit.config.Exchanges.TasksTopic,
		"x-message-ttl":          delayMillis,
		"x-expires":              delayMillis + 10000,
	}
	q, err := ch.QueueDeclare(
		delayedQueueName,
		true,
		true,
		false,
		false,
		args,
	)
	if err != nil {
		return nil, err
	}
	if err := ch.QueueBind(q.Name, q.Name, delayedExchangeName, false, nil); err != nil {
		return nil, err
	}
	return &DelayedExchangeQueue{
		DelayedExchangeName: delayedExchangeName,
		DelayedQueueName:    q.Name,
	}, nil
}

// assertDeadLetterExchangeAndQueue sets up the dead–letter (failed) exchange and queue.
func (rabbit *GoRabbit) assertDeadLetterExchangeAndQueue(ch *amqp.Channel) (string, error) {
	deadLetterExchangeName := rabbit.config.Exchanges.Failed
	deadLetterQueueName := rabbit.config.Queues.Failed
	if err := ch.ExchangeDeclare(deadLetterExchangeName, "fanout", true, false, false, false, nil); err != nil {
		return "", err
	}

	if _, err := ch.QueueDeclare(deadLetterQueueName, true, false, false, false, nil); err != nil {
		return "", err
	}
	if err := ch.QueueBind(deadLetterQueueName, "", deadLetterExchangeName, false, nil); err != nil {
		return "", err
	}
	return deadLetterExchangeName, nil
}

// decideConsumerRetry decides whether a message should be retried.
func decideConsumerRetry(currentAttempt int, options any) (bool, int) {
	// Default: fixed delay of 16 seconds, maximum 12 attempts.
	maxAttempts := 12
	delaySeconds := 16
	if opts, ok := options.(map[string]any); ok {
		if m, ok := opts["maxAttempts"].(int); ok {
			maxAttempts = m
		}
		if d, ok := opts["delaySeconds"].(int); ok {
			delaySeconds = d
		}
		if bt, ok := opts["backoffType"].(string); ok && bt == "exponential" {
			delaySeconds = int(float64(delaySeconds) * math.Pow(2, float64(currentAttempt)))
		}
	}
	if maxAttempts != -1 && currentAttempt >= maxAttempts {
		return false, 0
	}
	return true, delaySeconds
}

// removeFromSlice is a helper to remove an item from a slice.
func removeFromSlice(slice []any, item any) []any {
	newSlice := []any{}
	for _, v := range slice {
		if !reflect.DeepEqual(v, item) {
			newSlice = append(newSlice, v)
		}
	}
	return newSlice
}

// generateConnectionURL builds the connection URL from configuration.
func generateConnectionURL(cfg ConnectionConfiguration) string {
	if cfg.Protocol != "amqp" && cfg.Protocol != "amqps" {
		panic(fmt.Sprintf("Invalid protocol '%s'. Must be 'amqp' or 'amqps'", cfg.Protocol))
	}
	url := fmt.Sprintf("%s://", cfg.Protocol)
	if cfg.Username != "" {
		url += cfg.Username
		if cfg.Password != "" {
			url += ":" + cfg.Password
		}
		url += "@"
	}
	url += cfg.Host
	if cfg.Port != 0 {
		url += fmt.Sprintf(":%d", cfg.Port)
	}
	if cfg.VHost != "" {
		url += "/" + cfg.VHost
	}
	return url
}
