package gorabbit

import (
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rs/zerolog"
	"sync"
)

type ChannelPool struct {
	logger            *zerolog.Logger
	getConnectionFunc func() (*amqp.Connection, error)
	onChannelOpened   func(ch *amqp.Channel, channelType string) error
	onChannelClosed   func(channelType string, err error) error
	mu                sync.Mutex
	publisherChannel  *amqp.Channel
	consumerChannel   *amqp.Channel
}

// NewChannelPool rabbiteates a new ChannelPool.
func NewChannelPool(logger *zerolog.Logger,
	getConn func() (*amqp.Connection, error),
	onOpen func(ch *amqp.Channel, channelType string) error,
	onClose func(channelType string, err error) error) *ChannelPool {
	return &ChannelPool{
		logger:            logger,
		getConnectionFunc: getConn,
		onChannelOpened:   onOpen,
		onChannelClosed:   onClose,
	}
}

// GetPublisherChannel returns (or creates) a publisher channel.
func (cp *ChannelPool) GetPublisherChannel() (*amqp.Channel, error) {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	if cp.publisherChannel != nil {
		return cp.publisherChannel, nil
	}
	conn, err := cp.getConnectionFunc()
	if err != nil {
		return nil, err
	}
	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}
	cp.publisherChannel = ch
	cp.onChannelOpened(ch, "publisher")
	go func() {
		err := <-ch.NotifyClose(make(chan *amqp.Error))
		cp.onChannelClosed("publisher", err)
		cp.mu.Lock()
		cp.publisherChannel = nil
		cp.mu.Unlock()
	}()
	return ch, nil
}

// GetConsumerChannel returns (or creates) a consumer channel.
func (cp *ChannelPool) GetConsumerChannel() (*amqp.Channel, error) {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	if cp.consumerChannel != nil {
		return cp.consumerChannel, nil
	}
	conn, err := cp.getConnectionFunc()
	if err != nil {
		return nil, err
	}
	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}
	cp.consumerChannel = ch
	cp.onChannelOpened(ch, "consumer")
	go func() {
		err := <-ch.NotifyClose(make(chan *amqp.Error))
		cp.onChannelClosed("consumer", err)
		cp.mu.Lock()
		cp.consumerChannel = nil
		cp.mu.Unlock()
	}()
	return ch, nil
}

// Close closes all channels in the pool.
func (cp *ChannelPool) Close() {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	if cp.publisherChannel != nil {
		cp.publisherChannel.Close()
		cp.publisherChannel = nil
	}
	if cp.consumerChannel != nil {
		cp.consumerChannel.Close()
		cp.consumerChannel = nil
	}
}
