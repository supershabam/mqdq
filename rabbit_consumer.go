package mqhammer

import (
	"sync"

	"github.com/streadway/amqp"
)

type RabbitConsumerConfig struct {
	URI          string
	Exchange     string
	ExchangeType string
	Queue        string
	Key          string
	ConsumerTag  string
}

type rabbitAcknowledger struct {
	d amqp.Delivery
}

func (a rabbitAcknowledger) Ack() {
	a.d.Ack(false)
}

func (a rabbitAcknowledger) Nack() {
	a.d.Nack(false, true)
}

type RabbitConsumer struct {
	conn *amqp.Connection
	done chan struct{}
	in   <-chan amqp.Delivery
	once sync.Once
	err  error
}

func NewRabbitConsumer(config RabbitConsumerConfig) (*RabbitConsumer, error) {
	conn, err := amqp.Dial(config.URI)
	if err != nil {
		return nil, err
	}
	channel, err := conn.Channel()
	if err != nil {
		return nil, err
	}
	if err := channel.ExchangeDeclare(
		config.Exchange,     // name of the exchange
		config.ExchangeType, // type
		true,                // durable
		false,               // delete when complete
		false,               // internal
		false,               // noWait
		nil,                 // arguments
	); err != nil {
		return nil, err
	}
	_, err = channel.QueueDeclare(
		config.Queue, // name of the queue
		true,         // durable
		false,        // delete when usused
		false,        // exclusive
		false,        // noWait
		nil,          // arguments
	)
	if err != nil {
		return nil, err
	}
	if err = channel.QueueBind(
		config.Queue,    // name of the queue
		config.Key,      // bindingKey
		config.Exchange, // sourceExchange
		false,           // noWait
		nil,             // arguments
	); err != nil {
		return nil, err
	}
	in, err := channel.Consume(
		config.Queue,       // name
		config.ConsumerTag, // consumerTag,
		false,              // noAck
		false,              // exclusive
		false,              // noLocal
		false,              // noWait
		nil,                // arguments
	)
	if err != nil {
		return nil, err
	}
	return &RabbitConsumer{
		conn: conn,
		in:   in,
		done: make(chan struct{}),
	}, nil
}

func (c RabbitConsumer) Err() error {
	return c.err
}

func (c *RabbitConsumer) Stop() {
	c.once.Do(func() {
		close(c.done)
	})
}

// Consume returns a channel of Deliveries which are acknowledgable bundles
// of []byte. You must either Nack or Ack a Delivery so that the message
// queue can acknowledge that the message has been processed (or will not
// be processed).
func (c *RabbitConsumer) Consume() <-chan Delivery {
	var shutdown bool
	out := make(chan Delivery)

	go func() {
		defer close(out)

		for {
			select {
			case <-c.done:
				shutdown = true
				// closing the conn will cause "in" to close, but we may
				// still have messages in-flight that we need to handle
				if err := c.conn.Close(); err != nil {
					c.err = err
					return
				}
			case d, ok := <-c.in:
				// return if channel is closed
				if !ok {
					return
				}
				if shutdown {
					// make sure mq knows we are NOT fulfilling the message
					// that it had buffered up for us to process
					// multiple=false, requeue=true
					d.Nack(false, true)
					continue
				}
				out <- Delivery{
					Ackr: rabbitAcknowledger{d},
					Msg:  d.Body,
				}
			}
		}
	}()
	return out
}
