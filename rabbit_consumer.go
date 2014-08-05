package mqhammer

import (
	"crypto/rand"
	"fmt"
	"net/url"
	"sync"

	"github.com/streadway/amqp"
)

type RabbitConsumerConfig struct {
	URI          string
	Exchange     string
	ExchangeType string
	Queue        string
	Key          string
}

// ParseRabbitConsumerConfig parses an amqp schemed url string
// and sets the config parameters based on the querystring values
func ParseRabbitConsumerConfig(rawurl string) (*RabbitConsumerConfig, error) {
	config := &RabbitConsumerConfig{}

	u, err := url.Parse(rawurl)
	if err != nil {
		return nil, err
	}

	if u.Scheme != "amqp" {
		return nil, fmt.Errorf("expected scheme: amqp")
	}

	config.Exchange = u.Query().Get("exchange")
	if len(config.Exchange) == 0 {
		return nil, fmt.Errorf("expected query parameter: exchange")
	}

	config.ExchangeType = u.Query().Get("exchange_type")
	switch config.ExchangeType {
	case "direct", "fanout", "topic", "x-custom":
	default:
		return nil, fmt.Errorf("expected exchange type: %s", config.ExchangeType)
	}

	config.Queue = u.Query().Get("queue")
	if len(config.Queue) == 0 {
		return nil, fmt.Errorf("expected query parameter: queue")
	}

	config.Key = u.Query().Get("key")
	if len(config.Key) == 0 {
		return nil, fmt.Errorf("expected query parameter: key")
	}

	u.RawQuery = ""
	config.URI = u.String()
	return config, nil
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
	conn    *amqp.Connection
	channel *amqp.Channel
	done    chan struct{}
	in      <-chan amqp.Delivery
	once    sync.Once
	err     error
}

func NewRabbitConsumer(rawurl string) (*RabbitConsumer, error) {
	config, err := ParseRabbitConsumerConfig(rawurl)
	if err != nil {
		return nil, err
	}
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

	consumerTagBytes := make([]byte, 16)
	rand.Read(consumerTagBytes)

	in, err := channel.Consume(
		config.Queue,                        // name
		fmt.Sprintf("%x", consumerTagBytes), // consumerTag,
		false, // noAck
		false, // exclusive
		false, // noLocal
		false, // noWait
		nil,   // arguments
	)
	if err != nil {
		return nil, err
	}
	return &RabbitConsumer{
		conn:    conn,
		channel: channel,
		in:      in,
		done:    make(chan struct{}),
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
		defer c.conn.Close()

		for {
			select {
			case <-c.done:
				shutdown = true
				// closing the channel will cause "in" to close, but we may
				// still have messages in-flight that we need to handle
				if err := c.channel.Close(); err != nil {
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
