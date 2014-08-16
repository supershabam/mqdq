package mqdqx

import (
	"crypto/rand"
	"fmt"
	"net/url"

	"github.com/streadway/amqp"
	"github.com/supershabam/mqdq/mqdq"
)

type RabbitConsumerConfig struct {
	BindKey      string
	ConsumerTag  string
	Durable      bool
	Exchange     string
	ExchangeType string
	Queue        string
	URI          string
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
		return nil, fmt.Errorf("invalid url scheme: %s", u.Scheme)
	}

	// bind key is required and can't be ""
	config.BindKey = u.Query().Get("bind_key")
	if len(config.BindKey) == 0 {
		return nil, fmt.Errorf("missing required query parameter: bind_key")
	}

	// consumer tag defaults to a randomly generated hex id
	config.ConsumerTag = u.Query().Get("consumer_tag")
	if len(config.ConsumerTag) == 0 {
		consumerTagBytes := make([]byte, 16)
		rand.Read(consumerTagBytes)
		config.ConsumerTag = fmt.Sprintf("%x", consumerTagBytes)
	}

	// durable defaults to true, and may be set to "true" or "false"
	switch u.Query().Get("durable") {
	case "", "true":
		config.Durable = true
	case "false":
		config.Durable = false
	default:
		return nil, fmt.Errorf("durability must be set to true or false (defaults true)")
	}

	// exchange is required and can't be ""
	config.Exchange = u.Query().Get("exchange")
	if len(config.Exchange) == 0 {
		return nil, fmt.Errorf("missing required query parameter: exchange")
	}

	// exchange_type defaults to "direct" if not set, and may be set to "direct", "fanout", or "topic"
	config.ExchangeType = u.Query().Get("exchange_type")
	switch config.ExchangeType {
	case "":
		config.ExchangeType = "direct"
	case "direct", "fanout", "topic":
	default:
		return nil, fmt.Errorf("invalid exchange type %s", config.ExchangeType)
	}

	// queue is required
	config.Queue = u.Query().Get("queue")
	if len(config.Queue) == 0 {
		return nil, fmt.Errorf("expected query parameter: queue")
	}

	// drop all querystring values for the uri we'll actually let the amqp driver use
	u.RawQuery = ""
	config.URI = u.String()
	return config, nil
}

// RabbitAcknowledger lets us acknowledge rabbitConsumer messages once they're processed
// in a generic way
type RabbitAcknowledger struct {
	Delivery amqp.Delivery
}

// Ack says a message has been handled
func (a RabbitAcknowledger) Ack() {
	a.Delivery.Ack(false)
}

// Nack says that I received a message, but will not be handling it, pass it
// to somebody else
func (a RabbitAcknowledger) Nack() {
	a.Delivery.Nack(false, true)
}

type RabbitConsumer struct {
	Config RabbitConsumerConfig
	done   chan struct{}
	err    error
}

func NewRabbitConsumer(rawurl string) (*RabbitConsumer, error) {
	config, err := ParseRabbitConsumerConfig(rawurl)
	if err != nil {
		return nil, err
	}
	return &RabbitConsumer{
		Config: *config,
		done:   make(chan struct{}),
	}, nil
}

// Consume returns a channel of Deliveries which are acknowledgable bundles
// of []byte. You must either Nack or Ack a Delivery so that the message
// queue can acknowledge that the message has been processed (or will not
// be processed).
func (c *RabbitConsumer) Consume() <-chan mqdq.Delivery {
	out := make(chan mqdq.Delivery)
	go func() {
		defer close(out)

		conn, channel, err := boundRabbitConsumerConnChannel(c.Config)
		if err != nil {
			c.err = err
			return
		}

		// when we're exiting, close the rabbitConsumer connection after in-flight
		// acknowledgements complete
		defer func() {
			conn.Close()
		}()

		in, err := channel.Consume(
			c.Config.Queue,       // name
			c.Config.ConsumerTag, // consumerTag,
			false,                // noAck
			false,                // exclusive
			false,                // noLocal
			false,                // noWait
			nil,                  // arguments
		)
		if err != nil {
			c.err = err
			return
		}

		for {
			select {
			case <-c.done:
				// closing the channel will cause the in channel to close
				if err := channel.Close(); err != nil {
					c.err = err
					return
				}
			case rabbitDelivery, ok := <-in:
				if !ok {
					return
				}
				select {
				// if we've been stopped, noop to drain the in channel
				case <-c.done:
					// noop
				default:
					out <- mqdq.Delivery{
						Ackr: RabbitAcknowledger{rabbitDelivery},
						Msg:  rabbitDelivery.Body,
					}
				}
			}
		}
	}()
	return out
}

func (c RabbitConsumer) Err() error {
	return c.err
}

func (c *RabbitConsumer) Stop() {
	close(c.done)
}

func boundRabbitConsumerConnChannel(config RabbitConsumerConfig) (conn *amqp.Connection, channel *amqp.Channel, err error) {
	// handle shutting down conn on error for all return paths
	defer func() {
		if err != nil && conn != nil {
			conn.Close()
		}
	}()

	conn, err = amqp.Dial(config.URI)
	if err != nil {
		return
	}

	channel, err = conn.Channel()
	if err != nil {
		return
	}

	err = channel.ExchangeDeclare(
		config.Exchange,     // name of the exchange
		config.ExchangeType, // type
		config.Durable,      // durable
		false,               // delete when complete
		false,               // internal
		false,               // noWait
		nil,                 // arguments
	)
	if err != nil {
		return
	}

	_, err = channel.QueueDeclare(
		config.Queue,   // name of the queue
		config.Durable, // durable
		false,          // delete when usused
		false,          // exclusive
		false,          // noWait
		nil,            // arguments
	)
	if err != nil {
		return
	}

	err = channel.QueueBind(
		config.Queue,    // name of the queue
		config.BindKey,  // bindingKey
		config.Exchange, // sourceExchange
		false,           // noWait
		nil,             // arguments
	)

	return
}
