package impl

import (
	"crypto/rand"
	"fmt"
	// "net"
	"net/url"
	"sync"

	"github.com/supershabam/mqhammer"

	"github.com/streadway/amqp"
)

type RabbitConfig struct {
	URI          string
	Exchange     string
	ExchangeType string
	Queue        string
	Key          string
}

// ParseRabbitConfig parses an amqp schemed url string
// and sets the config parameters based on the querystring values
func ParseRabbitConfig(rawurl string) (*RabbitConfig, error) {
	config := &RabbitConfig{}

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
	d    amqp.Delivery
	wg   *sync.WaitGroup
	once *sync.Once
}

func newRabbitAcknowledger(d amqp.Delivery, wg *sync.WaitGroup) *rabbitAcknowledger {
	wg.Add(1)
	return &rabbitAcknowledger{
		d:    d,
		wg:   wg,
		once: &sync.Once{},
	}
}

func (a rabbitAcknowledger) Ack() {
	a.d.Ack(false)
	a.once.Do(func() {
		a.wg.Done()
	})
}

func (a rabbitAcknowledger) Nack() {
	a.d.Nack(false, true)
	a.once.Do(func() {
		a.wg.Done()
	})
}

type Rabbit struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	done    chan struct{}
	in      <-chan amqp.Delivery
	once    *sync.Once
	err     error
	wg      *sync.WaitGroup
}

func NewRabbitURL(rawurl string) (*Rabbit, error) {
	config, err := ParseRabbitConfig(rawurl)
	if err != nil {
		return nil, err
	}
	return NewRabbit(config)
}

func NewRabbit(config *RabbitConfig) (*Rabbit, error) {
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
	return &Rabbit{
		conn:    conn,
		channel: channel,
		in:      in,
		done:    make(chan struct{}),
		once:    &sync.Once{},
		wg:      &sync.WaitGroup{},
	}, nil
}

func (c Rabbit) Err() error {
	return c.err
}

func (c *Rabbit) Stop() {
	c.once.Do(func() {
		close(c.done)
	})
}

// Consume returns a channel of Deliveries which are acknowledgable bundles
// of []byte. You must either Nack or Ack a Delivery so that the message
// queue can acknowledge that the message has been processed (or will not
// be processed).
func (c *Rabbit) Consume() <-chan mqhammer.Delivery {
	out := make(chan mqhammer.Delivery)
	go func() {
		defer close(out)
		defer c.conn.Close()
		for {
			select {
			case <-c.done:
				if err := c.channel.Close(); err != nil {
					c.err = err
					return
				}
			case d, ok := <-c.in:
				if !ok {
					c.wg.Wait()
					return
				}
				ackr := newRabbitAcknowledger(d, c.wg)
				select {
				case <-c.done:
					ackr.Nack()
				default:
					out <- mqhammer.Delivery{
						Ackr: ackr,
						Msg:  d.Body,
					}
				}
			}
		}
	}()
	return out
}
