package mqdq

type Consumer interface {
	// Consume returns the Consume channel. You should only ever call this method once.
	// The channel will be closed when the consumption has ended, either due to
	// Stop being called, the upstream source being closed, or an error which has
	// stopped consumption. You should always check the state of the consumer by calling
	// Err after the Consume channel is closed.
	Consume() <-chan Delivery
	// Err allows you to check after the Consume channel has stopped whether or not
	// the Consumer stopped because of an error or not.
	Err() error
	// Stop signals the consumer to stop reading from its source. This method should
	// return immediately. Deliveries may still come in on the Consume channel. Once
	// the consumer has come to a full stop, the Consume channel will be closed.
	Stop()
}
