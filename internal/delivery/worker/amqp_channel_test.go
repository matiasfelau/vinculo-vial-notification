package worker

import (
	"context"

	amqp "github.com/rabbitmq/amqp091-go"
)

// fakeDeferredConfirmation implementa deferredConfirmation con un
// resultado configurable, sin depender de los campos internos (no
// exportados) de *amqp.DeferredConfirmation.
type fakeDeferredConfirmation struct {
	acked bool
	err   error
}

func (f fakeDeferredConfirmation) WaitContext(context.Context) (bool, error) {
	return f.acked, f.err
}

type fakeQosCall struct {
	prefetchCount int
	prefetchSize  int
	global        bool
}

// fakeAMQPChannel implementa AMQPChannel registrando las llamadas
// recibidas y devolviendo resultados configurables. Permite testear
// configureChannel y AMQPDeadLetterPublisher sin un broker real.
type fakeAMQPChannel struct {
	qosCalls []fakeQosCall
	qosErr   error

	confirmCalls int
	confirmErr   error

	notifyReturnCalls int

	publishedExchange   string
	publishedKey        string
	publishedMandatory  bool
	publishedImmediate  bool
	publishedMsg        amqp.Publishing
	publishConfirmation deferredConfirmation
	publishErr          error
}

func (f *fakeAMQPChannel) Qos(prefetchCount, prefetchSize int, global bool) error {
	f.qosCalls = append(f.qosCalls, fakeQosCall{prefetchCount: prefetchCount, prefetchSize: prefetchSize, global: global})
	return f.qosErr
}

func (f *fakeAMQPChannel) Confirm(bool) error {
	f.confirmCalls++
	return f.confirmErr
}

func (f *fakeAMQPChannel) NotifyReturn(c chan amqp.Return) chan amqp.Return {
	f.notifyReturnCalls++
	// Se cierra de inmediato: no hay devoluciones que simular en estos
	// tests, y logUnroutableReturns debe terminar sola en lugar de
	// dejar una goroutine bloqueada para siempre.
	close(c)
	return c
}

func (f *fakeAMQPChannel) Consume(string, string, bool, bool, bool, bool, amqp.Table) (<-chan amqp.Delivery, error) {
	return nil, nil
}

func (f *fakeAMQPChannel) PublishWithDeferredConfirmWithContext(_ context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) (deferredConfirmation, error) {
	f.publishedExchange = exchange
	f.publishedKey = key
	f.publishedMandatory = mandatory
	f.publishedImmediate = immediate
	f.publishedMsg = msg
	return f.publishConfirmation, f.publishErr
}

func (f *fakeAMQPChannel) Close() error {
	return nil
}

var _ AMQPChannel = (*fakeAMQPChannel)(nil)
