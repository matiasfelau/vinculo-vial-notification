package worker

import (
	"context"

	amqp "github.com/rabbitmq/amqp091-go"
)

// deferredConfirmation es el subconjunto de *amqp.DeferredConfirmation
// que este paquete consume. amqp091-go no expone un constructor
// publico para ese tipo (sus campos internos no son accesibles fuera
// del paquete): sin esta interfaz de por medio, no habria forma de
// construir un doble de prueba que resuelva WaitContext sin un broker
// real.
type deferredConfirmation interface {
	WaitContext(ctx context.Context) (bool, error)
}

var _ deferredConfirmation = (*amqp.DeferredConfirmation)(nil)

// consumeChannel es el subconjunto de AMQPChannel que Consumer
// necesita.
type consumeChannel interface {
	Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error)
}

// publishChannel es el subconjunto de AMQPChannel que
// AMQPDeadLetterPublisher necesita.
type publishChannel interface {
	PublishWithDeferredConfirmWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) (deferredConfirmation, error)
}

// AMQPChannel es el subconjunto de *amqp.Channel que este paquete
// consume en su totalidad: lo que configureChannel aplica al abrir la
// conexion (Qos, Confirm, NotifyReturn), mas lo que necesitan Consumer
// y AMQPDeadLetterPublisher. Aislarlo permite testear dial,
// configureChannel, Consumer y AMQPDeadLetterPublisher con un doble,
// sin depender de un broker real.
type AMQPChannel interface {
	consumeChannel
	publishChannel
	Qos(prefetchCount, prefetchSize int, global bool) error
	Confirm(noWait bool) error
	NotifyReturn(c chan amqp.Return) chan amqp.Return
	Close() error
}

// realAMQPChannel adapta *amqp.Channel a AMQPChannel. El metodo real
// PublishWithDeferredConfirmWithContext devuelve el tipo concreto
// *amqp.DeferredConfirmation; Go no permite que *amqp.Channel
// satisfaga directamente una interfaz cuyo mismo metodo devuelve en
// cambio deferredConfirmation (el retorno debe coincidir exactamente
// para que un tipo implemente una interfaz), asi que este adaptador
// hace esa traduccion. El resto de los metodos de AMQPChannel se
// promueven sin cambios desde el *amqp.Channel embebido.
type realAMQPChannel struct {
	*amqp.Channel
}

func newRealAMQPChannel(channel *amqp.Channel) realAMQPChannel {
	return realAMQPChannel{Channel: channel}
}

func (c realAMQPChannel) PublishWithDeferredConfirmWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) (deferredConfirmation, error) {
	confirmation, err := c.Channel.PublishWithDeferredConfirmWithContext(ctx, exchange, key, mandatory, immediate, msg)
	// Conversion explicita: un *amqp.DeferredConfirmation nil asignado
	// directamente a la interfaz de retorno produciria un valor de
	// interfaz no-nil que envuelve un puntero nil (la trampa clasica de
	// Go), y PublishToDLQ dejaria de poder detectar con == nil el caso
	// documentado por el SDK ("si el canal no esta en modo confirm, la
	// DeferredConfirmation sera nil").
	if confirmation == nil {
		return nil, err
	}
	return confirmation, err
}

var _ AMQPChannel = realAMQPChannel{}
