package worker

import (
	"context"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

// AlertCreatedConsumer consume notification.alert-created.q y aplica
// la decision del handler sobre cada delivery. El ACK es siempre
// manual: la cola se declara con autoAck en false.
type AlertCreatedConsumer struct {
	channel *amqp.Channel
	queue   string
	handler *AlertCreatedHandler
}

func NewAlertCreatedConsumer(channel *amqp.Channel, queue string, handler *AlertCreatedHandler) *AlertCreatedConsumer {
	return &AlertCreatedConsumer{channel: channel, queue: queue, handler: handler}
}

// Run consume la cola hasta que el contexto se cancela o el canal se
// cierra. Devuelve cuando termina de procesar el mensaje en vuelo, lo
// que le permite al llamador coordinar un apagado ordenado.
func (c *AlertCreatedConsumer) Run(ctx context.Context) error {
	deliveries, err := c.channel.Consume(c.queue, "", false, false, false, false, nil)
	if err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case delivery, ok := <-deliveries:
			if !ok {
				return nil
			}
			c.process(ctx, delivery)
		}
	}
}

func (c *AlertCreatedConsumer) process(ctx context.Context, delivery amqp.Delivery) {
	switch c.handler.Handle(ctx, delivery.Body) {
	case OutcomeAck:
		if err := delivery.Ack(false); err != nil {
			log.Printf("no se pudo confirmar el mensaje de alert.created: %v", err)
		}
	case OutcomeNackNoRequeue:
		if err := delivery.Nack(false, false); err != nil {
			log.Printf("no se pudo rechazar el mensaje de alert.created: %v", err)
		}
	}
}
