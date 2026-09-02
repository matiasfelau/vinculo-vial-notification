package worker

import (
	"context"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

// messageHandler decide que hacer con el cuerpo crudo de un delivery.
// AlertCreatedHandler y SessionsRevokedHandler implementan esta misma
// forma, lo que permite un unico Consumer generico para ambas colas.
type messageHandler interface {
	Handle(ctx context.Context, body []byte, retries int64) Outcome
}

// Consumer consume una cola de RabbitMQ y aplica la decision del
// handler sobre cada delivery. El ACK es siempre manual: la cola se
// declara con autoAck en false.
type Consumer struct {
	channel *amqp.Channel
	queue   string
	handler messageHandler
}

func NewConsumer(channel *amqp.Channel, queue string, handler messageHandler) *Consumer {
	return &Consumer{channel: channel, queue: queue, handler: handler}
}

// Run consume la cola hasta que el contexto se cancela o el canal se
// cierra. Devuelve cuando termina de procesar el mensaje en vuelo, lo
// que le permite al llamador coordinar un apagado ordenado.
func (c *Consumer) Run(ctx context.Context) error {
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

func (c *Consumer) process(ctx context.Context, delivery amqp.Delivery) {
	retries := deathCount(delivery.Headers, c.queue)

	switch c.handler.Handle(ctx, delivery.Body, retries) {
	case OutcomeAck:
		if err := delivery.Ack(false); err != nil {
			log.Printf("no se pudo confirmar el mensaje de %s: %v", c.queue, err)
		}
	case OutcomeNackNoRequeue:
		if err := delivery.Nack(false, false); err != nil {
			log.Printf("no se pudo rechazar el mensaje de %s: %v", c.queue, err)
		}
	}
}
