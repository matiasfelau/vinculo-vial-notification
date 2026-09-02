package worker

import (
	"context"

	amqp "github.com/rabbitmq/amqp091-go"
)

// dlqExchange es el exchange de dead letter de la plataforma. Es un
// nombre fijo de la topologia, igual que los nombres de cola: no varia
// entre entornos y no se declara desde aca (lo hace vinculo-vial-core).
const dlqExchange = "vvi.dlq.exchange"

// AMQPDeadLetterPublisher publica directamente al exchange de dead
// letter, sorteando el ciclo de reintentos normal. No declara
// exchanges, colas ni bindings: solo publica sobre topologia
// preexistente.
type AMQPDeadLetterPublisher struct {
	channel *amqp.Channel
}

func NewAMQPDeadLetterPublisher(channel *amqp.Channel) *AMQPDeadLetterPublisher {
	return &AMQPDeadLetterPublisher{channel: channel}
}

var _ deadLetterPublisher = (*AMQPDeadLetterPublisher)(nil)

func (p *AMQPDeadLetterPublisher) PublishToDLQ(ctx context.Context, body []byte, reason string) error {
	return p.channel.PublishWithContext(ctx, dlqExchange, "", false, false, amqp.Publishing{
		ContentType: "application/json",
		Body:        body,
		Headers: amqp.Table{
			"x-discard-reason": reason,
		},
	})
}
