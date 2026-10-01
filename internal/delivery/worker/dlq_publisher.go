package worker

import (
	"context"
	"errors"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

// dlqExchange es el exchange de dead letter de la plataforma. Es un
// nombre fijo de la topologia, igual que los nombres de cola: no varia
// entre entornos y no se declara desde aca (lo hace vinculo-vial-core).
const dlqExchange = "vvi.dlq.exchange"

// errDLQNotConfirmed se devuelve cuando el broker no confirmo la
// publicacion a la DLQ (nack de publisher confirm) o el canal no esta
// en modo confirm. En ambos casos no hay garantia de que el mensaje
// haya quedado persistido: el llamador no debe ACKear el mensaje
// original.
var errDLQNotConfirmed = errors.New("la dlq no confirmo la publicacion")

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

// PublishToDLQ publica el mensaje como persistente y con
// mandatory=true, y espera la confirmacion del broker (publisher
// confirm) antes de devolver exito. El canal debe estar en modo
// confirm (ver dial en reconnect.go); de no estarlo, se trata como
// fallo en lugar de arriesgar un ACK del mensaje original sin
// garantia real de que la DLQ lo recibio.
//
// Un mensaje mandatory que el broker no puede rutear (por ejemplo, la
// DLQ sin una cola bindeada) de todos modos recibe un ack de
// publisher confirm -- confirm certifica que el broker proceso la
// publicacion, no que la ruteo. Esa situacion se reporta por separado
// via NotifyReturn (ver logUnroutableReturns en reconnect.go), no
// como un error de esta llamada: no hay nada que este publisher
// pueda hacer distinto ante un problema de topologia que no declara.
func (p *AMQPDeadLetterPublisher) PublishToDLQ(ctx context.Context, body []byte, reason string) error {
	confirmation, err := p.channel.PublishWithDeferredConfirmWithContext(ctx, dlqExchange, "", true, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Body:         body,
		Headers: amqp.Table{
			"x-discard-reason": reason,
		},
	})
	if err != nil {
		return fmt.Errorf("no se pudo publicar a la dlq: %w", err)
	}
	if confirmation == nil {
		return errDLQNotConfirmed
	}

	acked, err := confirmation.WaitContext(ctx)
	if err != nil {
		return fmt.Errorf("no se recibio confirmacion de la dlq: %w", err)
	}
	if !acked {
		return errDLQNotConfirmed
	}

	return nil
}
