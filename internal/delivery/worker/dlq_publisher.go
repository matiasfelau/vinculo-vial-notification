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

// errDLQQueueRequired se devuelve cuando no se informa la cola de
// origen. La DLQ esta bindeada por el nombre de la cola de trabajo:
// publicar con otra routing key (o vacia) dejaria el mensaje sin ruta.
var errDLQQueueRequired = errors.New("la cola de origen es obligatoria para publicar en la dlq")

// AMQPDeadLetterPublisher publica directamente al exchange de dead
// letter, sorteando el ciclo de reintentos normal. No declara
// exchanges, colas ni bindings: solo publica sobre topologia
// preexistente.
type AMQPDeadLetterPublisher struct {
	channel publishChannel
}

func NewAMQPDeadLetterPublisher(channel publishChannel) *AMQPDeadLetterPublisher {
	return &AMQPDeadLetterPublisher{channel: channel}
}

var _ deadLetterPublisher = (*AMQPDeadLetterPublisher)(nil)

// PublishToDLQ publica el mensaje en vvi.dlq.exchange con routing key
// igual al nombre de la cola de trabajo de la que proviene (queue),
// que es por lo que esta bindeada su DLQ. Lo publica como persistente
// y con mandatory=true, y espera la confirmacion del broker (publisher
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
func (p *AMQPDeadLetterPublisher) PublishToDLQ(ctx context.Context, queue string, body []byte, reason string) error {
	if queue == "" {
		return errDLQQueueRequired
	}

	confirmation, err := p.channel.PublishWithDeferredConfirmWithContext(ctx, dlqExchange, queue, true, false, amqp.Publishing{
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
