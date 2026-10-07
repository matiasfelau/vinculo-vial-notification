package worker

import (
	"context"
	"errors"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestAMQPDeadLetterPublisher_PublicaComoPersistenteYMandatory(t *testing.T) {
	channel := &fakeAMQPChannel{publishConfirmation: fakeDeferredConfirmation{acked: true}}
	publisher := NewAMQPDeadLetterPublisher(channel)

	body := []byte(`{"foo":"bar"}`)
	if err := publisher.PublishToDLQ(context.Background(), testAlertQueue, body, "motivo de descarte"); err != nil {
		t.Fatalf("PublishToDLQ() devolvio un error inesperado: %v", err)
	}

	if channel.publishedExchange != dlqExchange {
		t.Errorf("exchange = %q, se esperaba %q", channel.publishedExchange, dlqExchange)
	}
	if !channel.publishedMandatory {
		t.Error("se esperaba que la publicacion fuera mandatory=true")
	}
	if channel.publishedImmediate {
		t.Error("se esperaba que la publicacion no fuera immediate=true")
	}
	if channel.publishedMsg.DeliveryMode != amqp.Persistent {
		t.Errorf("DeliveryMode = %d, se esperaba amqp.Persistent (%d)", channel.publishedMsg.DeliveryMode, amqp.Persistent)
	}
	if string(channel.publishedMsg.Body) != string(body) {
		t.Error("el cuerpo publicado no coincide con el original")
	}
	if got := channel.publishedMsg.Headers["x-discard-reason"]; got != "motivo de descarte" {
		t.Errorf("x-discard-reason = %v, se esperaba %q", got, "motivo de descarte")
	}
}

func TestAMQPDeadLetterPublisher_PublicaConLaColaDeOrigenComoRoutingKey(t *testing.T) {
	for _, queue := range []string{testAlertQueue, testSessionsQueue} {
		channel := &fakeAMQPChannel{publishConfirmation: fakeDeferredConfirmation{acked: true}}
		publisher := NewAMQPDeadLetterPublisher(channel)

		if err := publisher.PublishToDLQ(context.Background(), queue, []byte(`{}`), "motivo"); err != nil {
			t.Fatalf("PublishToDLQ(%q) devolvio un error inesperado: %v", queue, err)
		}

		if channel.publishedExchange != dlqExchange {
			t.Errorf("exchange = %q, se esperaba %q", channel.publishedExchange, dlqExchange)
		}
		if channel.publishedKey != queue {
			t.Errorf("routing key = %q, se esperaba la cola de origen %q", channel.publishedKey, queue)
		}
	}
}

func TestAMQPDeadLetterPublisher_RechazaUnaColaDeOrigenVacia_SinPublicar(t *testing.T) {
	channel := &fakeAMQPChannel{publishConfirmation: fakeDeferredConfirmation{acked: true}}
	publisher := NewAMQPDeadLetterPublisher(channel)

	err := publisher.PublishToDLQ(context.Background(), "", []byte(`{}`), "motivo")

	if !errors.Is(err, errDLQQueueRequired) {
		t.Fatalf("error = %v, se esperaba errDLQQueueRequired", err)
	}
	if channel.publishedExchange != "" {
		t.Error("no deberia haberse publicado nada sin cola de origen")
	}
}

func TestAMQPDeadLetterPublisher_ErrorSiElBrokerNoConfirma(t *testing.T) {
	channel := &fakeAMQPChannel{publishConfirmation: fakeDeferredConfirmation{acked: false}}
	publisher := NewAMQPDeadLetterPublisher(channel)

	if err := publisher.PublishToDLQ(context.Background(), testAlertQueue, []byte(`{}`), "motivo"); err == nil {
		t.Fatal("se esperaba un error cuando el broker no confirma la publicacion (nack de publisher confirm)")
	}
}

func TestAMQPDeadLetterPublisher_ErrorSiElCanalNoEstaEnModoConfirm(t *testing.T) {
	// publishConfirmation nil simula lo que devuelve el SDK real
	// cuando el canal no esta en modo confirm: no hay nada que
	// esperar, y no hay garantia de que el broker haya recibido el
	// mensaje.
	channel := &fakeAMQPChannel{publishConfirmation: nil}
	publisher := NewAMQPDeadLetterPublisher(channel)

	if err := publisher.PublishToDLQ(context.Background(), testAlertQueue, []byte(`{}`), "motivo"); err == nil {
		t.Fatal("se esperaba un error cuando el canal no esta en modo confirm")
	}
}

func TestAMQPDeadLetterPublisher_PropagaElErrorDePublish(t *testing.T) {
	channel := &fakeAMQPChannel{publishErr: errors.New("canal cerrado")}
	publisher := NewAMQPDeadLetterPublisher(channel)

	if err := publisher.PublishToDLQ(context.Background(), testAlertQueue, []byte(`{}`), "motivo"); err == nil {
		t.Fatal("se esperaba que se propagara el error de Publish")
	}
}

func TestAMQPDeadLetterPublisher_PropagaElErrorDeWaitContext(t *testing.T) {
	channel := &fakeAMQPChannel{publishConfirmation: fakeDeferredConfirmation{err: context.DeadlineExceeded}}
	publisher := NewAMQPDeadLetterPublisher(channel)

	if err := publisher.PublishToDLQ(context.Background(), testAlertQueue, []byte(`{}`), "motivo"); err == nil {
		t.Fatal("se esperaba que se propagara el error de WaitContext")
	}
}
