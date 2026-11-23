//go:build integration

// Pruebas de integracion contra un RabbitMQ real con la topologia de
// retry y DLQ por cola que declara vinculo-vial-core. Se excluyen del
// "go test ./..." comun.
//
// Requisitos:
//
//   - TEST_RABBITMQ_URL apuntando al broker, con la topologia de Core ya
//     declarada (este test no declara nada) y RETRY_TTL_MS bajo (1000 en
//     local) para que el ciclo de reintentos no tarde.
//
//   - Ningun otro consumidor sobre notification.alert-created.q ni
//     notification.sessions-revoked.q (por ejemplo, el contenedor del
//     servicio detenido): competiria por los mensajes del test.
//
//     TEST_RABBITMQ_URL=amqp://guest:guest@localhost:5672/ \
//     go test -tags=integration -count=1 -run Integracion ./internal/delivery/worker/
package worker

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/matiasfelau/vinculo-vial-notification/internal/domain"
	"github.com/matiasfelau/vinculo-vial-notification/internal/repository/memory"
	"github.com/matiasfelau/vinculo-vial-notification/internal/service/dispatch"
	"github.com/matiasfelau/vinculo-vial-notification/internal/service/sessions"
)

const (
	integrationMaxRetries  = 3
	integrationPrefetch    = 10
	integrationDLQWait     = 30 * time.Second
	integrationDLQPoll     = 200 * time.Millisecond
	integrationShutdownMax = 10 * time.Second
)

func integrationRabbitURL(t *testing.T) string {
	t.Helper()

	url := os.Getenv("TEST_RABBITMQ_URL")
	if url == "" {
		// Con el tag integration el llamador pidio explicitamente estas
		// pruebas: saltearlas en silencio ocultaria un CI mal configurado.
		t.Fatal("TEST_RABBITMQ_URL no esta definida: las pruebas de integracion requieren un RabbitMQ real")
	}
	return url
}

// deliveryRecorder envuelve un AMQPChannel y registra los headers de
// cada delivery de la cola consumida, para poder inspeccionar el
// x-death que trajo el mensaje en cada vuelta del ciclo de reintentos.
// El mensaje que llega a la DLQ es una publicacion nueva y no hereda
// x-death: la unica forma de observar el ciclo es en las entregas.
type deliveryRecorder struct {
	AMQPChannel

	mu      sync.Mutex
	headers []amqp.Table
}

func (r *deliveryRecorder) Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
	source, err := r.AMQPChannel.Consume(queue, consumer, autoAck, exclusive, noLocal, noWait, args)
	if err != nil {
		return nil, err
	}

	out := make(chan amqp.Delivery)
	go func() {
		defer close(out)
		for delivery := range source {
			r.mu.Lock()
			r.headers = append(r.headers, delivery.Headers)
			r.mu.Unlock()
			out <- delivery
		}
	}()
	return out, nil
}

func (r *deliveryRecorder) deliveries() []amqp.Table {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]amqp.Table(nil), r.headers...)
}

// deathEntry devuelve la entrada de x-death de la cola y razon dadas,
// o nil si no existe.
func deathEntry(headers amqp.Table, queue, reason string) amqp.Table {
	deaths, _ := headers["x-death"].([]interface{})
	for _, raw := range deaths {
		entry, ok := raw.(amqp.Table)
		if !ok {
			continue
		}
		if entryQueue, _ := entry["queue"].(string); entryQueue != queue {
			continue
		}
		if entryReason, _ := entry["reason"].(string); entryReason == reason {
			return entry
		}
	}
	return nil
}

// awaitDeadLetter espera a que aparezca en dlqQueue el mensaje con el
// eventID dado y lo confirma (limpia la DLQ). Los mensajes ajenos
// (restos de otras corridas) se devuelven a la cola sin tocarlos.
func awaitDeadLetter(t *testing.T, channel *amqp.Channel, dlqQueue, eventID string) amqp.Delivery {
	t.Helper()

	deadline := time.Now().Add(integrationDLQWait)
	for time.Now().Before(deadline) {
		delivery, ok, err := channel.Get(dlqQueue, false)
		if err != nil {
			t.Fatalf("no se pudo leer %s: %v", dlqQueue, err)
		}
		if !ok {
			time.Sleep(integrationDLQPoll)
			continue
		}

		envelope, parseErr := ParseEnvelope(delivery.Body)
		if parseErr != nil || envelope.EventID.String() != eventID {
			if err := delivery.Nack(false, true); err != nil {
				t.Fatalf("no se pudo devolver a %s un mensaje ajeno: %v", dlqQueue, err)
			}
			time.Sleep(integrationDLQPoll)
			continue
		}

		if err := delivery.Ack(false); err != nil {
			t.Fatalf("no se pudo confirmar el mensaje de %s: %v", dlqQueue, err)
		}
		return delivery
	}

	t.Fatalf("el mensaje no aparecio en %s tras %s", dlqQueue, integrationDLQWait)
	return amqp.Delivery{}
}

// runExhaustedRetriesScenario publica en workQueue un mensaje que el
// handler no puede procesar (fallo transitorio) y verifica que agote
// MAX_RETRIES recorriendo el ciclo completo (cola de trabajo -> retry
// -> cola de trabajo) y termine en dlqQueue. Devuelve el mensaje
// tal como quedo en la DLQ.
func runExhaustedRetriesScenario(t *testing.T, workQueue, retryQueue, dlqQueue string, newHandler func(deadLetters deadLetterPublisher) messageHandler, body []byte, eventID string) amqp.Delivery {
	t.Helper()

	url := integrationRabbitURL(t)

	consumerConn, rawChannel, err := dial(url, integrationPrefetch)
	if err != nil {
		t.Fatalf("no se pudo conectar el consumer al broker: %v", err)
	}
	defer consumerConn.Close()
	defer rawChannel.Close()

	recorder := &deliveryRecorder{AMQPChannel: rawChannel}
	deadLetters := NewAMQPDeadLetterPublisher(recorder)
	handler := newHandler(deadLetters)

	probeConn, err := amqp.Dial(url)
	if err != nil {
		t.Fatalf("no se pudo conectar la sonda al broker: %v", err)
	}
	defer probeConn.Close()
	probe, err := probeConn.Channel()
	if err != nil {
		t.Fatalf("no se pudo abrir el canal de la sonda: %v", err)
	}
	defer probe.Close()
	if err := probe.Confirm(false); err != nil {
		t.Fatalf("no se pudo habilitar confirm en la sonda: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	consumer := NewConsumer(recorder, workQueue, handler)
	go func() { runDone <- consumer.Run(ctx) }()
	defer func() {
		cancel()
		select {
		case <-runDone:
		case <-time.After(integrationShutdownMax):
			t.Error("el consumer no termino tras cancelar el contexto")
		}
	}()

	// Publicacion por el exchange default con la cola como routing key:
	// llega solo a esta cola y no a otros consumidores del evento.
	confirmation, err := probe.PublishWithDeferredConfirmWithContext(ctx, "", workQueue, true, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Body:         body,
	})
	if err != nil {
		t.Fatalf("no se pudo publicar en %s: %v", workQueue, err)
	}
	if acked, err := confirmation.WaitContext(ctx); err != nil || !acked {
		t.Fatalf("el broker no confirmo la publicacion en %s (acked=%v, err=%v)", workQueue, acked, err)
	}

	dead := awaitDeadLetter(t, probe, dlqQueue, eventID)

	if dead.Exchange != dlqExchange {
		t.Errorf("exchange del mensaje en la dlq = %q, se esperaba %q", dead.Exchange, dlqExchange)
	}
	if dead.RoutingKey != workQueue {
		t.Errorf("routing key del mensaje en la dlq = %q, se esperaba la cola de trabajo %q", dead.RoutingKey, workQueue)
	}
	if _, ok := dead.Headers["x-discard-reason"]; !ok {
		t.Error("el mensaje de la dlq no trae x-discard-reason")
	}

	deliveries := recorder.deliveries()
	if len(deliveries) != integrationMaxRetries+1 {
		t.Fatalf("entregas de %s = %d, se esperaban %d (1 inicial + %d reintentos)", workQueue, len(deliveries), integrationMaxRetries+1, integrationMaxRetries)
	}

	// La ultima entrega es la que supero MAX_RETRIES y se descarto: su
	// x-death prueba que cada reintento vuelve a la cola de trabajo por
	// el exchange default tras expirar en la .retry.
	last := deliveries[len(deliveries)-1]
	rejected := deathEntry(last, workQueue, "rejected")
	if rejected == nil {
		t.Fatalf("la ultima entrega no trae x-death de %s con reason=rejected: %v", workQueue, last["x-death"])
	}
	if count := asInt64(rejected["count"]); count != integrationMaxRetries {
		t.Errorf("x-death rejected de %s: count = %d, se esperaba MAX_RETRIES (%d)", workQueue, count, integrationMaxRetries)
	}
	expired := deathEntry(last, retryQueue, "expired")
	if expired == nil {
		t.Fatalf("la ultima entrega no trae x-death de %s con reason=expired: %v", retryQueue, last["x-death"])
	}
	if count := asInt64(expired["count"]); count != integrationMaxRetries {
		t.Errorf("x-death expired de %s: count = %d, se esperaba MAX_RETRIES (%d)", retryQueue, count, integrationMaxRetries)
	}
	if got := deathCount(last, workQueue); got != integrationMaxRetries {
		t.Errorf("deathCount(%s) = %d, se esperaba MAX_RETRIES (%d)", workQueue, got, integrationMaxRetries)
	}

	return dead
}

func TestIntegracionAlertCreated_AgotaReintentosYTerminaEnLaDLQ(t *testing.T) {
	const (
		workQueue  = "notification.alert-created.q"
		retryQueue = "notification.alert-created.retry"
		dlqQueue   = "notification.alert-created.dlq"
	)

	ownerID := uuid.New()
	eventID := uuid.New().String()

	newHandler := func(deadLetters deadLetterPublisher) messageHandler {
		deviceTokens := memory.NewDeviceTokenRepository()
		if err := deviceTokens.Upsert(context.Background(), &domain.DeviceToken{ID: uuid.New(), OwnerID: ownerID, FCMToken: "token-1", UpdatedAt: time.Now()}); err != nil {
			t.Fatalf("no se pudo preparar el token del test: %v", err)
		}
		service := dispatch.NewDispatchAlertService(
			memory.NewUnitOfWork(memory.NewNotificationLogRepository(), deviceTokens),
			deviceTokens,
			&fakeDispatcher{err: errors.New("fcm no disponible")},
		)
		return NewAlertCreatedHandler(service, deadLetters, workQueue, integrationMaxRetries)
	}

	runExhaustedRetriesScenario(t, workQueue, retryQueue, dlqQueue, newHandler, validEnvelope(eventID, ownerID.String()), eventID)
}

func TestIntegracionSessionsRevoked_AgotaReintentosYTerminaEnLaDLQ(t *testing.T) {
	const (
		workQueue  = "notification.sessions-revoked.q"
		retryQueue = "notification.sessions-revoked.retry"
		dlqQueue   = "notification.sessions-revoked.dlq"
	)

	eventID := uuid.New().String()

	newHandler := func(deadLetters deadLetterPublisher) messageHandler {
		repo := &failingDeviceTokenRepository{DeviceTokenRepository: memory.NewDeviceTokenRepository(), err: errors.New("base caida")}
		service := sessions.NewRevokeSessionsService(memory.NewUnitOfWork(memory.NewNotificationLogRepository(), repo))
		return NewSessionsRevokedHandler(service, deadLetters, workQueue, integrationMaxRetries)
	}

	runExhaustedRetriesScenario(t, workQueue, retryQueue, dlqQueue, newHandler, sessionsRevokedEnvelope(eventID, uuid.New().String()), eventID)
}
