package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/matiasfelau/vinculo-vial-notification/internal/domain"
	"github.com/matiasfelau/vinculo-vial-notification/internal/repository/memory"
	"github.com/matiasfelau/vinculo-vial-notification/internal/service/dispatch"
)

// fakeAcknowledger permite verificar, sin un canal AMQP real, si el
// consumer confirmo o rechazo un delivery y con que parametros.
type fakeAcknowledger struct {
	acked          []uint64
	nacked         []uint64
	nackedRequeue  []bool
	nackedMultiple []bool
}

func (f *fakeAcknowledger) Ack(tag uint64, _ bool) error {
	f.acked = append(f.acked, tag)
	return nil
}

func (f *fakeAcknowledger) Nack(tag uint64, multiple, requeue bool) error {
	f.nacked = append(f.nacked, tag)
	f.nackedMultiple = append(f.nackedMultiple, multiple)
	f.nackedRequeue = append(f.nackedRequeue, requeue)
	return nil
}

func (f *fakeAcknowledger) Reject(_ uint64, _ bool) error {
	return nil
}

func TestAlertCreatedConsumer_ProcessConfirmaUnEventoValido(t *testing.T) {
	deviceTokens := memory.NewDeviceTokenRepository()
	ownerID := uuid.New()
	_ = deviceTokens.Upsert(context.Background(), &domain.DeviceToken{ID: uuid.New(), OwnerID: ownerID, FCMToken: "token-1", UpdatedAt: time.Now()})

	service := dispatch.NewDispatchAlertService(memory.NewUnitOfWork(memory.NewNotificationLogRepository(), deviceTokens), deviceTokens, &fakeDispatcher{})
	handler := NewAlertCreatedHandler(service, &fakeDeadLetterPublisher{}, testAlertQueue, 3)
	consumer := NewConsumer(nil, "notification.alert-created.q", handler)

	ack := &fakeAcknowledger{}
	delivery := amqp.Delivery{
		Acknowledger: ack,
		DeliveryTag:  1,
		Body:         validEnvelope(uuid.New().String(), ownerID.String()),
	}

	consumer.process(context.Background(), delivery)

	if len(ack.acked) != 1 || ack.acked[0] != 1 {
		t.Fatalf("se esperaba Ack(1), se obtuvo acked=%v nacked=%v", ack.acked, ack.nacked)
	}
}

func TestAlertCreatedConsumer_ProcessRechazaSinRequeueAnteFalloTransitorio(t *testing.T) {
	deviceTokens := memory.NewDeviceTokenRepository()
	ownerID := uuid.New()
	_ = deviceTokens.Upsert(context.Background(), &domain.DeviceToken{ID: uuid.New(), OwnerID: ownerID, FCMToken: "token-1", UpdatedAt: time.Now()})

	service := dispatch.NewDispatchAlertService(memory.NewUnitOfWork(memory.NewNotificationLogRepository(), deviceTokens), deviceTokens, &fakeDispatcher{err: errors.New("fcm no disponible")})
	handler := NewAlertCreatedHandler(service, &fakeDeadLetterPublisher{}, testAlertQueue, 3)
	consumer := NewConsumer(nil, "notification.alert-created.q", handler)

	ack := &fakeAcknowledger{}
	delivery := amqp.Delivery{
		Acknowledger: ack,
		DeliveryTag:  7,
		Body:         validEnvelope(uuid.New().String(), ownerID.String()),
	}

	consumer.process(context.Background(), delivery)

	if len(ack.nacked) != 1 || ack.nacked[0] != 7 {
		t.Fatalf("se esperaba Nack(7), se obtuvo acked=%v nacked=%v", ack.acked, ack.nacked)
	}
	if ack.nackedRequeue[0] {
		t.Fatal("el nack no debe reencolar de forma inmediata")
	}
	if len(ack.acked) != 0 {
		t.Fatalf("no deberia haberse confirmado el mensaje, acked=%v", ack.acked)
	}
}

func TestAlertCreatedConsumer_ProcessConfirmaYDescartaEnvelopeInvalido(t *testing.T) {
	service := dispatch.NewDispatchAlertService(memory.NewUnitOfWork(memory.NewNotificationLogRepository(), memory.NewDeviceTokenRepository()), memory.NewDeviceTokenRepository(), &fakeDispatcher{})
	deadLetters := &fakeDeadLetterPublisher{}
	handler := NewAlertCreatedHandler(service, deadLetters, testAlertQueue, 3)
	consumer := NewConsumer(nil, "notification.alert-created.q", handler)

	ack := &fakeAcknowledger{}
	delivery := amqp.Delivery{
		Acknowledger: ack,
		DeliveryTag:  3,
		Body:         []byte(`{no es json`),
	}

	consumer.process(context.Background(), delivery)

	if len(ack.acked) != 1 || ack.acked[0] != 3 {
		t.Fatalf("se esperaba Ack(3), se obtuvo acked=%v nacked=%v", ack.acked, ack.nacked)
	}
	if len(deadLetters.published) != 1 {
		t.Fatalf("se esperaba un mensaje publicado a la dlq, se publicaron %d", len(deadLetters.published))
	}
}

func TestAlertCreatedConsumer_ProcessLeeXDeathYCortaAlSuperarMaxRetries(t *testing.T) {
	const queue = "notification.alert-created.q"

	deviceTokens := memory.NewDeviceTokenRepository()
	ownerID := uuid.New()
	_ = deviceTokens.Upsert(context.Background(), &domain.DeviceToken{ID: uuid.New(), OwnerID: ownerID, FCMToken: "token-1", UpdatedAt: time.Now()})

	service := dispatch.NewDispatchAlertService(memory.NewUnitOfWork(memory.NewNotificationLogRepository(), deviceTokens), deviceTokens, &fakeDispatcher{err: errors.New("fcm no disponible")})
	deadLetters := &fakeDeadLetterPublisher{}
	handler := NewAlertCreatedHandler(service, deadLetters, testAlertQueue, 3)
	consumer := NewConsumer(nil, queue, handler)

	ack := &fakeAcknowledger{}
	delivery := amqp.Delivery{
		Acknowledger: ack,
		DeliveryTag:  9,
		Body:         validEnvelope(uuid.New().String(), ownerID.String()),
		Headers: amqp.Table{
			"x-death": []interface{}{
				amqp.Table{"queue": queue, "reason": "rejected", "count": int64(3)},
			},
		},
	}

	consumer.process(context.Background(), delivery)

	if len(ack.acked) != 1 || ack.acked[0] != 9 {
		t.Fatalf("se esperaba Ack(9) al superar maxRetries, se obtuvo acked=%v nacked=%v", ack.acked, ack.nacked)
	}
	if len(deadLetters.published) != 1 {
		t.Fatalf("se esperaba un mensaje publicado a la dlq, se publicaron %d", len(deadLetters.published))
	}
}
