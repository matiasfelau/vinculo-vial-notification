package dispatch

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/matiasfelau/vinculo-vial-notification/internal/domain"
	"github.com/matiasfelau/vinculo-vial-notification/internal/repository/memory"
)

// fakeDispatcher registra cada llamada para poder verificar cuantas
// veces se disparo el envio y con que tokens.
type fakeDispatcher struct {
	calls [][]string
	err   error
}

func (f *fakeDispatcher) Dispatch(_ context.Context, tokens []string, _ string) ([]domain.DispatchOutcome, error) {
	f.calls = append(f.calls, tokens)
	if f.err != nil {
		return nil, f.err
	}

	outcomes := make([]domain.DispatchOutcome, len(tokens))
	for i, token := range tokens {
		outcomes[i] = domain.DispatchOutcome{Token: token, Success: true}
	}
	return outcomes, nil
}

func setup() (*DispatchAlertService, *memory.DeviceTokenRepository, *memory.NotificationLogRepository, *fakeDispatcher) {
	deviceTokens := memory.NewDeviceTokenRepository()
	notificationLogs := memory.NewNotificationLogRepository()
	dispatcher := &fakeDispatcher{}
	service := NewDispatchAlertService(notificationLogs, deviceTokens, dispatcher)
	return service, deviceTokens, notificationLogs, dispatcher
}

func TestDispatchAlertService_DespachaALosTokensDeLosDestinatarios(t *testing.T) {
	service, deviceTokens, _, dispatcher := setup()
	ctx := context.Background()

	owner := uuid.New()
	_ = deviceTokens.Upsert(ctx, &domain.DeviceToken{ID: uuid.New(), OwnerID: owner, FCMToken: "token-1", UpdatedAt: time.Now()})

	event := AlertCreatedEvent{EventID: uuid.New(), OwnerIDs: []uuid.UUID{owner}, AlertType: "choque"}
	if err := service.Execute(ctx, event); err != nil {
		t.Fatalf("Execute() devolvio un error inesperado: %v", err)
	}

	if len(dispatcher.calls) != 1 || len(dispatcher.calls[0]) != 1 || dispatcher.calls[0][0] != "token-1" {
		t.Fatalf("se esperaba una llamada de despacho con [token-1], se obtuvo: %v", dispatcher.calls)
	}
}

func TestDispatchAlertService_MismoEventIDEntregadoDosVeces_SoloUnDespacho(t *testing.T) {
	service, deviceTokens, _, dispatcher := setup()
	ctx := context.Background()

	owner := uuid.New()
	_ = deviceTokens.Upsert(ctx, &domain.DeviceToken{ID: uuid.New(), OwnerID: owner, FCMToken: "token-1", UpdatedAt: time.Now()})

	event := AlertCreatedEvent{EventID: uuid.New(), OwnerIDs: []uuid.UUID{owner}, AlertType: "choque"}

	if err := service.Execute(ctx, event); err != nil {
		t.Fatalf("Execute() del primer intento devolvio un error inesperado: %v", err)
	}
	if err := service.Execute(ctx, event); err != nil {
		t.Fatalf("Execute() del redelivery devolvio un error inesperado: %v", err)
	}

	if len(dispatcher.calls) != 1 {
		t.Fatalf("se esperaba un unico despacho, se hicieron %d", len(dispatcher.calls))
	}
}

func TestDispatchAlertService_SinTokens_NoDespachaYRegistraSkipped(t *testing.T) {
	service, _, notificationLogs, dispatcher := setup()
	ctx := context.Background()

	event := AlertCreatedEvent{EventID: uuid.New(), OwnerIDs: []uuid.UUID{uuid.New()}, AlertType: "choque"}
	if err := service.Execute(ctx, event); err != nil {
		t.Fatalf("Execute() devolvio un error inesperado: %v", err)
	}

	if len(dispatcher.calls) != 0 {
		t.Fatalf("no deberia haberse despachado nada, se hicieron %d llamadas", len(dispatcher.calls))
	}

	stored, ok := notificationLogs.Get(event.EventID)
	if !ok {
		t.Fatalf("se esperaba un registro para el evento %s", event.EventID)
	}
	if stored.Status != domain.StatusSkippedNoToken {
		t.Fatalf("Status = %q, se esperaba %q", stored.Status, domain.StatusSkippedNoToken)
	}
}

func TestDispatchAlertService_RegistraSentTrasUnDespachoExitoso(t *testing.T) {
	service, deviceTokens, notificationLogs, _ := setup()
	ctx := context.Background()

	owner := uuid.New()
	_ = deviceTokens.Upsert(ctx, &domain.DeviceToken{ID: uuid.New(), OwnerID: owner, FCMToken: "token-1", UpdatedAt: time.Now()})

	event := AlertCreatedEvent{EventID: uuid.New(), OwnerIDs: []uuid.UUID{owner}, AlertType: "choque"}
	if err := service.Execute(ctx, event); err != nil {
		t.Fatalf("Execute() devolvio un error inesperado: %v", err)
	}

	stored, ok := notificationLogs.Get(event.EventID)
	if !ok {
		t.Fatalf("se esperaba un registro para el evento %s", event.EventID)
	}
	if stored.Status != domain.StatusSent {
		t.Fatalf("Status = %q, se esperaba %q", stored.Status, domain.StatusSent)
	}
}

func TestDispatchAlertService_PropagaElErrorDelDispatcherYNoMarcaSent(t *testing.T) {
	service, deviceTokens, notificationLogs, dispatcher := setup()
	dispatcher.err = errors.New("fcm no disponible")
	ctx := context.Background()

	owner := uuid.New()
	_ = deviceTokens.Upsert(ctx, &domain.DeviceToken{ID: uuid.New(), OwnerID: owner, FCMToken: "token-1", UpdatedAt: time.Now()})

	event := AlertCreatedEvent{EventID: uuid.New(), OwnerIDs: []uuid.UUID{owner}, AlertType: "choque"}
	if err := service.Execute(ctx, event); err == nil {
		t.Fatal("se esperaba que Execute() propagara el error del dispatcher")
	}

	stored, ok := notificationLogs.Get(event.EventID)
	if !ok {
		t.Fatalf("se esperaba un registro para el evento %s", event.EventID)
	}
	if stored.Status == domain.StatusSent {
		t.Fatalf("el status no deberia haberse marcado como SENT tras un fallo del dispatcher")
	}
}
