package dispatch

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/matiasfelau/vinculo-vial-notification/internal/domain"
	"github.com/matiasfelau/vinculo-vial-notification/internal/repository/memory"
)

func setupNotifyInbox() (*NotifyInboxService, *memory.DeviceTokenRepository, *memory.NotificationLogRepository, *fakeDispatcher) {
	deviceTokens := memory.NewDeviceTokenRepository()
	notificationLogs := memory.NewNotificationLogRepository()
	dispatcher := &fakeDispatcher{}
	service := NewNotifyInboxService(memory.NewUnitOfWork(notificationLogs, deviceTokens), deviceTokens, dispatcher)
	return service, deviceTokens, notificationLogs, dispatcher
}

func TestNotifyInboxService_EnviaElTextoDelContratoSoloALosDuenos(t *testing.T) {
	service, deviceTokens, _, dispatcher := setupNotifyInbox()
	owner, other := uuid.New(), uuid.New()
	addToken(t, deviceTokens, owner, "token-dueno")
	addToken(t, deviceTokens, other, "token-ajeno")

	err := service.Execute(context.Background(), PendingDeliveredEvent{EventID: uuid.New(), OwnerIDs: []uuid.UUID{owner}})
	if err != nil {
		t.Fatalf("Execute() devolvio un error inesperado: %v", err)
	}

	if len(dispatcher.calls) != 1 || len(dispatcher.calls[0]) != 1 || dispatcher.calls[0][0] != "token-dueno" {
		t.Fatalf("se esperaba un unico despacho a token-dueno, se obtuvo %v", dispatcher.calls)
	}
	message := dispatcher.messages[0]
	if message.Body != "Tenés alertas sin leer en el buzón." {
		t.Errorf("Body = %q", message.Body)
	}
	if message.Data["type"] != "INBOX_UPDATED" || len(message.Data) != 1 {
		t.Errorf("Data = %v, se esperaba solo type=INBOX_UPDATED", message.Data)
	}
}

func TestNotifyInboxService_VariosDuenos_UnSoloDespachoConTodosSusTokens(t *testing.T) {
	service, deviceTokens, _, dispatcher := setupNotifyInbox()
	first, second := uuid.New(), uuid.New()
	addToken(t, deviceTokens, first, "token-1")
	addToken(t, deviceTokens, second, "token-2")

	err := service.Execute(context.Background(), PendingDeliveredEvent{EventID: uuid.New(), OwnerIDs: []uuid.UUID{first, second}})
	if err != nil {
		t.Fatalf("Execute() devolvio un error inesperado: %v", err)
	}

	if len(dispatcher.calls) != 1 || len(dispatcher.calls[0]) != 2 {
		t.Fatalf("se esperaba un unico despacho a los dos tokens, se obtuvo %v", dispatcher.calls)
	}
}

func TestNotifyInboxService_DuenoSinTokens_AckSinErrorNiDespacho(t *testing.T) {
	service, _, notificationLogs, dispatcher := setupNotifyInbox()

	eventID := uuid.New()
	err := service.Execute(context.Background(), PendingDeliveredEvent{EventID: eventID, OwnerIDs: []uuid.UUID{uuid.New()}})
	if err != nil {
		t.Fatalf("Execute() devolvio un error inesperado: %v", err)
	}

	if len(dispatcher.calls) != 0 {
		t.Errorf("no deberia despacharse nada sin tokens, se hicieron %d envios", len(dispatcher.calls))
	}
	stored, ok := notificationLogs.Get(eventID)
	if !ok || stored.Status != domain.StatusSkippedNoToken {
		t.Errorf("el evento deberia quedar registrado como %s, quedo %+v", domain.StatusSkippedNoToken, stored)
	}
}

func TestNotifyInboxService_EventoRepetido_NoVuelveAEnviar(t *testing.T) {
	service, deviceTokens, _, dispatcher := setupNotifyInbox()
	owner := uuid.New()
	addToken(t, deviceTokens, owner, "token-dueno")

	event := PendingDeliveredEvent{EventID: uuid.New(), OwnerIDs: []uuid.UUID{owner}}
	for i := 0; i < 2; i++ {
		if err := service.Execute(context.Background(), event); err != nil {
			t.Fatalf("Execute() #%d devolvio un error inesperado: %v", i+1, err)
		}
	}

	if len(dispatcher.calls) != 1 {
		t.Errorf("se esperaba un unico despacho pese al redelivery, se hicieron %d", len(dispatcher.calls))
	}
}

func TestNotifyInboxService_FalloDeFCM_DevuelveErrorYNoMarcaElEvento(t *testing.T) {
	service, deviceTokens, notificationLogs, dispatcher := setupNotifyInbox()
	owner := uuid.New()
	addToken(t, deviceTokens, owner, "token-dueno")
	dispatcher.err = errors.New("fcm no disponible")

	event := PendingDeliveredEvent{EventID: uuid.New(), OwnerIDs: []uuid.UUID{owner}}
	if err := service.Execute(context.Background(), event); err == nil {
		t.Fatal("se esperaba un error cuando FCM falla")
	}
	if _, ok := notificationLogs.Get(event.EventID); ok {
		t.Errorf("el evento no deberia quedar registrado: el reintento tiene que volver a enviar")
	}
}
