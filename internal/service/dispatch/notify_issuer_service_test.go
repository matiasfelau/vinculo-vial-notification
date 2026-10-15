package dispatch

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/matiasfelau/vinculo-vial-notification/internal/domain"
	"github.com/matiasfelau/vinculo-vial-notification/internal/repository/memory"
)

func setupNotifyIssuer() (*NotifyIssuerService, *memory.DeviceTokenRepository, *memory.NotificationLogRepository, *fakeDispatcher) {
	deviceTokens := memory.NewDeviceTokenRepository()
	notificationLogs := memory.NewNotificationLogRepository()
	dispatcher := &fakeDispatcher{}
	service := NewNotifyIssuerService(memory.NewUnitOfWork(notificationLogs, deviceTokens), deviceTokens, dispatcher)
	return service, deviceTokens, notificationLogs, dispatcher
}

func TestNotifyIssuerService_EnviaElTextoDelContratoAlEmisor(t *testing.T) {
	cases := []struct {
		notice IssuerNotice
		body   string
	}{
		{NoticeValidated, "Tu alerta fue validada. Le avisé al dueño."},
		{NoticeSeen, "El dueño leyó tu mensaje."},
		{NoticeRejected, "No pudimos validar tu alerta."},
		{NoticeExpired, "Tu alerta no tuvo respuesta."},
	}

	for _, tc := range cases {
		t.Run(string(tc.notice), func(t *testing.T) {
			service, deviceTokens, _, dispatcher := setupNotifyIssuer()
			issuer, other, alertID := uuid.New(), uuid.New(), uuid.New()
			addToken(t, deviceTokens, issuer, "token-emisor")
			addToken(t, deviceTokens, other, "token-ajeno")

			err := service.Execute(context.Background(), IssuerNoticeEvent{EventID: uuid.New(), IssuerID: issuer, AlertID: alertID, Notice: tc.notice})
			if err != nil {
				t.Fatalf("Execute() devolvio un error inesperado: %v", err)
			}

			if len(dispatcher.calls) != 1 || len(dispatcher.calls[0]) != 1 || dispatcher.calls[0][0] != "token-emisor" {
				t.Fatalf("se esperaba un unico despacho a token-emisor, se obtuvo %v", dispatcher.calls)
			}
			message := dispatcher.messages[0]
			if message.Title != "ViVIAna" || message.Body != tc.body {
				t.Errorf("mensaje = %q / %q, se esperaba ViVIAna / %q", message.Title, message.Body, tc.body)
			}
			if message.Data["type"] != "SENT_UPDATED" || message.Data["alert_id"] != alertID.String() || len(message.Data) != 2 {
				t.Errorf("Data = %v, se esperaba type=SENT_UPDATED y alert_id", message.Data)
			}
		})
	}
}

func TestNotifyIssuerService_EmisorSinTokens_AckSinErrorNiDespacho(t *testing.T) {
	service, _, notificationLogs, dispatcher := setupNotifyIssuer()
	event := IssuerNoticeEvent{EventID: uuid.New(), IssuerID: uuid.New(), AlertID: uuid.New(), Notice: NoticeRejected}

	if err := service.Execute(context.Background(), event); err != nil {
		t.Fatalf("Execute() devolvio un error inesperado: %v", err)
	}

	if len(dispatcher.calls) != 0 {
		t.Fatalf("no deberia haberse despachado nada, se hicieron %d llamadas", len(dispatcher.calls))
	}
	stored, ok := notificationLogs.Get(event.EventID)
	if !ok || stored.Status != domain.StatusSkippedNoToken {
		t.Fatalf("se esperaba SKIPPED_NO_TOKEN, se obtuvo %+v (existe=%v)", stored, ok)
	}
	if stored.RecipientID != nil {
		t.Error("el registro no deberia guardar al emisor como destinatario")
	}
}

func TestNotifyIssuerService_MismoEventIDEntregadoDosVeces_SoloUnDespacho(t *testing.T) {
	service, deviceTokens, _, dispatcher := setupNotifyIssuer()
	issuer := uuid.New()
	addToken(t, deviceTokens, issuer, "token-emisor")
	event := IssuerNoticeEvent{EventID: uuid.New(), IssuerID: issuer, AlertID: uuid.New(), Notice: NoticeSeen}

	for i := 0; i < 2; i++ {
		if err := service.Execute(context.Background(), event); err != nil {
			t.Fatalf("Execute() #%d devolvio un error inesperado: %v", i+1, err)
		}
	}

	if dispatcher.callCount() != 1 {
		t.Fatalf("se esperaba un unico despacho, se hicieron %d", dispatcher.callCount())
	}
}

func TestNotifyIssuerService_FalloDeFCM_PropagaElErrorYElReintentoVuelveAEnviar(t *testing.T) {
	service, deviceTokens, notificationLogs, dispatcher := setupNotifyIssuer()
	dispatcher.err = errors.New("fcm no disponible")
	dispatcher.failFirst = 1
	issuer := uuid.New()
	addToken(t, deviceTokens, issuer, "token-emisor")
	event := IssuerNoticeEvent{EventID: uuid.New(), IssuerID: issuer, AlertID: uuid.New(), Notice: NoticeExpired}

	if err := service.Execute(context.Background(), event); err == nil {
		t.Fatal("se esperaba que Execute() propagara el error del dispatcher")
	}
	if _, ok := notificationLogs.Get(event.EventID); ok {
		t.Fatal("tras un fallo de FCM el event_id no deberia estar registrado")
	}

	if err := service.Execute(context.Background(), event); err != nil {
		t.Fatalf("Execute() del reintento devolvio un error inesperado: %v", err)
	}
	if dispatcher.callCount() != 2 {
		t.Fatalf("se esperaban 2 intentos de despacho, se hicieron %d", dispatcher.callCount())
	}
}

func TestNotifyIssuerService_BorraLosTokensInvalidosReportadosPorFCM(t *testing.T) {
	service, deviceTokens, _, dispatcher := setupNotifyIssuer()
	issuer := uuid.New()
	addToken(t, deviceTokens, issuer, "token-valido")
	addToken(t, deviceTokens, issuer, "token-invalido")
	dispatcher.invalidTokens = map[string]bool{"token-invalido": true}

	err := service.Execute(context.Background(), IssuerNoticeEvent{EventID: uuid.New(), IssuerID: issuer, AlertID: uuid.New(), Notice: NoticeSeen})
	if err != nil {
		t.Fatalf("Execute() devolvio un error inesperado: %v", err)
	}

	restantes, err := deviceTokens.FindByOwnerIDs(context.Background(), []uuid.UUID{issuer})
	if err != nil {
		t.Fatalf("FindByOwnerIDs() devolvio un error inesperado: %v", err)
	}
	if len(restantes) != 1 || restantes[0].FCMToken != "token-valido" {
		t.Fatalf("se esperaba que solo sobreviviera token-valido, se obtuvo: %+v", restantes)
	}
}
