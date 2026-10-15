package dispatch

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/matiasfelau/vinculo-vial-notification/internal/domain"
	"github.com/matiasfelau/vinculo-vial-notification/internal/repository/memory"
)

func addToken(t *testing.T, repo *memory.DeviceTokenRepository, owner uuid.UUID, token string) {
	t.Helper()
	if err := repo.Upsert(context.Background(), &domain.DeviceToken{ID: uuid.New(), OwnerID: owner, FCMToken: token, UpdatedAt: time.Now()}); err != nil {
		t.Fatalf("Upsert() devolvio un error inesperado: %v", err)
	}
}

func TestDispatchAlertService_AvisaALosDuenosYAlEmisor(t *testing.T) {
	service, deviceTokens, _, dispatcher := setup()
	owner, issuer, alertID := uuid.New(), uuid.New(), uuid.New()
	addToken(t, deviceTokens, owner, "token-dueno")
	addToken(t, deviceTokens, issuer, "token-emisor")

	event := AlertCreatedEvent{EventID: uuid.New(), OwnerIDs: []uuid.UUID{owner}, AlertType: "CRASHED", AlertID: alertID, IssuerID: issuer}
	if err := service.Execute(context.Background(), event); err != nil {
		t.Fatalf("Execute() devolvio un error inesperado: %v", err)
	}

	if len(dispatcher.calls) != 2 {
		t.Fatalf("se esperaban 2 despachos (duenos y emisor), se hicieron %d", len(dispatcher.calls))
	}

	if dispatcher.calls[0][0] != "token-dueno" {
		t.Errorf("el primer despacho fue a %v, se esperaba token-dueno", dispatcher.calls[0])
	}
	ownerMsg := dispatcher.messages[0]
	if ownerMsg.Title != "Alerta de Vinculo Vial" || ownerMsg.Body != "Tenes una nueva alerta. Abri la app para ver el detalle." {
		t.Errorf("texto del aviso a los duenos cambio: %+v", ownerMsg)
	}
	if ownerMsg.Data["alert_type"] != "CRASHED" || ownerMsg.Data["alert_id"] != alertID.String() || len(ownerMsg.Data) != 2 {
		t.Errorf("Data del aviso a los duenos = %v, se esperaba alert_type y alert_id", ownerMsg.Data)
	}

	if dispatcher.calls[1][0] != "token-emisor" || len(dispatcher.calls[1]) != 1 {
		t.Errorf("el segundo despacho fue a %v, se esperaba solo token-emisor", dispatcher.calls[1])
	}
	issuerMsg := dispatcher.messages[1]
	if issuerMsg.Title != "ViVIAna" || issuerMsg.Body != "Tu alerta fue validada. Le avisé al dueño." {
		t.Errorf("texto del aviso al emisor = %+v", issuerMsg)
	}
	if issuerMsg.Data["type"] != "SENT_UPDATED" || issuerMsg.Data["alert_id"] != alertID.String() || len(issuerMsg.Data) != 2 {
		t.Errorf("Data del aviso al emisor = %v, se esperaba type=SENT_UPDATED y alert_id", issuerMsg.Data)
	}
}

func TestDispatchAlertService_NingunTextoNiDataDelAvisoALosDuenosContieneAlEmisor(t *testing.T) {
	service, deviceTokens, _, dispatcher := setup()
	owner, issuer := uuid.New(), uuid.New()
	addToken(t, deviceTokens, owner, "token-dueno")
	addToken(t, deviceTokens, issuer, "token-emisor")

	event := AlertCreatedEvent{EventID: uuid.New(), OwnerIDs: []uuid.UUID{owner}, AlertType: "CRASHED", AlertID: uuid.New(), IssuerID: issuer}
	if err := service.Execute(context.Background(), event); err != nil {
		t.Fatalf("Execute() devolvio un error inesperado: %v", err)
	}

	for i, message := range dispatcher.messages {
		serialized := message.Title + message.Body
		for k, v := range message.Data {
			serialized += k + v
		}
		if strings.Contains(serialized, issuer.String()) {
			t.Errorf("el mensaje %d contiene el id del emisor: %+v", i, message)
		}
	}
}

func TestDispatchAlertService_SinAlertID_NoMandaDataDeDeepLink(t *testing.T) {
	service, deviceTokens, _, dispatcher := setup()
	owner, issuer := uuid.New(), uuid.New()
	addToken(t, deviceTokens, owner, "token-dueno")
	addToken(t, deviceTokens, issuer, "token-emisor")

	event := AlertCreatedEvent{EventID: uuid.New(), OwnerIDs: []uuid.UUID{owner}, AlertType: "CRASHED", IssuerID: issuer}
	if err := service.Execute(context.Background(), event); err != nil {
		t.Fatalf("Execute() devolvio un error inesperado: %v", err)
	}

	if len(dispatcher.messages) != 2 {
		t.Fatalf("se esperaban 2 despachos, se hicieron %d", len(dispatcher.messages))
	}
	if _, ok := dispatcher.messages[0].Data["alert_id"]; ok {
		t.Errorf("el aviso a los duenos no deberia llevar alert_id: %v", dispatcher.messages[0].Data)
	}
	if dispatcher.messages[0].Data["alert_type"] != "CRASHED" {
		t.Errorf("el aviso a los duenos conserva alert_type: %v", dispatcher.messages[0].Data)
	}
	if len(dispatcher.messages[1].Data) != 0 {
		t.Errorf("el aviso al emisor no deberia llevar data de deep link: %v", dispatcher.messages[1].Data)
	}
}

func TestDispatchAlertService_EmisorSinTokens_AckSinErrorYAvisaALosDuenos(t *testing.T) {
	service, deviceTokens, notificationLogs, dispatcher := setup()
	owner, issuer := uuid.New(), uuid.New()
	addToken(t, deviceTokens, owner, "token-dueno")

	event := AlertCreatedEvent{EventID: uuid.New(), OwnerIDs: []uuid.UUID{owner}, AlertType: "CRASHED", AlertID: uuid.New(), IssuerID: issuer}
	if err := service.Execute(context.Background(), event); err != nil {
		t.Fatalf("Execute() devolvio un error inesperado: %v", err)
	}

	if len(dispatcher.calls) != 1 || dispatcher.calls[0][0] != "token-dueno" {
		t.Fatalf("se esperaba un unico despacho a token-dueno, se obtuvo %v", dispatcher.calls)
	}
	stored, ok := notificationLogs.Get(issuerDedupeID(event.EventID))
	if !ok || stored.Status != domain.StatusSkippedNoToken {
		t.Fatalf("se esperaba el aviso al emisor registrado como SKIPPED_NO_TOKEN, se obtuvo %+v (existe=%v)", stored, ok)
	}
	if stored.RecipientID != nil {
		t.Error("el registro del aviso al emisor no deberia guardar al destinatario")
	}
}

func TestDispatchAlertService_SinEmisor_SoloAvisaALosDuenos(t *testing.T) {
	service, deviceTokens, notificationLogs, dispatcher := setup()
	owner := uuid.New()
	addToken(t, deviceTokens, owner, "token-dueno")

	event := AlertCreatedEvent{EventID: uuid.New(), OwnerIDs: []uuid.UUID{owner}, AlertType: "CRASHED"}
	if err := service.Execute(context.Background(), event); err != nil {
		t.Fatalf("Execute() devolvio un error inesperado: %v", err)
	}

	if len(dispatcher.calls) != 1 {
		t.Fatalf("se esperaba un unico despacho, se hicieron %d", len(dispatcher.calls))
	}
	if _, ok := notificationLogs.Get(issuerDedupeID(event.EventID)); ok {
		t.Error("no deberia registrarse un aviso al emisor si el evento no trae emisor")
	}
}

func TestDispatchAlertService_FallaElAvisoAlEmisor_ElReintentoNoReenviaALosDuenos(t *testing.T) {
	service, deviceTokens, notificationLogs, dispatcher := setup()
	dispatcher.failCalls = map[int]bool{2: true} // la llamada 2 es la del emisor
	owner, issuer := uuid.New(), uuid.New()
	addToken(t, deviceTokens, owner, "token-dueno")
	addToken(t, deviceTokens, issuer, "token-emisor")
	event := AlertCreatedEvent{EventID: uuid.New(), OwnerIDs: []uuid.UUID{owner}, AlertType: "CRASHED", AlertID: uuid.New(), IssuerID: issuer}
	ctx := context.Background()

	if err := service.Execute(ctx, event); err == nil {
		t.Fatal("se esperaba que Execute() propagara el fallo del aviso al emisor")
	}
	if _, ok := notificationLogs.Get(issuerDedupeID(event.EventID)); ok {
		t.Fatal("el aviso fallido al emisor no deberia quedar registrado")
	}

	if err := service.Execute(ctx, event); err != nil {
		t.Fatalf("Execute() del reintento devolvio un error inesperado: %v", err)
	}

	// 1: duenos, 2: emisor (falla), 3: emisor (reintento). Los duenos ya
	// estaban confirmados y no se reenvian.
	if dispatcher.callCount() != 3 {
		t.Fatalf("se esperaban 3 despachos, se hicieron %d", dispatcher.callCount())
	}
	if dispatcher.calls[2][0] != "token-emisor" {
		t.Errorf("el reintento deberia ir solo al emisor, fue a %v", dispatcher.calls[2])
	}
}

func TestDispatchAlertService_FallaElAvisoALosDuenos_IgualIntentaAvisarAlEmisor(t *testing.T) {
	service, deviceTokens, _, dispatcher := setup()
	dispatcher.failCalls = map[int]bool{1: true}
	owner, issuer := uuid.New(), uuid.New()
	addToken(t, deviceTokens, owner, "token-dueno")
	addToken(t, deviceTokens, issuer, "token-emisor")
	event := AlertCreatedEvent{EventID: uuid.New(), OwnerIDs: []uuid.UUID{owner}, AlertType: "CRASHED", AlertID: uuid.New(), IssuerID: issuer}

	if err := service.Execute(context.Background(), event); err == nil {
		t.Fatal("se esperaba que Execute() propagara el fallo del aviso a los duenos")
	}
	if dispatcher.callCount() != 2 {
		t.Fatalf("se esperaban 2 intentos (duenos y emisor), se hicieron %d", dispatcher.callCount())
	}
}

func TestDispatchAlertService_RedeliveryConEmisor_NoDuplicaNingunAviso(t *testing.T) {
	service, deviceTokens, _, dispatcher := setup()
	owner, issuer := uuid.New(), uuid.New()
	addToken(t, deviceTokens, owner, "token-dueno")
	addToken(t, deviceTokens, issuer, "token-emisor")
	event := AlertCreatedEvent{EventID: uuid.New(), OwnerIDs: []uuid.UUID{owner}, AlertType: "CRASHED", AlertID: uuid.New(), IssuerID: issuer}

	for i := 0; i < 2; i++ {
		if err := service.Execute(context.Background(), event); err != nil {
			t.Fatalf("Execute() #%d devolvio un error inesperado: %v", i+1, err)
		}
	}

	if dispatcher.callCount() != 2 {
		t.Fatalf("se esperaban 2 despachos en total (uno por destinatario), se hicieron %d", dispatcher.callCount())
	}
}
