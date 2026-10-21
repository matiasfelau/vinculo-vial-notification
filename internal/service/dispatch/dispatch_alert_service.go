package dispatch

import (
	"context"
	"errors"

	"github.com/google/uuid"

	"github.com/matiasfelau/vinculo-vial-notification/internal/domain"
)

// AlertCreatedEvent es la porcion del Event Envelope de alert.created
// que este servicio necesita, ya extraida y validada por el consumer.
//
// OwnerIDs puede estar vacio si IssuerID no lo esta. AlertID e IssuerID
// son opcionales. AlertID viaja en el payload desde
// US-09; las alertas anteriores no lo traen (uuid.Nil). IssuerID es el
// actor del envelope: su unico uso permitido es resolver los tokens del
// aviso al emisor. Nunca se incluye en el contenido de una push ni en
// un log.
type AlertCreatedEvent struct {
	EventID   uuid.UUID
	OwnerIDs  []uuid.UUID
	AlertType string
	AlertID   uuid.UUID
	IssuerID  uuid.UUID
}

// DispatchAlertService procesa un evento alert.created: avisa a los
// duenos (push generica, si hay) y, ademas, al emisor de la alerta.
//
// Son dos envios independientes, cada uno con su propia clave de
// idempotencia: el event_id para los duenos y una derivada de el para el
// emisor. Asi, si uno falla y el mensaje se reintenta, el que ya
// confirmo se descarta como duplicado en lugar de enviarse de nuevo.
type DispatchAlertService struct {
	deliverer
}

// NewDispatchAlertService construye el servicio. deviceTokens se usa
// fuera de la transaccion, para la limpieza de tokens invalidos.
func NewDispatchAlertService(
	unitOfWork domain.UnitOfWork,
	deviceTokens domain.DeviceTokenRepository,
	dispatcher domain.Dispatcher,
) *DispatchAlertService {
	return &DispatchAlertService{deliverer{
		unitOfWork:   unitOfWork,
		deviceTokens: deviceTokens,
		dispatcher:   dispatcher,
	}}
}

// issuerDedupeID deriva, de forma determinista, la clave de
// idempotencia del aviso al emisor a partir del event_id: distinta de la
// de los duenos (que usa el event_id tal cual) y estable entre
// reintentos.
func issuerDedupeID(eventID uuid.UUID) uuid.UUID {
	return uuid.NewSHA1(eventID, []byte("issuer"))
}

// Execute aplica el flujo documentado para alert.created. Ambos envios
// se intentan aunque uno falle; si alguno falla, el error vuelve al
// consumer para que reintente el mensaje completo.
func (s *DispatchAlertService) Execute(ctx context.Context, event AlertCreatedEvent) error {
	// Una alerta puede validarse antes de tener destinatarios (la patente
	// todavia no tiene duenos): en ese caso solo se avisa al emisor.
	var ownersErr error
	if len(event.OwnerIDs) > 0 {
		ownersErr = s.deliver(ctx, event.EventID, event.OwnerIDs, ownerMessage(event.AlertType, event.AlertID))
	}

	var issuerErr error
	if event.IssuerID != uuid.Nil {
		issuerErr = s.deliver(ctx, issuerDedupeID(event.EventID), []uuid.UUID{event.IssuerID}, issuerMessage(NoticeValidated, event.AlertID))
	}

	return errors.Join(ownersErr, issuerErr)
}
