package dispatch

import (
	"context"

	"github.com/google/uuid"

	"github.com/matiasfelau/vinculo-vial-notification/internal/domain"
)

// IssuerNoticeEvent es la porcion del Event Envelope de alert.rejected,
// alert.expired y alert.seen que este servicio necesita. IssuerID es el
// actor del envelope: el destinatario de la push es siempre el.
type IssuerNoticeEvent struct {
	EventID  uuid.UUID
	IssuerID uuid.UUID
	AlertID  uuid.UUID
	Notice   IssuerNotice
}

// NotifyIssuerService avisa al emisor de una alerta de un cambio de
// estado. El contenido es generico y depende solo del tipo de aviso:
// ningun otro dato del evento (motivos, veredictos, tipo de alerta)
// llega hasta la push.
type NotifyIssuerService struct {
	deliverer
}

func NewNotifyIssuerService(
	unitOfWork domain.UnitOfWork,
	deviceTokens domain.DeviceTokenRepository,
	dispatcher domain.Dispatcher,
) *NotifyIssuerService {
	return &NotifyIssuerService{deliverer{
		unitOfWork:   unitOfWork,
		deviceTokens: deviceTokens,
		dispatcher:   dispatcher,
	}}
}

// Execute envia el aviso al emisor, deduplicando por event_id. Si el
// emisor no tiene tokens no es un error: se registra y se confirma.
func (s *NotifyIssuerService) Execute(ctx context.Context, event IssuerNoticeEvent) error {
	return s.deliver(ctx, event.EventID, []uuid.UUID{event.IssuerID}, issuerMessage(event.Notice, event.AlertID))
}
