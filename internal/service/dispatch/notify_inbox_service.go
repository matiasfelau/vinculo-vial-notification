package dispatch

import (
	"context"

	"github.com/google/uuid"

	"github.com/matiasfelau/vinculo-vial-notification/internal/domain"
)

// PendingDeliveredEvent es la porcion del Event Envelope de
// alert.pending.delivered que este servicio necesita. El evento no trae
// actor: los destinatarios son solo los duenos de OwnerIDs.
type PendingDeliveredEvent struct {
	EventID  uuid.UUID
	OwnerIDs []uuid.UUID
}

// NotifyInboxService avisa a los duenos que recibieron alertas
// pendientes al vincular su vehiculo. Es una unica push por dueno por
// evento, con contenido generico: no lleva cantidad ni datos de las
// alertas.
type NotifyInboxService struct {
	deliverer
}

func NewNotifyInboxService(
	unitOfWork domain.UnitOfWork,
	deviceTokens domain.DeviceTokenRepository,
	dispatcher domain.Dispatcher,
) *NotifyInboxService {
	return &NotifyInboxService{deliverer{
		unitOfWork:   unitOfWork,
		deviceTokens: deviceTokens,
		dispatcher:   dispatcher,
	}}
}

// Execute envia el aviso a los duenos, deduplicando por event_id. Si
// ninguno tiene tokens no es un error: se registra y se confirma.
func (s *NotifyInboxService) Execute(ctx context.Context, event PendingDeliveredEvent) error {
	return s.deliver(ctx, event.EventID, event.OwnerIDs, inboxMessage())
}
