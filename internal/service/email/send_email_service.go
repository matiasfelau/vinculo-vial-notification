package email

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/google/uuid"

	"github.com/matiasfelau/vinculo-vial-notification/internal/domain"
)

// SendEmailService envia el email de un evento email.requested.
//
// Idempotencia: el event_id se registra y el email se envia dentro de una
// misma transaccion que solo se confirma si el servidor SMTP lo acepto (o
// lo rechazo de forma permanente). Si el envio falla de forma transitoria,
// la transaccion se revierte y el error vuelve al consumer, que hace NACK:
// el reintento no se descarta como duplicado. Una entrega duplicada
// concurrente espera en la clave unica del event_id y se descarta cuando
// la primera confirma.
//
// Limitacion conocida: si el servidor acepta el mensaje y el commit de la
// transaccion falla, el reintento lo vuelve a enviar (at-least-once).
//
// Datos: los logs de este servicio registran el event_id, la plantilla y
// el resultado; nunca el destinatario ni el codigo.
type SendEmailService struct {
	unitOfWork domain.UnitOfWork
	sender     domain.EmailSender
	now        func() time.Time
}

func NewSendEmailService(unitOfWork domain.UnitOfWork, sender domain.EmailSender) *SendEmailService {
	return &SendEmailService{unitOfWork: unitOfWork, sender: sender, now: time.Now}
}

// Execute envia el email, deduplicando por event_id. Un error nil cubre el
// envio exitoso, el redelivery de un evento ya procesado, el pedido
// vencido (se descarta sin enviar) y el rechazo permanente del servidor:
// el consumer confirma el mensaje en todos esos casos. Un error que
// envuelve ErrInvalidRequest es permanente; cualquier otro es transitorio.
func (s *SendEmailService) Execute(ctx context.Context, eventID uuid.UUID, request Request) error {
	now := s.now()

	if request.ExpiresAt != nil && now.After(*request.ExpiresAt) {
		log.Printf("el email del evento %s (plantilla %s) ya vencio: se descarta sin enviar", eventID, request.Template)
		return nil
	}

	message, err := Render(request, now)
	if err != nil {
		return err
	}

	var rejection error
	err = s.unitOfWork.Do(ctx, func(tx domain.TxRepositories) error {
		// RecipientID queda nulo a proposito: el destinatario es un
		// dato personal y no se persiste junto al evento.
		logEntry := &domain.NotificationLog{
			ID:        uuid.New(),
			EventID:   eventID,
			Status:    domain.StatusSent,
			CreatedAt: now,
		}
		if err := tx.NotificationLogs.Register(ctx, logEntry); err != nil {
			return err
		}

		sendErr := s.sender.Send(ctx, message)
		if errors.Is(sendErr, domain.ErrEmailRejected) {
			// Confirmar la transaccion registra el rechazo: el redelivery
			// del mismo evento se descarta como duplicado.
			rejection = sendErr
			return tx.NotificationLogs.UpdateStatus(ctx, eventID, domain.StatusFailed)
		}
		return sendErr
	})
	if errors.Is(err, domain.ErrEventAlreadyProcessed) {
		return nil
	}
	if err != nil {
		return err
	}

	if rejection != nil {
		log.Printf("el servidor de correo rechazo de forma permanente el email del evento %s (plantilla %s), se descarta: %v", eventID, request.Template, rejection)
		return nil
	}

	log.Printf("email del evento %s (plantilla %s) enviado", eventID, request.Template)
	return nil
}
