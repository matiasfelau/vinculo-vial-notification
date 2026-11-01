package worker

import (
	"context"
	"fmt"
	"log"

	"github.com/google/uuid"

	"github.com/matiasfelau/vinculo-vial-notification/internal/service/sessions"
)

// SessionsRevokedHandler decide, a partir del cuerpo crudo del
// mensaje, que corresponde hacer con el delivery de
// user.sessions.revoked. No conoce el canal AMQP.
type SessionsRevokedHandler struct {
	revokeService *sessions.RevokeSessionsService
	deadLetters   deadLetterPublisher
	queue         string
	maxRetries    int64
}

func NewSessionsRevokedHandler(revokeService *sessions.RevokeSessionsService, deadLetters deadLetterPublisher, queue string, maxRetries int64) *SessionsRevokedHandler {
	return &SessionsRevokedHandler{revokeService: revokeService, deadLetters: deadLetters, queue: queue, maxRetries: maxRetries}
}

var _ messageHandler = (*SessionsRevokedHandler)(nil)

func (h *SessionsRevokedHandler) Handle(ctx context.Context, body []byte, retries int64) Outcome {
	envelope, err := ParseEnvelope(body)
	if err != nil {
		return h.sendToDeadLetter(ctx, body, err)
	}

	ownerID, err := uuid.Parse(envelope.Actor)
	if err != nil {
		return h.sendToDeadLetter(ctx, body, fmt.Errorf("actor invalido: %w", err))
	}

	if _, err := parseSessionsRevokedPayload(envelope.Payload); err != nil {
		return h.sendToDeadLetter(ctx, body, err)
	}

	event := sessions.SessionsRevokedEvent{EventID: envelope.EventID, OwnerID: ownerID}

	if err := h.revokeService.Execute(ctx, event); err != nil {
		if retries >= h.maxRetries {
			log.Printf("se supero MAX_RETRIES (%d) procesando la revocacion del evento %s, se descarta a la dlq: %v", h.maxRetries, envelope.EventID, err)
			return h.sendToDeadLetter(ctx, body, err)
		}

		log.Printf("fallo transitorio procesando la revocacion del evento %s (intento %d de %d): %v", envelope.EventID, retries+1, h.maxRetries, err)
		return OutcomeNackNoRequeue
	}

	return OutcomeAck
}

// sendToDeadLetter publica el mensaje a la DLQ y solo ACKea el
// original si la DLQ confirmo la recepcion. Ver el comentario
// equivalente en AlertCreatedHandler.sendToDeadLetter.
func (h *SessionsRevokedHandler) sendToDeadLetter(ctx context.Context, body []byte, cause error) Outcome {
	log.Printf("descartando evento de revocacion de sesiones a la dlq: %v", cause)
	if err := h.deadLetters.PublishToDLQ(ctx, h.queue, body, cause.Error()); err != nil {
		log.Printf("no se pudo publicar a la dlq, se reintenta: %v", err)
		return OutcomeNackNoRequeue
	}
	return OutcomeAck
}
