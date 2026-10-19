package worker

import (
	"context"
	"log"

	"github.com/matiasfelau/vinculo-vial-notification/internal/service/dispatch"
)

// PendingDeliveredHandler decide, a partir del cuerpo crudo del mensaje,
// que corresponde hacer con un delivery de alert.pending.delivered:
// avisar a cada dueno de owner_ids que tiene alertas sin leer. No conoce
// el canal AMQP.
//
// El evento no trae actor y el handler lo ignora aunque viniera: este
// aviso nunca llega al emisor de ninguna alerta.
type PendingDeliveredHandler struct {
	notifyService *dispatch.NotifyInboxService
	deadLetters   deadLetterPublisher
	queue         string
	maxRetries    int64
}

var _ messageHandler = (*PendingDeliveredHandler)(nil)

func NewPendingDeliveredHandler(notifyService *dispatch.NotifyInboxService, deadLetters deadLetterPublisher, queue string, maxRetries int64) *PendingDeliveredHandler {
	return &PendingDeliveredHandler{notifyService: notifyService, deadLetters: deadLetters, queue: queue, maxRetries: maxRetries}
}

// Handle procesa un delivery. retries es la cantidad de veces que este
// mensaje ya fue rechazado desde la cola de trabajo (leida del header
// x-death por el consumer): el corte del ciclo de reintentos lo aplica
// este handler.
func (h *PendingDeliveredHandler) Handle(ctx context.Context, body []byte, retries int64) Outcome {
	envelope, err := ParseEnvelope(body)
	if err != nil {
		return h.sendToDeadLetter(ctx, body, err)
	}

	payload, err := parsePendingDeliveredPayload(envelope.Payload)
	if err != nil {
		return h.sendToDeadLetter(ctx, body, err)
	}

	event := dispatch.PendingDeliveredEvent{
		EventID:  envelope.EventID,
		OwnerIDs: payload.OwnerIDs,
	}

	if err := h.notifyService.Execute(ctx, event); err != nil {
		if retries >= h.maxRetries {
			log.Printf("se supero MAX_RETRIES (%d) procesando el evento %s de %s, se descarta a la dlq: %v", h.maxRetries, envelope.EventID, h.queue, err)
			return h.sendToDeadLetter(ctx, body, err)
		}

		log.Printf("fallo transitorio procesando el evento %s de %s (intento %d de %d): %v", envelope.EventID, h.queue, retries+1, h.maxRetries, err)
		return OutcomeNackNoRequeue
	}

	return OutcomeAck
}

// sendToDeadLetter publica el mensaje a la DLQ y solo ACKea el
// original si la DLQ confirmo la recepcion. Ver el comentario
// equivalente en AlertCreatedHandler.sendToDeadLetter.
func (h *PendingDeliveredHandler) sendToDeadLetter(ctx context.Context, body []byte, cause error) Outcome {
	log.Printf("descartando mensaje de %s a la dlq: %v", h.queue, cause)
	if err := h.deadLetters.PublishToDLQ(ctx, h.queue, body, cause.Error()); err != nil {
		log.Printf("no se pudo publicar a la dlq, se reintenta: %v", err)
		return OutcomeNackNoRequeue
	}
	return OutcomeAck
}
