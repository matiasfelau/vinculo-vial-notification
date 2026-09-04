package worker

import (
	"context"
	"log"

	"github.com/matiasfelau/vinculo-vial-notification/internal/service/dispatch"
)

// deadLetterPublisher envia un mensaje directamente a la DLQ de la
// plataforma, sorteando el ciclo de reintentos. La implementacion real
// publica sobre el canal AMQP; en las pruebas se reemplaza por un
// doble.
type deadLetterPublisher interface {
	PublishToDLQ(ctx context.Context, body []byte, reason string) error
}

// Outcome indica al consumer AMQP que accion tomar sobre el delivery
// original.
type Outcome int

const (
	// OutcomeAck confirma el mensaje: se proceso con exito, era un
	// redelivery ya conocido, o era invalido y ya se envio a la DLQ.
	OutcomeAck Outcome = iota
	// OutcomeNackNoRequeue rechaza el mensaje sin reencolarlo de forma
	// inmediata. La topologia declarada por vinculo-vial-core lo
	// deriva a su cola de retry.
	OutcomeNackNoRequeue
)

// AlertCreatedHandler decide, a partir del cuerpo crudo del mensaje,
// que corresponde hacer con el delivery. No conoce el canal AMQP: esa
// responsabilidad es del consumer que lo invoca.
type AlertCreatedHandler struct {
	dispatchService *dispatch.DispatchAlertService
	deadLetters     deadLetterPublisher
	maxRetries      int64
}

func NewAlertCreatedHandler(dispatchService *dispatch.DispatchAlertService, deadLetters deadLetterPublisher, maxRetries int64) *AlertCreatedHandler {
	return &AlertCreatedHandler{dispatchService: dispatchService, deadLetters: deadLetters, maxRetries: maxRetries}
}

// Handle procesa un delivery. retries es la cantidad de veces que este
// mensaje ya fue rechazado desde la cola de alertas (leida del header
// x-death por el consumer): el corte del ciclo de reintentos lo aplica
// este handler, no la topologia de RabbitMQ.
func (h *AlertCreatedHandler) Handle(ctx context.Context, body []byte, retries int64) Outcome {
	envelope, err := ParseEnvelope(body)
	if err != nil {
		h.sendToDeadLetter(ctx, body, err)
		return OutcomeAck
	}

	payload, err := parseAlertCreatedPayload(envelope.Payload)
	if err != nil {
		h.sendToDeadLetter(ctx, body, err)
		return OutcomeAck
	}

	event := dispatch.AlertCreatedEvent{
		EventID:   envelope.EventID,
		OwnerIDs:  payload.OwnerIDs,
		AlertType: payload.AlertType,
	}

	if err := h.dispatchService.Execute(ctx, event); err != nil {
		if retries >= h.maxRetries {
			log.Printf("se supero MAX_RETRIES (%d) procesando el evento %s, se descarta a la dlq: %v", h.maxRetries, envelope.EventID, err)
			h.sendToDeadLetter(ctx, body, err)
			return OutcomeAck
		}

		log.Printf("fallo transitorio procesando el evento %s (intento %d de %d): %v", envelope.EventID, retries+1, h.maxRetries, err)
		return OutcomeNackNoRequeue
	}

	return OutcomeAck
}

func (h *AlertCreatedHandler) sendToDeadLetter(ctx context.Context, body []byte, cause error) {
	log.Printf("envelope invalido, se descarta a la dlq: %v", cause)
	if err := h.deadLetters.PublishToDLQ(ctx, body, cause.Error()); err != nil {
		log.Printf("no se pudo publicar a la dlq: %v", err)
	}
}
