package worker

import (
	"context"
	"log"

	"github.com/google/uuid"

	"github.com/matiasfelau/vinculo-vial-notification/internal/service/dispatch"
)

// deadLetterPublisher envia un mensaje directamente a la DLQ de la
// plataforma, sorteando el ciclo de reintentos. La implementacion real
// publica sobre el canal AMQP; en las pruebas se reemplaza por un
// doble.
type deadLetterPublisher interface {
	PublishToDLQ(ctx context.Context, queue string, body []byte, reason string) error
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
	queue           string
	maxRetries      int64
}

func NewAlertCreatedHandler(dispatchService *dispatch.DispatchAlertService, deadLetters deadLetterPublisher, queue string, maxRetries int64) *AlertCreatedHandler {
	return &AlertCreatedHandler{dispatchService: dispatchService, deadLetters: deadLetters, queue: queue, maxRetries: maxRetries}
}

// Handle procesa un delivery. retries es la cantidad de veces que este
// mensaje ya fue rechazado desde la cola de alertas (leida del header
// x-death por el consumer): el corte del ciclo de reintentos lo aplica
// este handler, no la topologia de RabbitMQ.
func (h *AlertCreatedHandler) Handle(ctx context.Context, body []byte, retries int64) Outcome {
	envelope, err := ParseEnvelope(body)
	if err != nil {
		return h.sendToDeadLetter(ctx, body, err)
	}

	payload, err := parseAlertCreatedPayload(envelope.Payload)
	if err != nil {
		return h.sendToDeadLetter(ctx, body, err)
	}

	event := dispatch.AlertCreatedEvent{
		EventID:   envelope.EventID,
		OwnerIDs:  payload.OwnerIDs,
		AlertType: payload.AlertType,
		AlertID:   payload.AlertID,
		IssuerID:  issuerFromActor(envelope),
	}

	if err := h.dispatchService.Execute(ctx, event); err != nil {
		if retries >= h.maxRetries {
			log.Printf("se supero MAX_RETRIES (%d) procesando el evento %s, se descarta a la dlq: %v", h.maxRetries, envelope.EventID, err)
			return h.sendToDeadLetter(ctx, body, err)
		}

		log.Printf("fallo transitorio procesando el evento %s (intento %d de %d): %v", envelope.EventID, retries+1, h.maxRetries, err)
		return OutcomeNackNoRequeue
	}

	return OutcomeAck
}

// issuerFromActor devuelve el emisor de la alerta (el actor del
// envelope), o uuid.Nil si el actor no es un UUID valido. Un actor
// invalido no invalida el evento: el aviso a los duenos no depende de el,
// asi que solo se omite el aviso al emisor. El log lleva el eventId y no
// el valor del actor.
func issuerFromActor(envelope EventEnvelope) uuid.UUID {
	issuerID, err := uuid.Parse(envelope.Actor)
	if err != nil {
		log.Printf("el evento %s no trae un actor valido, se omite el aviso al emisor", envelope.EventID)
		return uuid.Nil
	}
	return issuerID
}

// sendToDeadLetter publica el mensaje a la DLQ y solo ACKea el
// original si la DLQ confirmo la recepcion. Si la publicacion falla
// (broker caido, DLQ sin confirmar), el mensaje se rechaza sin
// requeue para que el ciclo normal de reintentos le de otra
// oportunidad a la DLQ de estar disponible, en lugar de perderlo en
// silencio con un ACK que no garantiza nada.
func (h *AlertCreatedHandler) sendToDeadLetter(ctx context.Context, body []byte, cause error) Outcome {
	log.Printf("descartando mensaje a la dlq: %v", cause)
	if err := h.deadLetters.PublishToDLQ(ctx, h.queue, body, cause.Error()); err != nil {
		log.Printf("no se pudo publicar a la dlq, se reintenta: %v", err)
		return OutcomeNackNoRequeue
	}
	return OutcomeAck
}
