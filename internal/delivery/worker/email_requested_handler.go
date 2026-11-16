package worker

import (
	"context"
	"errors"
	"log"

	emailservice "github.com/matiasfelau/vinculo-vial-notification/internal/service/email"
)

// EmailRequestedHandler decide, a partir del cuerpo crudo del mensaje, que
// corresponde hacer con un delivery de email.requested: enviar el email de
// la plantilla pedida. No conoce el canal AMQP.
//
// El actor del envelope no se usa: el destinatario es el campo to del
// payload (en EMAIL_VERIFICATION y ACCOUNT_EXISTS el actor es nulo porque
// todavia no hay cuenta, y eso es normal).
//
// Datos personales: este handler nunca loguea to ni code, y al publicar en
// la DLQ los reemplaza por null (redactEmailRequest).
type EmailRequestedHandler struct {
	service     *emailservice.SendEmailService
	deadLetters deadLetterPublisher
	queue       string
	maxRetries  int64
}

var _ messageHandler = (*EmailRequestedHandler)(nil)

func NewEmailRequestedHandler(service *emailservice.SendEmailService, deadLetters deadLetterPublisher, queue string, maxRetries int64) *EmailRequestedHandler {
	return &EmailRequestedHandler{service: service, deadLetters: deadLetters, queue: queue, maxRetries: maxRetries}
}

// Handle procesa un delivery. retries es la cantidad de veces que este
// mensaje ya fue rechazado desde la cola de trabajo (leida del header
// x-death por el consumer): el corte del ciclo de reintentos lo aplica
// este handler.
func (h *EmailRequestedHandler) Handle(ctx context.Context, body []byte, retries int64) Outcome {
	envelope, err := ParseEnvelope(body)
	if err != nil {
		return h.sendToDeadLetter(ctx, body, err)
	}

	payload, err := parseEmailRequestedPayload(envelope.Payload)
	if err != nil {
		return h.sendToDeadLetter(ctx, body, err)
	}

	request := emailservice.Request{
		Template:   emailservice.Template(payload.Template),
		To:         payload.To,
		Code:       payload.Code,
		ExpiresAt:  payload.ExpiresAt,
		OccurredAt: envelope.Timestamp,
	}

	err = h.service.Execute(ctx, envelope.EventID, request)
	if err == nil {
		return OutcomeAck
	}

	// Un pedido invalido es permanente: reintentar no lo arregla.
	if errors.Is(err, emailservice.ErrInvalidRequest) {
		return h.sendToDeadLetter(ctx, body, err)
	}

	if retries >= h.maxRetries {
		log.Printf("se supero MAX_RETRIES (%d) procesando el evento %s de %s, se descarta a la dlq: %v", h.maxRetries, envelope.EventID, h.queue, err)
		return h.sendToDeadLetter(ctx, body, err)
	}

	log.Printf("fallo transitorio procesando el evento %s de %s (intento %d de %d): %v", envelope.EventID, h.queue, retries+1, h.maxRetries, err)
	return OutcomeNackNoRequeue
}

// sendToDeadLetter publica el mensaje redactado a la DLQ y solo ACKea el
// original si la DLQ confirmo la recepcion. Ver el comentario equivalente
// en AlertCreatedHandler.sendToDeadLetter.
func (h *EmailRequestedHandler) sendToDeadLetter(ctx context.Context, body []byte, cause error) Outcome {
	log.Printf("descartando mensaje de %s a la dlq: %v", h.queue, cause)
	if err := h.deadLetters.PublishToDLQ(ctx, h.queue, redactEmailRequest(body), cause.Error()); err != nil {
		log.Printf("no se pudo publicar a la dlq, se reintenta: %v", err)
		return OutcomeNackNoRequeue
	}
	return OutcomeAck
}
