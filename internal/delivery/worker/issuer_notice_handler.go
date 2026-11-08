package worker

import (
	"context"
	"fmt"
	"log"

	"github.com/google/uuid"

	"github.com/matiasfelau/vinculo-vial-notification/internal/service/dispatch"
)

// IssuerNoticeHandler decide, a partir del cuerpo crudo del mensaje,
// que corresponde hacer con un delivery de alert.rejected,
// alert.expired o alert.seen: avisar al emisor, que viaja en el actor
// del envelope. No conoce el canal AMQP.
//
// Los tres eventos comparten forma y tratamiento; solo cambia el aviso.
// Por eso hay un unico tipo con un constructor por consumidor.
//
// Sin actor (el emisor se dio de baja, US-11) no hay a quien avisar: el
// mensaje se confirma sin push y nunca va a la DLQ. Un actor presente que
// no es un UUID si es invalido.
//
// Anonimato: los logs de este handler registran el eventId y la causa,
// nunca el actor ni el contenido del payload.
type IssuerNoticeHandler struct {
	notifyService *dispatch.NotifyIssuerService
	notice        dispatch.IssuerNotice
	deadLetters   deadLetterPublisher
	queue         string
	maxRetries    int64
}

var _ messageHandler = (*IssuerNoticeHandler)(nil)

func newIssuerNoticeHandler(notice dispatch.IssuerNotice, notifyService *dispatch.NotifyIssuerService, deadLetters deadLetterPublisher, queue string, maxRetries int64) *IssuerNoticeHandler {
	return &IssuerNoticeHandler{notifyService: notifyService, notice: notice, deadLetters: deadLetters, queue: queue, maxRetries: maxRetries}
}

// NewAlertRejectedHandler consume alert.rejected (RED de IA).
func NewAlertRejectedHandler(notifyService *dispatch.NotifyIssuerService, deadLetters deadLetterPublisher, queue string, maxRetries int64) *IssuerNoticeHandler {
	return newIssuerNoticeHandler(dispatch.NoticeRejected, notifyService, deadLetters, queue, maxRetries)
}

// NewAlertExpiredHandler consume alert.expired (sin veredicto a tiempo).
func NewAlertExpiredHandler(notifyService *dispatch.NotifyIssuerService, deadLetters deadLetterPublisher, queue string, maxRetries int64) *IssuerNoticeHandler {
	return newIssuerNoticeHandler(dispatch.NoticeExpired, notifyService, deadLetters, queue, maxRetries)
}

// NewAlertSeenHandler consume alert.seen (el primer dueno abrio el chat).
func NewAlertSeenHandler(notifyService *dispatch.NotifyIssuerService, deadLetters deadLetterPublisher, queue string, maxRetries int64) *IssuerNoticeHandler {
	return newIssuerNoticeHandler(dispatch.NoticeSeen, notifyService, deadLetters, queue, maxRetries)
}

// Handle procesa un delivery. retries es la cantidad de veces que este
// mensaje ya fue rechazado desde la cola de trabajo (leida del header
// x-death por el consumer): el corte del ciclo de reintentos lo aplica
// este handler.
func (h *IssuerNoticeHandler) Handle(ctx context.Context, body []byte, retries int64) Outcome {
	envelope, err := ParseEnvelope(body)
	if err != nil {
		return h.sendToDeadLetter(ctx, body, err)
	}

	var issuerID uuid.UUID
	if envelope.HasActor() {
		issuerID, err = uuid.Parse(envelope.Actor)
		if err != nil {
			return h.sendToDeadLetter(ctx, body, fmt.Errorf("actor invalido: %w", err))
		}
	}

	payload, err := parseIssuerNoticePayload(envelope.Payload)
	if err != nil {
		return h.sendToDeadLetter(ctx, body, err)
	}

	if !envelope.HasActor() {
		log.Printf("el evento %s de %s no trae actor (emisor dado de baja): no hay a quien avisar, se confirma sin push", envelope.EventID, h.queue)
		return OutcomeAck
	}

	event := dispatch.IssuerNoticeEvent{
		EventID:  envelope.EventID,
		IssuerID: issuerID,
		AlertID:  payload.AlertID,
		Notice:   h.notice,
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
func (h *IssuerNoticeHandler) sendToDeadLetter(ctx context.Context, body []byte, cause error) Outcome {
	log.Printf("descartando mensaje de %s a la dlq: %v", h.queue, cause)
	if err := h.deadLetters.PublishToDLQ(ctx, h.queue, body, cause.Error()); err != nil {
		log.Printf("no se pudo publicar a la dlq, se reintenta: %v", err)
		return OutcomeNackNoRequeue
	}
	return OutcomeAck
}
