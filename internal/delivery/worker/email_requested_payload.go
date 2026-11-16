package worker

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// emailRequestedPayload es el payload de email.requested. to es el
// destinatario y code un codigo de un solo uso: ambos son datos
// sensibles que nunca se loguean ni llegan a la DLQ.
type emailRequestedPayload struct {
	Template  string     `json:"template"`
	To        string     `json:"to"`
	Code      *string    `json:"code"`
	ExpiresAt *time.Time `json:"expires_at"`
}

// parseEmailRequestedPayload decodifica el payload. Los errores que
// devuelve describen el problema sin reproducir valores del payload: el
// mensaje termina en un log y en un header de la DLQ.
func parseEmailRequestedPayload(raw json.RawMessage) (emailRequestedPayload, error) {
	var payload emailRequestedPayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		return emailRequestedPayload{}, fmt.Errorf("payload invalido: %s", describeDecodeError(err))
	}
	if payload.Template == "" {
		return emailRequestedPayload{}, errors.New("payload sin template")
	}
	if payload.To == "" {
		return emailRequestedPayload{}, errors.New("payload sin destinatario")
	}
	return payload, nil
}

func describeDecodeError(err error) string {
	var syntaxErr *json.SyntaxError
	if errors.As(err, &syntaxErr) {
		return "json mal formado"
	}
	var typeErr *json.UnmarshalTypeError
	if errors.As(err, &typeErr) {
		return fmt.Sprintf("el campo %q tiene un tipo invalido", typeErr.Field)
	}
	return "un campo tiene un formato invalido"
}

// emptyRedactedBody es lo que se publica en la DLQ cuando el cuerpo no se
// pudo parsear lo bastante como para redactarlo: antes que arriesgar un
// email o un codigo en claro, se descarta el contenido.
var emptyRedactedBody = []byte("{}")

// redactEmailRequest devuelve el cuerpo con to y code en null, para
// publicar un email.requested en la DLQ sin datos personales ni codigos.
// El resto del envelope (eventId, actor, timestamp, template, expires_at)
// se conserva para poder diagnosticar. Si el cuerpo no es un envelope con
// un payload objeto, devuelve un cuerpo vacio: no hay forma de saber que
// parte tiene datos sensibles.
func redactEmailRequest(body []byte) []byte {
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(body, &envelope); err != nil || envelope == nil {
		return emptyRedactedBody
	}

	var payload map[string]json.RawMessage
	if err := json.Unmarshal(envelope["payload"], &payload); err != nil || payload == nil {
		return emptyRedactedBody
	}

	payload["to"] = json.RawMessage("null")
	payload["code"] = json.RawMessage("null")

	redactedPayload, err := json.Marshal(payload)
	if err != nil {
		return emptyRedactedBody
	}
	envelope["payload"] = redactedPayload

	redacted, err := json.Marshal(envelope)
	if err != nil {
		return emptyRedactedBody
	}
	return redacted
}
