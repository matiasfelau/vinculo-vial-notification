package worker

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// EventEnvelope es el sobre estandar que publican los productores de
// eventos de la plataforma. El actor identifica al usuario al que se
// avisa (el emisor en alert.*, el usuario en user.sessions.revoked): se
// usa solo para resolver sus device tokens. Nunca se propaga al
// contenido de una push, ni a un log junto al payload.
//
// El actor puede faltar: las alertas de un emisor dado de baja salen sin
// el (US-11). Ver HasActor.
type EventEnvelope struct {
	EventID   uuid.UUID       `json:"eventId"`
	EventType string          `json:"eventType"`
	Timestamp time.Time       `json:"timestamp"`
	Producer  string          `json:"producer"`
	Actor     string          `json:"actor"`
	Payload   json.RawMessage `json:"payload"`
}

// HasActor indica si el envelope trae actor. Un actor ausente, null o
// vacio es un evento legitimo sin emisor; uno presente que no es un UUID
// es invalido y lo decide cada handler.
func (e EventEnvelope) HasActor() bool {
	return e.Actor != ""
}

// ParseEnvelope decodifica el sobre y valida el minimo indispensable
// para poder procesarlo. Un error aca es permanente: reintentar no
// cambia el resultado.
func ParseEnvelope(body []byte) (EventEnvelope, error) {
	var envelope EventEnvelope
	if err := json.Unmarshal(body, &envelope); err != nil {
		return EventEnvelope{}, fmt.Errorf("envelope invalido: %w", err)
	}
	if envelope.EventID == uuid.Nil {
		return EventEnvelope{}, fmt.Errorf("envelope sin eventId")
	}
	if envelope.EventType == "" {
		return EventEnvelope{}, fmt.Errorf("envelope sin eventType")
	}
	return envelope, nil
}
