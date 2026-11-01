package worker

import (
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
)

// alertCreatedPayload es el contenido de payload para el evento
// alert.created. El actor del envelope queda deliberadamente fuera de
// esta estructura. OwnerIDs puede venir vacio (alerta sin destinatarios
// todavia): si el evento es valido depende tambien del actor, y eso lo
// resuelve el handler. AlertID es opcional: las alertas anteriores a US-09
// no lo traen y se procesan sin deep link (uuid.Nil).
type alertCreatedPayload struct {
	OwnerIDs  []uuid.UUID `json:"owner_ids"`
	AlertType string      `json:"alert_type"`
	AlertID   uuid.UUID   `json:"alert_id"`
}

func parseAlertCreatedPayload(raw json.RawMessage) (alertCreatedPayload, error) {
	var payload alertCreatedPayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		return alertCreatedPayload{}, fmt.Errorf("payload invalido: %w", err)
	}
	if payload.AlertType == "" {
		return alertCreatedPayload{}, fmt.Errorf("payload sin alert_type")
	}
	return payload, nil
}
