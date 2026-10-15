package worker

import (
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
)

// issuerNoticePayload es la porcion del payload de alert.rejected,
// alert.expired y alert.seen que este servicio usa: solo alert_id, para
// el deep link. El resto de los campos (reason_codes, alert_type) se
// ignora a proposito: el emisor nunca debe recibir el motivo de un
// rechazo, y el aviso no depende del tipo de alerta.
type issuerNoticePayload struct {
	AlertID uuid.UUID `json:"alert_id"`
}

func parseIssuerNoticePayload(raw json.RawMessage) (issuerNoticePayload, error) {
	var payload issuerNoticePayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		return issuerNoticePayload{}, fmt.Errorf("payload invalido: %w", err)
	}
	if payload.AlertID == uuid.Nil {
		return issuerNoticePayload{}, fmt.Errorf("payload sin alert_id")
	}
	return payload, nil
}
