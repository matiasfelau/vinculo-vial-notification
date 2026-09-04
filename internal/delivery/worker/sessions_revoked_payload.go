package worker

import (
	"encoding/json"
	"fmt"
)

// sessionsRevokedPayload es el contenido de payload para el evento
// user.sessions.revoked. reason es exclusivamente informativo: el
// owner cuyos tokens se borran viene del actor del envelope, no de
// aca.
type sessionsRevokedPayload struct {
	Reason string `json:"reason"`
}

func parseSessionsRevokedPayload(raw json.RawMessage) (sessionsRevokedPayload, error) {
	var payload sessionsRevokedPayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		return sessionsRevokedPayload{}, fmt.Errorf("payload invalido: %w", err)
	}
	return payload, nil
}
