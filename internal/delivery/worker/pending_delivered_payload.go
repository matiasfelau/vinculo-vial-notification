package worker

import (
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
)

// pendingDeliveredPayload es la porcion del payload de
// alert.pending.delivered que este servicio usa: solo owner_ids. count
// se ignora a proposito: el aviso es generico.
type pendingDeliveredPayload struct {
	OwnerIDs []uuid.UUID `json:"owner_ids"`
}

func parsePendingDeliveredPayload(raw json.RawMessage) (pendingDeliveredPayload, error) {
	var payload pendingDeliveredPayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		return pendingDeliveredPayload{}, fmt.Errorf("payload invalido: %w", err)
	}
	if len(payload.OwnerIDs) == 0 {
		return pendingDeliveredPayload{}, fmt.Errorf("payload sin owner_ids")
	}
	return payload, nil
}
