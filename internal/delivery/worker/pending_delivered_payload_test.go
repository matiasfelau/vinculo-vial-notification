package worker

import "testing"

func TestParsePendingDeliveredPayload_AceptaUnPayloadValidoIgnorandoCount(t *testing.T) {
	payload, err := parsePendingDeliveredPayload([]byte(`{"owner_ids": ["33333333-3333-3333-3333-333333333333"], "count": 4}`))
	if err != nil {
		t.Fatalf("parsePendingDeliveredPayload() devolvio un error inesperado: %v", err)
	}
	if len(payload.OwnerIDs) != 1 {
		t.Errorf("se esperaba un owner_id, se obtuvieron %d", len(payload.OwnerIDs))
	}
}

func TestParsePendingDeliveredPayload_RechazaPayloadInvalido(t *testing.T) {
	cases := map[string]string{
		"sin owner_ids":        `{"count": 1}`,
		"owner_ids vacio":      `{"owner_ids": [], "count": 1}`,
		"owner_id sin formato": `{"owner_ids": ["no-es-uuid"], "count": 1}`,
		"json mal formado":     `{invalido`,
	}
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := parsePendingDeliveredPayload([]byte(raw)); err == nil {
				t.Fatal("se esperaba un error")
			}
		})
	}
}
