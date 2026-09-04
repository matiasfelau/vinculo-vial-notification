package worker

import "testing"

func TestParseAlertCreatedPayload_AceptaUnPayloadValido(t *testing.T) {
	payload, err := parseAlertCreatedPayload([]byte(`{"owner_ids": ["33333333-3333-3333-3333-333333333333"], "alert_type": "choque"}`))
	if err != nil {
		t.Fatalf("parseAlertCreatedPayload() devolvio un error inesperado: %v", err)
	}
	if len(payload.OwnerIDs) != 1 {
		t.Errorf("se esperaba un owner_id, se obtuvieron %d", len(payload.OwnerIDs))
	}
	if payload.AlertType != "choque" {
		t.Errorf("AlertType = %q, se esperaba %q", payload.AlertType, "choque")
	}
}

func TestParseAlertCreatedPayload_RechazaPayloadSinOwnerIDs(t *testing.T) {
	if _, err := parseAlertCreatedPayload([]byte(`{"owner_ids": [], "alert_type": "choque"}`)); err == nil {
		t.Fatal("se esperaba un error ante owner_ids vacio")
	}
}

func TestParseAlertCreatedPayload_RechazaPayloadSinAlertType(t *testing.T) {
	if _, err := parseAlertCreatedPayload([]byte(`{"owner_ids": ["33333333-3333-3333-3333-333333333333"]}`)); err == nil {
		t.Fatal("se esperaba un error ante alert_type ausente")
	}
}

func TestParseAlertCreatedPayload_RechazaJSONMalFormado(t *testing.T) {
	if _, err := parseAlertCreatedPayload([]byte(`{invalido`)); err == nil {
		t.Fatal("se esperaba un error ante JSON mal formado")
	}
}
