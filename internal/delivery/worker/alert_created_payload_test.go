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

func TestParseAlertCreatedPayload_AceptaOwnerIDsVacio(t *testing.T) {
	payload, err := parseAlertCreatedPayload([]byte(`{"owner_ids": [], "alert_type": "choque"}`))
	if err != nil {
		t.Fatalf("parseAlertCreatedPayload() devolvio un error inesperado: %v", err)
	}
	if len(payload.OwnerIDs) != 0 {
		t.Errorf("se esperaban 0 owner_ids, se obtuvieron %d", len(payload.OwnerIDs))
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

func TestParseAlertCreatedPayload_AceptaAlertID(t *testing.T) {
	payload, err := parseAlertCreatedPayload([]byte(`{"owner_ids": ["33333333-3333-3333-3333-333333333333"], "alert_type": "choque", "alert_id": "55555555-5555-5555-5555-555555555555"}`))
	if err != nil {
		t.Fatalf("parseAlertCreatedPayload() devolvio un error inesperado: %v", err)
	}
	if payload.AlertID.String() != "55555555-5555-5555-5555-555555555555" {
		t.Errorf("AlertID = %s, no coincide con el del payload", payload.AlertID)
	}
}

func TestParseAlertCreatedPayload_ToleraPayloadSinAlertID(t *testing.T) {
	payload, err := parseAlertCreatedPayload([]byte(`{"owner_ids": ["33333333-3333-3333-3333-333333333333"], "alert_type": "choque"}`))
	if err != nil {
		t.Fatalf("parseAlertCreatedPayload() devolvio un error inesperado: %v", err)
	}
	if payload.AlertID.String() != "00000000-0000-0000-0000-000000000000" {
		t.Errorf("AlertID = %s, se esperaba el UUID nulo cuando el payload no lo trae", payload.AlertID)
	}
}

func TestParseAlertCreatedPayload_RechazaAlertIDMalFormado(t *testing.T) {
	if _, err := parseAlertCreatedPayload([]byte(`{"owner_ids": ["33333333-3333-3333-3333-333333333333"], "alert_type": "choque", "alert_id": "no-es-uuid"}`)); err == nil {
		t.Fatal("se esperaba un error ante un alert_id que no es UUID")
	}
}
