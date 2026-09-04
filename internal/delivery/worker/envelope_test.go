package worker

import (
	"testing"
)

func TestParseEnvelope_AceptaUnSobreValido(t *testing.T) {
	body := []byte(`{
		"eventId": "11111111-1111-1111-1111-111111111111",
		"eventType": "alert.created",
		"timestamp": "2026-01-01T00:00:00Z",
		"producer": "vinculo-vial-alert",
		"actor": "22222222-2222-2222-2222-222222222222",
		"payload": {"owner_ids": ["33333333-3333-3333-3333-333333333333"], "alert_type": "choque"}
	}`)

	envelope, err := ParseEnvelope(body)
	if err != nil {
		t.Fatalf("ParseEnvelope() devolvio un error inesperado: %v", err)
	}
	if envelope.EventType != "alert.created" {
		t.Errorf("EventType = %q, se esperaba %q", envelope.EventType, "alert.created")
	}
}

func TestParseEnvelope_RechazaJSONMalFormado(t *testing.T) {
	if _, err := ParseEnvelope([]byte(`{invalido`)); err == nil {
		t.Fatal("se esperaba un error ante JSON mal formado")
	}
}

func TestParseEnvelope_RechazaSobreSinEventId(t *testing.T) {
	body := []byte(`{"eventType": "alert.created", "payload": {}}`)
	if _, err := ParseEnvelope(body); err == nil {
		t.Fatal("se esperaba un error ante un sobre sin eventId")
	}
}

func TestParseEnvelope_RechazaSobreSinEventType(t *testing.T) {
	body := []byte(`{"eventId": "11111111-1111-1111-1111-111111111111", "payload": {}}`)
	if _, err := ParseEnvelope(body); err == nil {
		t.Fatal("se esperaba un error ante un sobre sin eventType")
	}
}
