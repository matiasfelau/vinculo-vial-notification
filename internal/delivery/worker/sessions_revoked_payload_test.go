package worker

import "testing"

func TestParseSessionsRevokedPayload_AceptaUnPayloadValido(t *testing.T) {
	payload, err := parseSessionsRevokedPayload([]byte(`{"reason": "secuestro de sesion detectado"}`))
	if err != nil {
		t.Fatalf("parseSessionsRevokedPayload() devolvio un error inesperado: %v", err)
	}
	if payload.Reason != "secuestro de sesion detectado" {
		t.Errorf("Reason = %q, no coincide", payload.Reason)
	}
}

func TestParseSessionsRevokedPayload_AceptaPayloadSinReason(t *testing.T) {
	if _, err := parseSessionsRevokedPayload([]byte(`{}`)); err != nil {
		t.Fatalf("parseSessionsRevokedPayload() devolvio un error inesperado: %v", err)
	}
}

func TestParseSessionsRevokedPayload_RechazaJSONMalFormado(t *testing.T) {
	if _, err := parseSessionsRevokedPayload([]byte(`{invalido`)); err == nil {
		t.Fatal("se esperaba un error ante JSON mal formado")
	}
}
