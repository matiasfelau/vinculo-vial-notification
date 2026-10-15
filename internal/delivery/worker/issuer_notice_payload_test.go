package worker

import "testing"

func TestParseIssuerNoticePayload_AceptaUnPayloadValido(t *testing.T) {
	payload, err := parseIssuerNoticePayload([]byte(`{"alert_id": "55555555-5555-5555-5555-555555555555", "alert_type": "CRASHED"}`))
	if err != nil {
		t.Fatalf("parseIssuerNoticePayload() devolvio un error inesperado: %v", err)
	}
	if payload.AlertID.String() != "55555555-5555-5555-5555-555555555555" {
		t.Errorf("AlertID = %s, no coincide con el del payload", payload.AlertID)
	}
}

func TestParseIssuerNoticePayload_IgnoraLosCamposQueNoUsa(t *testing.T) {
	// alert.rejected trae reason_codes: este servicio no debe leerlo.
	if _, err := parseIssuerNoticePayload([]byte(`{"alert_id": "55555555-5555-5555-5555-555555555555", "reason_codes": ["PLATE_MISMATCH"]}`)); err != nil {
		t.Fatalf("parseIssuerNoticePayload() devolvio un error inesperado: %v", err)
	}
}

func TestParseIssuerNoticePayload_Rechaza(t *testing.T) {
	cases := map[string]string{
		"sin alert_id":        `{"alert_type": "CRASHED"}`,
		"alert_id vacio":      `{"alert_id": ""}`,
		"alert_id no es uuid": `{"alert_id": "xyz"}`,
		"json mal formado":    `{invalido`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := parseIssuerNoticePayload([]byte(body)); err == nil {
				t.Fatal("se esperaba un error")
			}
		})
	}
}
