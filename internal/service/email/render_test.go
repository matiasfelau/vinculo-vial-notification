package email

import (
	"errors"
	"strings"
	"testing"
	"time"
)

var testNow = time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)

func ptr[T any](v T) *T { return &v }

func TestRender_PlantillasDelContrato(t *testing.T) {
	cases := []struct {
		name     Template
		request  Request
		subject  string
		contains []string
	}{
		{
			name:    TemplateEmailVerification,
			request: Request{Template: TemplateEmailVerification, To: "ana@example.com", Code: ptr("123456"), ExpiresAt: ptr(testNow.Add(10 * time.Minute))},
			subject: "Tu código de Vinculo Vial",
			contains: []string{
				"123456", "10 minutos", "Si no fuiste vos, ignorá este mail",
			},
		},
		{
			name:    TemplateAccountExists,
			request: Request{Template: TemplateAccountExists, To: "ana@example.com"},
			subject: "Ya tenés una cuenta en Vinculo Vial",
			contains: []string{
				"ya tiene una cuenta", "recuperarla desde la app", "Si no fuiste vos, ignorá este mail",
			},
		},
		{
			name:    TemplatePasswordReset,
			request: Request{Template: TemplatePasswordReset, To: "ana@example.com", Code: ptr("654321"), ExpiresAt: ptr(testNow.Add(15 * time.Minute))},
			subject: "Recuperá tu contraseña",
			contains: []string{
				"654321", "15 minutos", "nadie puede cambiar tu contraseña sin validar tu identidad",
			},
		},
		{
			name:    TemplatePasswordChanged,
			request: Request{Template: TemplatePasswordChanged, To: "ana@example.com", OccurredAt: testNow},
			subject: "Cambiaste tu contraseña",
			contains: []string{
				"15/11/2026", "09:00", "se cerraron todas tus sesiones", "recuperá tu cuenta desde la app",
			},
		},
	}

	for _, tc := range cases {
		t.Run(string(tc.name), func(t *testing.T) {
			message, err := Render(tc.request, testNow)
			if err != nil {
				t.Fatalf("Render() devolvio un error inesperado: %v", err)
			}

			if message.To != "ana@example.com" {
				t.Errorf("To = %q, se esperaba el destinatario del pedido", message.To)
			}
			if message.Subject != tc.subject {
				t.Errorf("Subject = %q, se esperaba %q", message.Subject, tc.subject)
			}
			for _, want := range tc.contains {
				if !strings.Contains(message.TextBody, want) {
					t.Errorf("el texto plano no contiene %q:\n%s", want, message.TextBody)
				}
				if !strings.Contains(message.HTMLBody, want) {
					t.Errorf("el HTML no contiene %q:\n%s", want, message.HTMLBody)
				}
			}
		})
	}
}

// El contrato prohibe imagenes remotas y enlaces de seguimiento: ninguna
// plantilla puede contener recursos externos ni enlaces.
func TestRender_SinImagenesRemotasNiEnlaces(t *testing.T) {
	requests := []Request{
		{Template: TemplateEmailVerification, To: "ana@example.com", Code: ptr("123456"), ExpiresAt: ptr(testNow.Add(10 * time.Minute))},
		{Template: TemplateAccountExists, To: "ana@example.com"},
		{Template: TemplatePasswordReset, To: "ana@example.com", Code: ptr("123456"), ExpiresAt: ptr(testNow.Add(10 * time.Minute))},
		{Template: TemplatePasswordChanged, To: "ana@example.com"},
	}

	for _, request := range requests {
		t.Run(string(request.Template), func(t *testing.T) {
			message, err := Render(request, testNow)
			if err != nil {
				t.Fatalf("Render() devolvio un error inesperado: %v", err)
			}

			for _, body := range []string{message.TextBody, message.HTMLBody} {
				lower := strings.ToLower(body)
				for _, forbidden := range []string{"http:", "https:", "<img", "<a ", "<script", "<link", "src="} {
					if strings.Contains(lower, forbidden) {
						t.Errorf("el cuerpo contiene %q y no deberia:\n%s", forbidden, body)
					}
				}
			}
			if !strings.Contains(message.HTMLBody, "<html") {
				t.Errorf("el HTML deberia ser un documento minimo: %s", message.HTMLBody)
			}
		})
	}
}

func TestRender_VencimientoEnMinutosRestantes(t *testing.T) {
	cases := []struct {
		name      string
		expiresIn time.Duration
		want      string
	}{
		{"redondea hacia arriba", 9*time.Minute + 1*time.Second, "10 minutos"},
		{"un minuto exacto va en singular", time.Minute, "1 minuto."},
		{"menos de un minuto se muestra como uno", 20 * time.Second, "1 minuto."},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			request := Request{Template: TemplateEmailVerification, To: "ana@example.com", Code: ptr("123456"), ExpiresAt: ptr(testNow.Add(tc.expiresIn))}

			message, err := Render(request, testNow)
			if err != nil {
				t.Fatalf("Render() devolvio un error inesperado: %v", err)
			}
			if !strings.Contains(message.TextBody, tc.want) {
				t.Errorf("el texto no contiene %q:\n%s", tc.want, message.TextBody)
			}
		})
	}
}

// PASSWORD_CHANGED muestra la hora de Buenos Aires aunque el evento venga
// en UTC, y respeta el cambio de dia.
func TestRender_PasswordChangedUsaLaHoraDeBuenosAires(t *testing.T) {
	occurredAt := time.Date(2026, 1, 2, 2, 30, 0, 0, time.UTC)

	message, err := Render(Request{Template: TemplatePasswordChanged, To: "ana@example.com", OccurredAt: occurredAt}, testNow)
	if err != nil {
		t.Fatalf("Render() devolvio un error inesperado: %v", err)
	}

	if !strings.Contains(message.TextBody, "01/01/2026 a las 23:30") {
		t.Errorf("se esperaba 01/01/2026 a las 23:30 (UTC-3), se obtuvo:\n%s", message.TextBody)
	}
}

func TestRender_PasswordChangedSinInstanteUsaAhora(t *testing.T) {
	message, err := Render(Request{Template: TemplatePasswordChanged, To: "ana@example.com"}, testNow)
	if err != nil {
		t.Fatalf("Render() devolvio un error inesperado: %v", err)
	}

	if !strings.Contains(message.TextBody, "15/11/2026 a las 09:00") {
		t.Errorf("se esperaba la hora actual en Buenos Aires, se obtuvo:\n%s", message.TextBody)
	}
}

func TestRender_PedidosInvalidosSonPermanentesYNoRevelanDatos(t *testing.T) {
	validCode := ptr("123456")
	validExpiry := ptr(testNow.Add(10 * time.Minute))

	cases := []struct {
		name    string
		request Request
	}{
		{"plantilla desconocida", Request{Template: "OTRA", To: "ana@example.com"}},
		{"destinatario vacio", Request{Template: TemplateAccountExists, To: ""}},
		{"destinatario mal formado", Request{Template: TemplateAccountExists, To: "no-es-un-email"}},
		{"destinatario con nombre", Request{Template: TemplateAccountExists, To: "Ana <ana@example.com>"}},
		{"destinatario con salto de linea", Request{Template: TemplateAccountExists, To: "ana@example.com\r\nBcc: otro@example.com"}},
		{"codigo ausente", Request{Template: TemplateEmailVerification, To: "ana@example.com", ExpiresAt: validExpiry}},
		{"codigo no numerico", Request{Template: TemplatePasswordReset, To: "ana@example.com", Code: ptr("12<b>"), ExpiresAt: validExpiry}},
		{"codigo demasiado largo", Request{Template: TemplatePasswordReset, To: "ana@example.com", Code: ptr("1234567890123"), ExpiresAt: validExpiry}},
		{"codigo sin vencimiento", Request{Template: TemplateEmailVerification, To: "ana@example.com", Code: validCode}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Render(tc.request, testNow)

			if !errors.Is(err, ErrInvalidRequest) {
				t.Fatalf("error = %v, se esperaba ErrInvalidRequest", err)
			}
			if tc.request.To != "" && strings.Contains(err.Error(), tc.request.To) {
				t.Errorf("el error no deberia incluir el destinatario: %v", err)
			}
		})
	}
}

// Los codigos solo viajan en las plantillas que los llevan: un codigo en un
// pedido de ACCOUNT_EXISTS se ignora en lugar de incrustarse.
func TestRender_AccountExistsIgnoraElCodigo(t *testing.T) {
	message, err := Render(Request{Template: TemplateAccountExists, To: "ana@example.com", Code: ptr("999999")}, testNow)
	if err != nil {
		t.Fatalf("Render() devolvio un error inesperado: %v", err)
	}

	if strings.Contains(message.TextBody, "999999") || strings.Contains(message.HTMLBody, "999999") {
		t.Error("ACCOUNT_EXISTS no debe incluir un codigo")
	}
}
