package smtp

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"mime"
	"mime/multipart"
	"net/mail"
	"strings"
	"testing"
	"time"

	"github.com/matiasfelau/vinculo-vial-notification/internal/domain"
	"github.com/matiasfelau/vinculo-vial-notification/pkg/breaker"
)

const testRecipient = "persona.secreta@example.com"

func testMessage() domain.EmailMessage {
	return domain.EmailMessage{
		To:       testRecipient,
		Subject:  "Tu código de Vinculo Vial",
		TextBody: "Tu código es 123456.\nVence en 10 minutos.",
		HTMLBody: "<!doctype html><html><body><p>Tu código es 123456.</p></body></html>",
	}
}

func newTestSender(t *testing.T, server *testServer, mutate func(*Config)) *Sender {
	t.Helper()

	cfg := Config{
		Host:    "127.0.0.1",
		Port:    server.port(),
		From:    "Vinculo Vial <no-responder@vinculovial.local>",
		Timeout: 2 * time.Second,
	}
	if mutate != nil {
		mutate(&cfg)
	}

	sender, err := NewSender(cfg, nil)
	if err != nil {
		t.Fatalf("NewSender() devolvio un error inesperado: %v", err)
	}
	return sender
}

func TestSend_EntregaElMensajeConRemitenteDestinatarioYPartes(t *testing.T) {
	server := newTestServer(t, nil)
	sender := newTestSender(t, server, nil)
	now = func() time.Time { return time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC) }
	t.Cleanup(func() { now = time.Now })

	if err := sender.Send(context.Background(), testMessage()); err != nil {
		t.Fatalf("Send() devolvio un error inesperado: %v", err)
	}

	received := server.received()
	if len(received) != 1 {
		t.Fatalf("el servidor recibio %d mensajes, se esperaba 1", len(received))
	}
	got := received[0]
	if got.from != "no-responder@vinculovial.local" {
		t.Errorf("MAIL FROM = %q, se esperaba el remitente configurado", got.from)
	}
	if len(got.rcpts) != 1 || got.rcpts[0] != testRecipient {
		t.Errorf("RCPT TO = %v, se esperaba [%s]", got.rcpts, testRecipient)
	}

	parsed, err := mail.ReadMessage(strings.NewReader(got.data))
	if err != nil {
		t.Fatalf("el mensaje recibido no es RFC 5322 valido: %v", err)
	}
	subject, err := new(mime.WordDecoder).DecodeHeader(parsed.Header.Get("Subject"))
	if err != nil || subject != "Tu código de Vinculo Vial" {
		t.Errorf("Subject = %q (err=%v), se esperaba el asunto con acento", subject, err)
	}
	if from := parsed.Header.Get("From"); !strings.Contains(from, "no-responder@vinculovial.local") || !strings.Contains(from, "Vinculo Vial") {
		t.Errorf("From = %q", from)
	}
	if to := parsed.Header.Get("To"); !strings.Contains(to, testRecipient) {
		t.Errorf("To = %q, se esperaba el destinatario", to)
	}
	if id := parsed.Header.Get("Message-ID"); !strings.HasSuffix(id, "@vinculovial.local>") {
		t.Errorf("Message-ID = %q, se esperaba un id propio del dominio del remitente", id)
	}
	if date := parsed.Header.Get("Date"); !strings.Contains(date, "15 Nov 2026") {
		t.Errorf("Date = %q", date)
	}

	mediaType, params, err := mime.ParseMediaType(parsed.Header.Get("Content-Type"))
	if err != nil || mediaType != "multipart/alternative" {
		t.Fatalf("Content-Type = %q (err=%v), se esperaba multipart/alternative", mediaType, err)
	}
	reader := multipart.NewReader(parsed.Body, params["boundary"])
	bodies := map[string]string{}
	for {
		part, err := reader.NextPart()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("parte MIME invalida: %v", err)
		}
		raw, err := io.ReadAll(part)
		if err != nil {
			t.Fatalf("no se pudo leer la parte: %v", err)
		}
		partType, _, _ := mime.ParseMediaType(part.Header.Get("Content-Type"))
		bodies[partType] = strings.ReplaceAll(string(raw), "\r\n", "\n")
	}
	if want := "Tu código es 123456.\nVence en 10 minutos."; strings.TrimSpace(bodies["text/plain"]) != want {
		t.Errorf("parte de texto = %q, se esperaba %q", bodies["text/plain"], want)
	}
	if !strings.Contains(bodies["text/html"], "<p>Tu código es 123456.</p>") {
		t.Errorf("parte HTML = %q", bodies["text/html"])
	}
}

func TestSend_RechazoPermanenteEnMailRcptYData(t *testing.T) {
	for _, step := range []struct {
		verb  string
		reply string
	}{
		{"MAIL", "550 5.7.1 remitente no autorizado " + testRecipient},
		{"RCPT", "550 5.1.1 <" + testRecipient + ">: Recipient address rejected"},
		{"DATA", "554 5.6.0 no acepto datos para " + testRecipient},
		{"END", "554 5.7.1 mensaje rechazado como spam para " + testRecipient},
	} {
		t.Run(step.verb, func(t *testing.T) {
			server := newTestServer(t, func(s *testServer) { s.replies[step.verb] = step.reply })
			sender := newTestSender(t, server, nil)

			err := sender.Send(context.Background(), testMessage())

			if !errors.Is(err, domain.ErrEmailRejected) {
				t.Fatalf("error = %v, se esperaba ErrEmailRejected", err)
			}
			if strings.Contains(err.Error(), testRecipient) || strings.Contains(err.Error(), "persona.secreta") {
				t.Errorf("el error no deberia reproducir el texto del servidor ni el destinatario: %v", err)
			}
			if !strings.Contains(err.Error(), "55") {
				t.Errorf("el error deberia conservar el codigo de la respuesta: %v", err)
			}
			if len(server.received()) != 0 {
				t.Error("el servidor no deberia haber aceptado el mensaje")
			}
		})
	}
}

func TestSend_Respuesta4xxEsTransitoria(t *testing.T) {
	for _, step := range []struct {
		verb  string
		reply string
	}{
		{"RCPT", "451 4.3.0 <" + testRecipient + ">: intenta mas tarde"},
		{"MAIL", "454 4.7.0 throttling para " + testRecipient},
		{"END", "452 4.3.1 sin espacio para " + testRecipient},
	} {
		t.Run(step.verb, func(t *testing.T) {
			server := newTestServer(t, func(s *testServer) { s.replies[step.verb] = step.reply })
			sender := newTestSender(t, server, nil)

			err := sender.Send(context.Background(), testMessage())

			if err == nil || errors.Is(err, domain.ErrEmailRejected) {
				t.Fatalf("error = %v, se esperaba un error transitorio", err)
			}
			if strings.Contains(err.Error(), testRecipient) || strings.Contains(err.Error(), "persona.secreta") {
				t.Errorf("el error no deberia reproducir el texto del servidor: %v", err)
			}
		})
	}
}

// Un 5xx en el saludo, en la autenticacion o en STARTTLS no rechaza este
// mensaje: es un problema de la conexion o de la configuracion, y se
// reintenta en lugar de descartar el email.
func TestSend_5xxFueraDelDialogoDeEnvioEsTransitorio(t *testing.T) {
	cert, pool := selfSignedCert(t)

	cases := []struct {
		name      string
		configure func(*testServer)
		mutate    func(*Config)
	}{
		{"saludo", func(s *testServer) { s.greeting = "554 sin servicio" }, nil},
		{"autenticacion", func(s *testServer) { s.advertiseAuth = true; s.replies["AUTH"] = "535 5.7.8 credenciales invalidas" }, func(c *Config) { c.Username, c.Password = "u", "p" }},
		{"starttls", func(s *testServer) {
			s.tlsConfig = &tls.Config{Certificates: []tls.Certificate{cert}}
			s.replies["STARTTLS"] = "554 5.7.0 TLS no disponible"
		}, func(c *Config) { c.StartTLS = true }},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := newTestServer(t, tc.configure)
			sender := newTestSender(t, server, tc.mutate)
			sender.tlsConfig = func() *tls.Config {
				return &tls.Config{RootCAs: pool, ServerName: "127.0.0.1", MinVersion: tls.VersionTLS12}
			}

			err := sender.Send(context.Background(), testMessage())

			if err == nil || errors.Is(err, domain.ErrEmailRejected) {
				t.Fatalf("error = %v, se esperaba un error transitorio", err)
			}
		})
	}
}

func TestSend_ConStartTLSNegociaTLSYAutenticaAntesDeEnviar(t *testing.T) {
	cert, pool := selfSignedCert(t)
	server := newTestServer(t, func(s *testServer) {
		s.tlsConfig = &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
		s.advertiseAuth = true
	})
	sender := newTestSender(t, server, func(c *Config) {
		c.StartTLS = true
		c.Username = "usuario-smtp"
		c.Password = "clave-smtp"
	})
	sender.tlsConfig = func() *tls.Config {
		return &tls.Config{RootCAs: pool, ServerName: "127.0.0.1", MinVersion: tls.VersionTLS12}
	}

	if err := sender.Send(context.Background(), testMessage()); err != nil {
		t.Fatalf("Send() devolvio un error inesperado: %v", err)
	}

	received := server.received()
	if len(received) != 1 {
		t.Fatalf("el servidor recibio %d mensajes, se esperaba 1", len(received))
	}
	if !received[0].tls {
		t.Error("el mensaje deberia haber viajado sobre TLS")
	}
	if received[0].authUser != "usuario-smtp" || received[0].authPass != "clave-smtp" {
		t.Errorf("credenciales recibidas = %q/%q, se esperaban las configuradas", received[0].authUser, received[0].authPass)
	}
}

// Con STARTTLS exigido, un servidor que no lo ofrece nunca recibe el
// mensaje en texto plano.
func TestSend_StartTLSExigidoNoDegradaATextoPlano(t *testing.T) {
	server := newTestServer(t, nil)
	sender := newTestSender(t, server, func(c *Config) { c.StartTLS = true })

	err := sender.Send(context.Background(), testMessage())

	if err == nil || errors.Is(err, domain.ErrEmailRejected) {
		t.Fatalf("error = %v, se esperaba un error transitorio", err)
	}
	if len(server.received()) != 0 {
		t.Fatal("el mensaje no deberia haberse enviado sin TLS")
	}
}

func TestSend_ConStartTLSRechazaUnCertificadoNoConfiable(t *testing.T) {
	cert, _ := selfSignedCert(t)
	server := newTestServer(t, func(s *testServer) { s.tlsConfig = &tls.Config{Certificates: []tls.Certificate{cert}} })
	sender := newTestSender(t, server, func(c *Config) { c.StartTLS = true })
	// Sin tocar tlsConfig: usa la verificacion del sistema, que no confia
	// en el certificado de prueba.

	err := sender.Send(context.Background(), testMessage())

	var unknownAuthority x509.UnknownAuthorityError
	if !errors.As(err, &unknownAuthority) {
		t.Fatalf("error = %v, se esperaba un certificado de autoridad desconocida", err)
	}
	if len(server.received()) != 0 {
		t.Fatal("no deberia haberse enviado nada")
	}
}

func TestSend_SinCredencialesNoAutentica(t *testing.T) {
	server := newTestServer(t, func(s *testServer) { s.advertiseAuth = true })
	sender := newTestSender(t, server, nil)

	if err := sender.Send(context.Background(), testMessage()); err != nil {
		t.Fatalf("Send() devolvio un error inesperado: %v", err)
	}

	if received := server.received(); len(received) != 1 || received[0].authUser != "" {
		t.Fatalf("no deberia haber autenticacion sin credenciales: %+v", received)
	}
}

func TestSend_TimeoutCortaUnServidorQueNoResponde(t *testing.T) {
	server := newTestServer(t, func(s *testServer) { s.hang = true })
	sender := newTestSender(t, server, func(c *Config) { c.Timeout = 150 * time.Millisecond })

	start := time.Now()
	err := sender.Send(context.Background(), testMessage())

	if err == nil || errors.Is(err, domain.ErrEmailRejected) {
		t.Fatalf("error = %v, se esperaba un error transitorio por timeout", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("Send() tardo %s, el timeout era de 150ms", elapsed)
	}
}

func TestSend_ContextoCancelado_CortaLaConexion(t *testing.T) {
	server := newTestServer(t, func(s *testServer) { s.hang = true })
	sender := newTestSender(t, server, func(c *Config) { c.Timeout = 30 * time.Second })

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := sender.Send(ctx, testMessage())

	if err == nil {
		t.Fatal("se esperaba un error por contexto vencido")
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("Send() tardo %s pese a que el contexto vencio a los 150ms", elapsed)
	}
}

func TestSend_ServidorInalcanzableEsTransitorio(t *testing.T) {
	server := newTestServer(t, nil)
	sender := newTestSender(t, server, nil)
	_ = server.listener.Close()

	err := sender.Send(context.Background(), testMessage())

	if err == nil || errors.Is(err, domain.ErrEmailRejected) {
		t.Fatalf("error = %v, se esperaba un error transitorio de conexion", err)
	}
	if strings.Contains(err.Error(), testRecipient) {
		t.Errorf("el error no deberia incluir el destinatario: %v", err)
	}
}

func TestSend_DestinatarioInvalidoSeRechazaSinTocarLaRed(t *testing.T) {
	server := newTestServer(t, nil)
	sender := newTestSender(t, server, nil)

	for _, to := range []string{"", "no-es-un-email", "Ana <ana@example.com>", "ana@example.com\r\nBcc: otro@example.com"} {
		message := testMessage()
		message.To = to

		err := sender.Send(context.Background(), message)

		if !errors.Is(err, domain.ErrEmailRejected) {
			t.Errorf("To=%q: error = %v, se esperaba ErrEmailRejected", to, err)
		}
	}
	if got := server.connections.Load(); got != 0 {
		t.Errorf("no deberia haberse abierto ninguna conexion, se abrieron %d", got)
	}
}

func TestSend_AsuntoConSaltoDeLineaNoSeEnvia(t *testing.T) {
	server := newTestServer(t, nil)
	sender := newTestSender(t, server, nil)
	message := testMessage()
	message.Subject = "Hola\r\nBcc: otro@example.com"

	if err := sender.Send(context.Background(), message); err == nil {
		t.Fatal("se esperaba un error por asunto invalido")
	}
	if len(server.received()) != 0 {
		t.Fatal("no deberia haberse enviado nada")
	}
}

// El cuerpo viaja intacto aunque contenga lineas con un punto solo o una
// linea que imita el fin de datos.
func TestSend_CuerpoConPuntoSoloNoTruncaElMensaje(t *testing.T) {
	server := newTestServer(t, nil)
	sender := newTestSender(t, server, nil)
	message := testMessage()
	message.TextBody = "antes\n.\ndespues"

	if err := sender.Send(context.Background(), message); err != nil {
		t.Fatalf("Send() devolvio un error inesperado: %v", err)
	}

	received := server.received()
	if len(received) != 1 || !strings.Contains(received[0].data, "despues") {
		t.Fatalf("el cuerpo llego truncado: %+v", received)
	}
}

func newBreaker(threshold int) *breaker.Breaker {
	return breaker.New(breaker.Config{FailureThreshold: threshold, Window: time.Minute, OpenDuration: time.Minute})
}

func TestSend_BreakerAbreTrasFallasTransitoriasYRechazaSinTocarLaRed(t *testing.T) {
	server := newTestServer(t, func(s *testServer) { s.replies["RCPT"] = "451 4.3.0 intenta mas tarde" })
	sender := newTestSender(t, server, nil)
	sender.breaker = newBreaker(2)

	for i := 0; i < 2; i++ {
		if err := sender.Send(context.Background(), testMessage()); err == nil {
			t.Fatalf("envio #%d: se esperaba un error transitorio", i+1)
		}
	}
	connections := server.connections.Load()

	err := sender.Send(context.Background(), testMessage())

	if !errors.Is(err, breaker.ErrOpen) {
		t.Fatalf("error = %v, se esperaba breaker.ErrOpen con el circuito abierto", err)
	}
	if got := server.connections.Load(); got != connections {
		t.Errorf("con el circuito abierto no debe haber conexiones nuevas: antes %d, ahora %d", connections, got)
	}
}

// Un rechazo permanente es una respuesta valida del servidor: no cuenta
// como falla de la dependencia ni abre el circuito.
func TestSend_RechazoPermanenteNoAbreElBreaker(t *testing.T) {
	server := newTestServer(t, func(s *testServer) { s.replies["RCPT"] = "550 5.1.1 no existe" })
	sender := newTestSender(t, server, nil)
	sender.breaker = newBreaker(2)

	for i := 0; i < 5; i++ {
		err := sender.Send(context.Background(), testMessage())
		if !errors.Is(err, domain.ErrEmailRejected) {
			t.Fatalf("envio #%d: error = %v, se esperaba ErrEmailRejected (el circuito no debe abrirse)", i+1, err)
		}
	}
}

func TestNewSender_ValidaLaConfiguracion(t *testing.T) {
	valid := Config{Host: "mailpit", Port: 1025, From: "Vinculo Vial <no-responder@vinculovial.local>", Timeout: time.Second}

	cases := map[string]func(*Config){
		"sin host":           func(c *Config) { c.Host = "" },
		"puerto cero":        func(c *Config) { c.Port = 0 },
		"puerto fuera":       func(c *Config) { c.Port = 70000 },
		"sin timeout":        func(c *Config) { c.Timeout = 0 },
		"remitente invalido": func(c *Config) { c.From = "no-es-una-direccion" },
	}

	if _, err := NewSender(valid, nil); err != nil {
		t.Fatalf("la configuracion valida fue rechazada: %v", err)
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			cfg := valid
			mutate(&cfg)
			if _, err := NewSender(cfg, nil); err == nil {
				t.Fatal("se esperaba un error")
			}
		})
	}
}
