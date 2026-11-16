package email

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/matiasfelau/vinculo-vial-notification/internal/domain"
	"github.com/matiasfelau/vinculo-vial-notification/internal/repository/memory"
)

type fakeSender struct {
	mu       sync.Mutex
	messages []domain.EmailMessage
	// errs se consume de a uno por llamada; vacio significa exito.
	errs []error
}

func (f *fakeSender) Send(_ context.Context, message domain.EmailMessage) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.messages = append(f.messages, message)
	if len(f.errs) > 0 {
		err := f.errs[0]
		f.errs = f.errs[1:]
		return err
	}
	return nil
}

func (f *fakeSender) sent() []domain.EmailMessage {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]domain.EmailMessage(nil), f.messages...)
}

type fixture struct {
	service *SendEmailService
	sender  *fakeSender
	logs    *memory.NotificationLogRepository
}

func newFixture() fixture {
	logs := memory.NewNotificationLogRepository()
	sender := &fakeSender{}
	service := NewSendEmailService(memory.NewUnitOfWork(logs, memory.NewDeviceTokenRepository()), sender)
	service.now = func() time.Time { return testNow }
	return fixture{service: service, sender: sender, logs: logs}
}

func verificationRequest(to string) Request {
	return Request{Template: TemplateEmailVerification, To: to, Code: ptr("123456"), ExpiresAt: ptr(testNow.Add(10 * time.Minute))}
}

// captureLogs redirige el logger estandar y devuelve lo escrito hasta el
// momento.
func captureLogs(t *testing.T) func() string {
	t.Helper()

	var buf bytes.Buffer
	var mu sync.Mutex
	log.SetOutput(writerFunc(func(p []byte) (int, error) {
		mu.Lock()
		defer mu.Unlock()
		return buf.Write(p)
	}))
	t.Cleanup(func() { log.SetOutput(os.Stderr) })

	return func() string {
		mu.Lock()
		defer mu.Unlock()
		return buf.String()
	}
}

type writerFunc func([]byte) (int, error)

func (f writerFunc) Write(p []byte) (int, error) { return f(p) }

func TestExecute_EnviaElEmailDeLaPlantillaYRegistraElEvento(t *testing.T) {
	f := newFixture()
	eventID := uuid.New()

	if err := f.service.Execute(context.Background(), eventID, verificationRequest("ana@example.com")); err != nil {
		t.Fatalf("Execute() devolvio un error inesperado: %v", err)
	}

	sent := f.sender.sent()
	if len(sent) != 1 {
		t.Fatalf("se esperaba 1 envio, se obtuvieron %d", len(sent))
	}
	if sent[0].To != "ana@example.com" || sent[0].Subject != "Tu código de Vinculo Vial" || !strings.Contains(sent[0].TextBody, "123456") {
		t.Errorf("mensaje inesperado: %+v", sent[0])
	}

	entry, ok := f.logs.Get(eventID)
	if !ok || entry.Status != domain.StatusSent {
		t.Fatalf("registro = %+v (existe=%v), se esperaba un registro SENT", entry, ok)
	}
	if entry.RecipientID != nil {
		t.Error("el registro no debe guardar un destinatario")
	}
}

func TestExecute_EventoDuplicadoNoVuelveAEnviar(t *testing.T) {
	f := newFixture()
	eventID := uuid.New()

	for i := 0; i < 3; i++ {
		if err := f.service.Execute(context.Background(), eventID, verificationRequest("ana@example.com")); err != nil {
			t.Fatalf("Execute() #%d devolvio un error inesperado: %v", i+1, err)
		}
	}

	if got := len(f.sender.sent()); got != 1 {
		t.Fatalf("se esperaba 1 envio pese a 3 entregas del mismo evento, se obtuvieron %d", got)
	}
}

func TestExecute_EntregasConcurrentesDelMismoEventoEnvianUnaVez(t *testing.T) {
	f := newFixture()
	eventID := uuid.New()

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = f.service.Execute(context.Background(), eventID, verificationRequest("ana@example.com"))
		}()
	}
	wg.Wait()

	if got := len(f.sender.sent()); got != 1 {
		t.Fatalf("se esperaba 1 envio, se obtuvieron %d", got)
	}
}

func TestExecute_FallaTransitoriaRevierteElRegistroYElReintentoEnvia(t *testing.T) {
	f := newFixture()
	f.sender.errs = []error{errors.New("smtp: conexion rechazada")}
	eventID := uuid.New()

	err := f.service.Execute(context.Background(), eventID, verificationRequest("ana@example.com"))
	if err == nil || errors.Is(err, domain.ErrEmailRejected) || errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("error = %v, se esperaba un error transitorio", err)
	}
	if _, ok := f.logs.Get(eventID); ok {
		t.Fatal("tras una falla transitoria el evento no debe quedar registrado")
	}

	if err := f.service.Execute(context.Background(), eventID, verificationRequest("ana@example.com")); err != nil {
		t.Fatalf("el reintento devolvio un error inesperado: %v", err)
	}
	if got := len(f.sender.sent()); got != 2 {
		t.Fatalf("se esperaban 2 intentos de envio (falla y reintento), se obtuvieron %d", got)
	}
	if entry, ok := f.logs.Get(eventID); !ok || entry.Status != domain.StatusSent {
		t.Fatalf("tras el reintento exitoso se esperaba un registro SENT, se obtuvo %+v (existe=%v)", entry, ok)
	}
}

func TestExecute_RechazoPermanenteSeConfirmaSinReintentoYSeRegistraComoFallido(t *testing.T) {
	f := newFixture()
	f.sender.errs = []error{fmt.Errorf("%w: codigo 550", domain.ErrEmailRejected)}
	eventID := uuid.New()

	if err := f.service.Execute(context.Background(), eventID, verificationRequest("ana@example.com")); err != nil {
		t.Fatalf("un rechazo permanente se confirma sin error, se obtuvo: %v", err)
	}

	entry, ok := f.logs.Get(eventID)
	if !ok || entry.Status != domain.StatusFailed {
		t.Fatalf("registro = %+v (existe=%v), se esperaba un registro FAILED", entry, ok)
	}

	// Una reentrega del mismo evento no vuelve a pegarle al servidor.
	if err := f.service.Execute(context.Background(), eventID, verificationRequest("ana@example.com")); err != nil {
		t.Fatalf("la reentrega devolvio un error inesperado: %v", err)
	}
	if got := len(f.sender.sent()); got != 1 {
		t.Fatalf("se esperaba 1 solo intento de envio, se obtuvieron %d", got)
	}
}

func TestExecute_EmailVencidoSeDescartaSinEnviarNiRegistrar(t *testing.T) {
	f := newFixture()
	eventID := uuid.New()
	request := Request{Template: TemplateEmailVerification, To: "ana@example.com", Code: ptr("123456"), ExpiresAt: ptr(testNow.Add(-time.Second))}

	if err := f.service.Execute(context.Background(), eventID, request); err != nil {
		t.Fatalf("un email vencido se confirma sin error, se obtuvo: %v", err)
	}

	if got := len(f.sender.sent()); got != 0 {
		t.Fatalf("un email vencido no debe enviarse, se enviaron %d", got)
	}
}

func TestExecute_PedidoInvalidoDevuelveErrorPermanenteSinEnviar(t *testing.T) {
	f := newFixture()

	err := f.service.Execute(context.Background(), uuid.New(), Request{Template: "OTRA", To: "ana@example.com"})

	if !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("error = %v, se esperaba ErrInvalidRequest", err)
	}
	if got := len(f.sender.sent()); got != 0 {
		t.Fatalf("un pedido invalido no debe enviarse, se enviaron %d", got)
	}
}

// Ni el destinatario ni el codigo aparecen en los logs, en ninguno de los
// resultados posibles.
func TestExecute_NoLogueaDestinatarioNiCodigo(t *testing.T) {
	const to = "persona.secreta@example.com"
	const code = "987654"

	scenarios := map[string][]error{
		"envio exitoso":      nil,
		"falla transitoria":  {errors.New("smtp: conexion rechazada")},
		"rechazo permanente": {fmt.Errorf("%w: codigo 550", domain.ErrEmailRejected)},
	}

	for name, errs := range scenarios {
		t.Run(name, func(t *testing.T) {
			logs := captureLogs(t)
			f := newFixture()
			f.sender.errs = errs
			request := Request{Template: TemplateEmailVerification, To: to, Code: ptr(code), ExpiresAt: ptr(testNow.Add(10 * time.Minute))}

			err := f.service.Execute(context.Background(), uuid.New(), request)

			output := logs()
			if err != nil {
				output += err.Error()
			}
			if strings.Contains(output, to) || strings.Contains(output, "persona.secreta") || strings.Contains(output, code) {
				t.Errorf("el log contiene el destinatario o el codigo:\n%s", output)
			}
		})
	}

	t.Run("email vencido", func(t *testing.T) {
		logs := captureLogs(t)
		f := newFixture()
		request := Request{Template: TemplateEmailVerification, To: to, Code: ptr(code), ExpiresAt: ptr(testNow.Add(-time.Minute))}

		_ = f.service.Execute(context.Background(), uuid.New(), request)

		if output := logs(); strings.Contains(output, to) || strings.Contains(output, code) {
			t.Errorf("el log contiene el destinatario o el codigo:\n%s", output)
		}
	})
}
