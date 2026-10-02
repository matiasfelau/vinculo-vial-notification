package dispatch

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/matiasfelau/vinculo-vial-notification/internal/domain"
	"github.com/matiasfelau/vinculo-vial-notification/internal/repository/memory"
)

// fakeDispatcher registra cada llamada para poder verificar cuantas
// veces se disparo el envio y con que tokens. Los tokens listados en
// invalidTokens se devuelven marcados como Invalid, simulando una
// respuesta Unregistered o InvalidArgument de FCM.
//
// Es seguro para uso concurrente. started y release permiten
// controlar una entrega en vuelo: started recibe una senial cuando
// Dispatch arranca, y Dispatch espera en release (si no es nil) antes
// de responder.
type fakeDispatcher struct {
	mu            sync.Mutex
	calls         [][]string
	err           error
	failFirst     int // con err != nil: 0 = falla siempre; N = falla solo las primeras N llamadas
	invalidTokens map[string]bool
	started       chan struct{}
	release       chan struct{}
}

func (f *fakeDispatcher) Dispatch(_ context.Context, tokens []string, _ string) ([]domain.DispatchOutcome, error) {
	f.mu.Lock()
	f.calls = append(f.calls, tokens)
	attempt := len(f.calls)
	f.mu.Unlock()

	if f.started != nil {
		f.started <- struct{}{}
	}
	if f.release != nil {
		<-f.release
	}

	if f.err != nil && (f.failFirst == 0 || attempt <= f.failFirst) {
		return nil, f.err
	}

	outcomes := make([]domain.DispatchOutcome, len(tokens))
	for i, token := range tokens {
		invalid := f.invalidTokens[token]
		outcomes[i] = domain.DispatchOutcome{Token: token, Success: !invalid, Invalid: invalid}
	}
	return outcomes, nil
}

func (f *fakeDispatcher) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

func setup() (*DispatchAlertService, *memory.DeviceTokenRepository, *memory.NotificationLogRepository, *fakeDispatcher) {
	deviceTokens := memory.NewDeviceTokenRepository()
	notificationLogs := memory.NewNotificationLogRepository()
	dispatcher := &fakeDispatcher{}
	service := NewDispatchAlertService(memory.NewUnitOfWork(notificationLogs, deviceTokens), deviceTokens, dispatcher)
	return service, deviceTokens, notificationLogs, dispatcher
}

func TestDispatchAlertService_DespachaALosTokensDeLosDestinatarios(t *testing.T) {
	service, deviceTokens, _, dispatcher := setup()
	ctx := context.Background()

	owner := uuid.New()
	_ = deviceTokens.Upsert(ctx, &domain.DeviceToken{ID: uuid.New(), OwnerID: owner, FCMToken: "token-1", UpdatedAt: time.Now()})

	event := AlertCreatedEvent{EventID: uuid.New(), OwnerIDs: []uuid.UUID{owner}, AlertType: "choque"}
	if err := service.Execute(ctx, event); err != nil {
		t.Fatalf("Execute() devolvio un error inesperado: %v", err)
	}

	if len(dispatcher.calls) != 1 || len(dispatcher.calls[0]) != 1 || dispatcher.calls[0][0] != "token-1" {
		t.Fatalf("se esperaba una llamada de despacho con [token-1], se obtuvo: %v", dispatcher.calls)
	}
}

func TestDispatchAlertService_MismoEventIDEntregadoDosVeces_SoloUnDespacho(t *testing.T) {
	service, deviceTokens, _, dispatcher := setup()
	ctx := context.Background()

	owner := uuid.New()
	_ = deviceTokens.Upsert(ctx, &domain.DeviceToken{ID: uuid.New(), OwnerID: owner, FCMToken: "token-1", UpdatedAt: time.Now()})

	event := AlertCreatedEvent{EventID: uuid.New(), OwnerIDs: []uuid.UUID{owner}, AlertType: "choque"}

	if err := service.Execute(ctx, event); err != nil {
		t.Fatalf("Execute() del primer intento devolvio un error inesperado: %v", err)
	}
	if err := service.Execute(ctx, event); err != nil {
		t.Fatalf("Execute() del redelivery devolvio un error inesperado: %v", err)
	}

	if len(dispatcher.calls) != 1 {
		t.Fatalf("se esperaba un unico despacho, se hicieron %d", len(dispatcher.calls))
	}
}

func TestDispatchAlertService_SinTokens_NoDespachaYRegistraSkipped(t *testing.T) {
	service, _, notificationLogs, dispatcher := setup()
	ctx := context.Background()

	event := AlertCreatedEvent{EventID: uuid.New(), OwnerIDs: []uuid.UUID{uuid.New()}, AlertType: "choque"}
	if err := service.Execute(ctx, event); err != nil {
		t.Fatalf("Execute() devolvio un error inesperado: %v", err)
	}

	if len(dispatcher.calls) != 0 {
		t.Fatalf("no deberia haberse despachado nada, se hicieron %d llamadas", len(dispatcher.calls))
	}

	stored, ok := notificationLogs.Get(event.EventID)
	if !ok {
		t.Fatalf("se esperaba un registro para el evento %s", event.EventID)
	}
	if stored.Status != domain.StatusSkippedNoToken {
		t.Fatalf("Status = %q, se esperaba %q", stored.Status, domain.StatusSkippedNoToken)
	}
}

func TestDispatchAlertService_RegistraSentTrasUnDespachoExitoso(t *testing.T) {
	service, deviceTokens, notificationLogs, _ := setup()
	ctx := context.Background()

	owner := uuid.New()
	_ = deviceTokens.Upsert(ctx, &domain.DeviceToken{ID: uuid.New(), OwnerID: owner, FCMToken: "token-1", UpdatedAt: time.Now()})

	event := AlertCreatedEvent{EventID: uuid.New(), OwnerIDs: []uuid.UUID{owner}, AlertType: "choque"}
	if err := service.Execute(ctx, event); err != nil {
		t.Fatalf("Execute() devolvio un error inesperado: %v", err)
	}

	stored, ok := notificationLogs.Get(event.EventID)
	if !ok {
		t.Fatalf("se esperaba un registro para el evento %s", event.EventID)
	}
	if stored.Status != domain.StatusSent {
		t.Fatalf("Status = %q, se esperaba %q", stored.Status, domain.StatusSent)
	}
}

func TestDispatchAlertService_FalloDeFCM_RevierteElRegistroYElReintentoVuelveAEnviar(t *testing.T) {
	service, deviceTokens, notificationLogs, dispatcher := setup()
	dispatcher.err = errors.New("fcm no disponible")
	dispatcher.failFirst = 1
	ctx := context.Background()

	owner := uuid.New()
	_ = deviceTokens.Upsert(ctx, &domain.DeviceToken{ID: uuid.New(), OwnerID: owner, FCMToken: "token-1", UpdatedAt: time.Now()})

	event := AlertCreatedEvent{EventID: uuid.New(), OwnerIDs: []uuid.UUID{owner}, AlertType: "choque"}
	if err := service.Execute(ctx, event); err == nil {
		t.Fatal("se esperaba que Execute() propagara el error del dispatcher")
	}

	// Rollback: el event_id no quedo registrado. Si quedara, el
	// reintento (NACK) se descartaria como duplicado y la push se
	// perderia.
	if _, ok := notificationLogs.Get(event.EventID); ok {
		t.Fatal("tras un fallo de FCM el event_id no deberia estar registrado")
	}

	if err := service.Execute(ctx, event); err != nil {
		t.Fatalf("Execute() del reintento devolvio un error inesperado: %v", err)
	}
	if dispatcher.callCount() != 2 {
		t.Fatalf("se esperaban 2 intentos de despacho (el fallido y el reintento), se hicieron %d", dispatcher.callCount())
	}

	stored, ok := notificationLogs.Get(event.EventID)
	if !ok || stored.Status != domain.StatusSent {
		t.Fatalf("tras el reintento exitoso se esperaba un registro SENT, se obtuvo %+v (existe=%v)", stored, ok)
	}
}

func TestDispatchAlertService_DosEntregasConcurrentes_UnSoloEnvio(t *testing.T) {
	service, deviceTokens, notificationLogs, dispatcher := setup()
	dispatcher.started = make(chan struct{}, 2)
	dispatcher.release = make(chan struct{})
	ctx := context.Background()

	owner := uuid.New()
	_ = deviceTokens.Upsert(ctx, &domain.DeviceToken{ID: uuid.New(), OwnerID: owner, FCMToken: "token-1", UpdatedAt: time.Now()})
	event := AlertCreatedEvent{EventID: uuid.New(), OwnerIDs: []uuid.UUID{owner}, AlertType: "choque"}

	first := make(chan error, 1)
	second := make(chan error, 1)

	go func() { first <- service.Execute(ctx, event) }()
	<-dispatcher.started // la primera esta dentro del despacho, con el event_id reservado

	go func() { second <- service.Execute(ctx, event) }()
	time.Sleep(50 * time.Millisecond) // la segunda queda esperando en la clave unica

	select {
	case err := <-second:
		t.Fatalf("la entrega duplicada no deberia terminar mientras la primera sigue abierta (err=%v)", err)
	default:
	}

	close(dispatcher.release)

	if err := <-first; err != nil {
		t.Fatalf("Execute() de la primera entrega devolvio un error inesperado: %v", err)
	}
	if err := <-second; err != nil {
		t.Fatalf("Execute() de la entrega duplicada devolvio un error inesperado: %v", err)
	}

	if dispatcher.callCount() != 1 {
		t.Fatalf("se esperaba un unico envio entre dos entregas concurrentes, se hicieron %d", dispatcher.callCount())
	}
	if _, ok := notificationLogs.Get(event.EventID); !ok {
		t.Fatal("se esperaba el event_id registrado tras confirmar la primera entrega")
	}
}

func TestDispatchAlertService_PrimeraEntregaFalla_LaDuplicadaEnEsperaTomaElRelevo(t *testing.T) {
	service, deviceTokens, notificationLogs, dispatcher := setup()
	dispatcher.err = errors.New("fcm no disponible")
	dispatcher.failFirst = 1
	dispatcher.started = make(chan struct{}, 2)
	dispatcher.release = make(chan struct{})
	ctx := context.Background()

	owner := uuid.New()
	_ = deviceTokens.Upsert(ctx, &domain.DeviceToken{ID: uuid.New(), OwnerID: owner, FCMToken: "token-1", UpdatedAt: time.Now()})
	event := AlertCreatedEvent{EventID: uuid.New(), OwnerIDs: []uuid.UUID{owner}, AlertType: "choque"}

	first := make(chan error, 1)
	second := make(chan error, 1)

	go func() { first <- service.Execute(ctx, event) }()
	<-dispatcher.started

	go func() { second <- service.Execute(ctx, event) }()
	time.Sleep(50 * time.Millisecond)

	close(dispatcher.release)

	if err := <-first; err == nil {
		t.Fatal("se esperaba que la primera entrega fallara (FCM caido)")
	}
	// La primera revirtio: la duplicada que esperaba no es un duplicado
	// real, asi que procede y envia.
	if err := <-second; err != nil {
		t.Fatalf("la entrega que esperaba deberia haber procedido y enviado, devolvio: %v", err)
	}

	if dispatcher.callCount() != 2 {
		t.Fatalf("se esperaban 2 despachos (el fallido y el de la entrega que tomo el relevo), se hicieron %d", dispatcher.callCount())
	}
	if _, ok := notificationLogs.Get(event.EventID); !ok {
		t.Fatal("se esperaba el event_id registrado tras el envio exitoso")
	}
}

func TestDispatchAlertService_BorraLosTokensInvalidosReportadosPorFCM(t *testing.T) {
	service, deviceTokens, _, dispatcher := setup()
	ctx := context.Background()

	owner := uuid.New()
	_ = deviceTokens.Upsert(ctx, &domain.DeviceToken{ID: uuid.New(), OwnerID: owner, FCMToken: "token-valido", UpdatedAt: time.Now()})
	_ = deviceTokens.Upsert(ctx, &domain.DeviceToken{ID: uuid.New(), OwnerID: owner, FCMToken: "token-invalido", UpdatedAt: time.Now()})
	dispatcher.invalidTokens = map[string]bool{"token-invalido": true}

	event := AlertCreatedEvent{EventID: uuid.New(), OwnerIDs: []uuid.UUID{owner}, AlertType: "choque"}
	if err := service.Execute(ctx, event); err != nil {
		t.Fatalf("Execute() devolvio un error inesperado: %v", err)
	}

	restantes, err := deviceTokens.FindByOwnerIDs(ctx, []uuid.UUID{owner})
	if err != nil {
		t.Fatalf("FindByOwnerIDs() devolvio un error inesperado: %v", err)
	}
	if len(restantes) != 1 || restantes[0].FCMToken != "token-valido" {
		t.Fatalf("se esperaba que solo sobreviviera token-valido, se obtuvo: %+v", restantes)
	}
}

func TestDispatchAlertService_SinTokensInvalidos_NoBorraNada(t *testing.T) {
	service, deviceTokens, _, _ := setup()
	ctx := context.Background()

	owner := uuid.New()
	_ = deviceTokens.Upsert(ctx, &domain.DeviceToken{ID: uuid.New(), OwnerID: owner, FCMToken: "token-1", UpdatedAt: time.Now()})

	event := AlertCreatedEvent{EventID: uuid.New(), OwnerIDs: []uuid.UUID{owner}, AlertType: "choque"}
	if err := service.Execute(ctx, event); err != nil {
		t.Fatalf("Execute() devolvio un error inesperado: %v", err)
	}

	restantes, err := deviceTokens.FindByOwnerIDs(ctx, []uuid.UUID{owner})
	if err != nil {
		t.Fatalf("FindByOwnerIDs() devolvio un error inesperado: %v", err)
	}
	if len(restantes) != 1 {
		t.Fatalf("no deberia haberse borrado ningun token, se obtuvo: %+v", restantes)
	}
}
