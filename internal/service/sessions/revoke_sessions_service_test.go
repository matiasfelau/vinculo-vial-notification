package sessions

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

func TestRevokeSessionsService_BorraTodosLosTokensDelOwner(t *testing.T) {
	deviceTokens := memory.NewDeviceTokenRepository()
	notificationLogs := memory.NewNotificationLogRepository()
	service := NewRevokeSessionsService(memory.NewUnitOfWork(notificationLogs, deviceTokens))
	ctx := context.Background()

	owner := uuid.New()
	otro := uuid.New()
	_ = deviceTokens.Upsert(ctx, &domain.DeviceToken{ID: uuid.New(), OwnerID: owner, FCMToken: "d1", UpdatedAt: time.Now()})
	_ = deviceTokens.Upsert(ctx, &domain.DeviceToken{ID: uuid.New(), OwnerID: owner, FCMToken: "d2", UpdatedAt: time.Now()})
	_ = deviceTokens.Upsert(ctx, &domain.DeviceToken{ID: uuid.New(), OwnerID: otro, FCMToken: "d3", UpdatedAt: time.Now()})

	event := SessionsRevokedEvent{EventID: uuid.New(), OwnerID: owner}
	if err := service.Execute(ctx, event); err != nil {
		t.Fatalf("Execute() devolvio un error inesperado: %v", err)
	}

	restantes, err := deviceTokens.FindByOwnerIDs(ctx, []uuid.UUID{owner, otro})
	if err != nil {
		t.Fatalf("FindByOwnerIDs() devolvio un error inesperado: %v", err)
	}
	if len(restantes) != 1 || restantes[0].FCMToken != "d3" {
		t.Fatalf("solo deberia sobrevivir el token del otro owner, se obtuvo: %+v", restantes)
	}
}

func TestRevokeSessionsService_MismoEventIDEntregadoDosVeces_SoloUnaBaja(t *testing.T) {
	deviceTokens := memory.NewDeviceTokenRepository()
	notificationLogs := memory.NewNotificationLogRepository()
	service := NewRevokeSessionsService(memory.NewUnitOfWork(notificationLogs, deviceTokens))
	ctx := context.Background()

	owner := uuid.New()
	_ = deviceTokens.Upsert(ctx, &domain.DeviceToken{ID: uuid.New(), OwnerID: owner, FCMToken: "d1", UpdatedAt: time.Now()})

	event := SessionsRevokedEvent{EventID: uuid.New(), OwnerID: owner}
	if err := service.Execute(ctx, event); err != nil {
		t.Fatalf("Execute() del primer intento devolvio un error inesperado: %v", err)
	}

	// Un usuario podria registrar un token nuevo entre el primer
	// procesamiento y el redelivery: el redelivery no deberia
	// tocarlo, porque el evento ya fue procesado.
	_ = deviceTokens.Upsert(ctx, &domain.DeviceToken{ID: uuid.New(), OwnerID: owner, FCMToken: "d2", UpdatedAt: time.Now()})

	if err := service.Execute(ctx, event); err != nil {
		t.Fatalf("Execute() del redelivery devolvio un error inesperado: %v", err)
	}

	restantes, err := deviceTokens.FindByOwnerIDs(ctx, []uuid.UUID{owner})
	if err != nil {
		t.Fatalf("FindByOwnerIDs() devolvio un error inesperado: %v", err)
	}
	if len(restantes) != 1 || restantes[0].FCMToken != "d2" {
		t.Fatalf("el redelivery no deberia haber tocado el token registrado despues del primer procesamiento, se obtuvo: %+v", restantes)
	}
}

func TestRevokeSessionsService_PropagaElErrorDelBorrado(t *testing.T) {
	notificationLogs := memory.NewNotificationLogRepository()
	repo := &failingDeviceTokenRepository{err: errors.New("base caida")}
	service := NewRevokeSessionsService(memory.NewUnitOfWork(notificationLogs, repo))

	event := SessionsRevokedEvent{EventID: uuid.New(), OwnerID: uuid.New()}
	if err := service.Execute(context.Background(), event); err == nil {
		t.Fatal("se esperaba que Execute() propagara el error del borrado")
	}
}

func TestRevokeSessionsService_FalloDelBorrado_RevierteElRegistroYElReintentoBorra(t *testing.T) {
	notificationLogs := memory.NewNotificationLogRepository()
	deviceTokens := memory.NewDeviceTokenRepository()
	ctx := context.Background()

	owner := uuid.New()
	_ = deviceTokens.Upsert(ctx, &domain.DeviceToken{ID: uuid.New(), OwnerID: owner, FCMToken: "d1", UpdatedAt: time.Now()})
	event := SessionsRevokedEvent{EventID: uuid.New(), OwnerID: owner}

	// Primer intento: el borrado falla.
	failing := &failingDeviceTokenRepository{DeviceTokenRepository: deviceTokens, err: errors.New("base caida")}
	if err := NewRevokeSessionsService(memory.NewUnitOfWork(notificationLogs, failing)).Execute(ctx, event); err == nil {
		t.Fatal("se esperaba que el primer intento fallara")
	}

	// Rollback: sin registro, el reintento no se descarta como duplicado.
	if _, ok := notificationLogs.Get(event.EventID); ok {
		t.Fatal("tras un fallo del borrado el event_id no deberia estar registrado")
	}

	// Reintento con la base recuperada: ahora si borra.
	if err := NewRevokeSessionsService(memory.NewUnitOfWork(notificationLogs, deviceTokens)).Execute(ctx, event); err != nil {
		t.Fatalf("Execute() del reintento devolvio un error inesperado: %v", err)
	}
	restantes, _ := deviceTokens.FindByOwnerIDs(ctx, []uuid.UUID{owner})
	if len(restantes) != 0 {
		t.Fatalf("el reintento deberia haber borrado los tokens, quedaron: %+v", restantes)
	}
}

func TestRevokeSessionsService_DosEntregasConcurrentes_UnaSolaBaja(t *testing.T) {
	notificationLogs := memory.NewNotificationLogRepository()
	repo := &gatedDeviceTokenRepository{
		DeviceTokenRepository: memory.NewDeviceTokenRepository(),
		started:               make(chan struct{}, 2),
		release:               make(chan struct{}),
	}
	service := NewRevokeSessionsService(memory.NewUnitOfWork(notificationLogs, repo))
	ctx := context.Background()

	event := SessionsRevokedEvent{EventID: uuid.New(), OwnerID: uuid.New()}

	first := make(chan error, 1)
	second := make(chan error, 1)

	go func() { first <- service.Execute(ctx, event) }()
	<-repo.started // la primera esta dentro del borrado, con el event_id reservado

	go func() { second <- service.Execute(ctx, event) }()
	time.Sleep(50 * time.Millisecond) // la segunda queda esperando en la clave unica

	close(repo.release)

	if err := <-first; err != nil {
		t.Fatalf("Execute() de la primera entrega devolvio un error inesperado: %v", err)
	}
	if err := <-second; err != nil {
		t.Fatalf("Execute() de la entrega duplicada devolvio un error inesperado: %v", err)
	}
	if repo.deleteCount() != 1 {
		t.Fatalf("se esperaba una unica baja entre dos entregas concurrentes, hubo %d", repo.deleteCount())
	}
}

// failingDeviceTokenRepository envuelve el repositorio en memoria para
// forzar un error en DeleteByOwnerID sin necesitar una base real.
type failingDeviceTokenRepository struct {
	*memory.DeviceTokenRepository
	err error
}

func (f *failingDeviceTokenRepository) DeleteByOwnerID(context.Context, uuid.UUID) error {
	return f.err
}

// gatedDeviceTokenRepository cuenta los borrados y permite retener
// DeleteByOwnerID hasta que el test lo libere.
type gatedDeviceTokenRepository struct {
	domain.DeviceTokenRepository
	mu      sync.Mutex
	deletes int
	started chan struct{}
	release chan struct{}
}

func (g *gatedDeviceTokenRepository) DeleteByOwnerID(ctx context.Context, ownerID uuid.UUID) error {
	g.mu.Lock()
	g.deletes++
	g.mu.Unlock()

	g.started <- struct{}{}
	<-g.release
	return g.DeviceTokenRepository.DeleteByOwnerID(ctx, ownerID)
}

func (g *gatedDeviceTokenRepository) deleteCount() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.deletes
}
