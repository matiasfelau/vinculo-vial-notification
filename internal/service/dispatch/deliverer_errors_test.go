package dispatch

import (
	"bytes"
	"context"
	"errors"
	"log"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/matiasfelau/vinculo-vial-notification/internal/domain"
	"github.com/matiasfelau/vinculo-vial-notification/internal/repository/memory"
)

// faultyDeviceTokens falla donde el test lo pide y delega el resto en
// el repositorio en memoria.
type faultyDeviceTokens struct {
	*memory.DeviceTokenRepository
	findErr   error
	deleteErr error
}

func (f *faultyDeviceTokens) FindByOwnerIDs(ctx context.Context, ownerIDs []uuid.UUID) ([]domain.DeviceToken, error) {
	if f.findErr != nil {
		return nil, f.findErr
	}
	return f.DeviceTokenRepository.FindByOwnerIDs(ctx, ownerIDs)
}

func (f *faultyDeviceTokens) DeleteByTokens(ctx context.Context, tokens []string) error {
	if f.deleteErr != nil {
		return f.deleteErr
	}
	return f.DeviceTokenRepository.DeleteByTokens(ctx, tokens)
}

func newFaultyService(tokens *faultyDeviceTokens) (*DispatchAlertService, *memory.NotificationLogRepository, *fakeDispatcher) {
	logs := memory.NewNotificationLogRepository()
	dispatcher := &fakeDispatcher{}
	return NewDispatchAlertService(memory.NewUnitOfWork(logs, tokens), tokens, dispatcher), logs, dispatcher
}

func TestDispatchAlertService_SiFallaLaBusquedaDeTokensDevuelveErrorYNoEnvia(t *testing.T) {
	tokens := &faultyDeviceTokens{DeviceTokenRepository: memory.NewDeviceTokenRepository(), findErr: errors.New("base caida")}
	service, _, dispatcher := newFaultyService(tokens)

	err := service.Execute(context.Background(), AlertCreatedEvent{EventID: uuid.New(), OwnerIDs: []uuid.UUID{uuid.New()}, AlertType: "choque"})

	if err == nil || !strings.Contains(err.Error(), "base caida") {
		t.Fatalf("err = %v, se esperaba el error de la base para que el consumer reintente", err)
	}
	if dispatcher.callCount() != 0 {
		t.Fatalf("no deberia haberse despachado nada, llamadas = %d", dispatcher.callCount())
	}
}

func TestDispatchAlertService_SiFallaLaLimpiezaDeTokensInvalidosElEnvioIgualSeConfirma(t *testing.T) {
	var logs bytes.Buffer
	log.SetOutput(&logs)
	defer log.SetOutput(os.Stderr)

	tokens := &faultyDeviceTokens{DeviceTokenRepository: memory.NewDeviceTokenRepository(), deleteErr: errors.New("base caida")}
	service, _, dispatcher := newFaultyService(tokens)
	dispatcher.invalidTokens = map[string]bool{"token-secreto-invalido": true}

	owner := uuid.New()
	_ = tokens.Upsert(context.Background(), &domain.DeviceToken{ID: uuid.New(), OwnerID: owner, FCMToken: "token-secreto-invalido", UpdatedAt: time.Now()})

	err := service.Execute(context.Background(), AlertCreatedEvent{EventID: uuid.New(), OwnerIDs: []uuid.UUID{owner}, AlertType: "choque"})

	if err != nil {
		t.Fatalf("Execute() = %v: la limpieza es best-effort y no debe provocar un reintento que duplique la push", err)
	}
	if dispatcher.callCount() != 1 {
		t.Fatalf("llamadas al dispatcher = %d, se esperaba 1", dispatcher.callCount())
	}
	if !strings.Contains(logs.String(), "no se pudieron eliminar los tokens invalidos") {
		t.Errorf("deberia loguearse el fallo de la limpieza, log = %q", logs.String())
	}
	if strings.Contains(logs.String(), "token-secreto-invalido") {
		t.Errorf("el log no debe incluir tokens: %q", logs.String())
	}
}
