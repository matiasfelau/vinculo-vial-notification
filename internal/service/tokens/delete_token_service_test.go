package tokens

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"github.com/matiasfelau/vinculo-vial-notification/internal/repository/memory"
)

func TestDeleteTokenService_EsIdempotenteSiElTokenNoExiste(t *testing.T) {
	repo := memory.NewDeviceTokenRepository()
	service := NewDeleteTokenService(repo)

	if err := service.Execute(context.Background(), uuid.New(), "no-existe"); err != nil {
		t.Fatalf("Execute() sobre un token inexistente no deberia fallar: %v", err)
	}
}

func TestDeleteTokenService_BorraElTokenDelOwner(t *testing.T) {
	repo := memory.NewDeviceTokenRepository()
	registerService := NewRegisterTokenService(repo)
	deleteService := NewDeleteTokenService(repo)
	ctx := context.Background()

	owner := uuid.New()
	if err := registerService.Execute(ctx, owner, "token-1"); err != nil {
		t.Fatalf("Execute() de registro devolvio un error inesperado: %v", err)
	}

	if err := deleteService.Execute(ctx, owner, "token-1"); err != nil {
		t.Fatalf("Execute() de baja devolvio un error inesperado: %v", err)
	}

	found, err := repo.FindByOwnerIDs(ctx, []uuid.UUID{owner})
	if err != nil {
		t.Fatalf("FindByOwnerIDs() devolvio un error inesperado: %v", err)
	}
	if len(found) != 0 {
		t.Fatalf("el token deberia haberse borrado, se obtuvo: %+v", found)
	}
}
