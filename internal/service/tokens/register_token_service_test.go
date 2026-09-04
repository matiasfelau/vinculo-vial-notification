package tokens

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"github.com/matiasfelau/vinculo-vial-notification/internal/repository/memory"
)

func TestRegisterTokenService_RegistraElTokenParaElOwner(t *testing.T) {
	repo := memory.NewDeviceTokenRepository()
	service := NewRegisterTokenService(repo)
	ctx := context.Background()

	owner := uuid.New()
	if err := service.Execute(ctx, owner, "token-1"); err != nil {
		t.Fatalf("Execute() devolvio un error inesperado: %v", err)
	}

	found, err := repo.FindByOwnerIDs(ctx, []uuid.UUID{owner})
	if err != nil {
		t.Fatalf("FindByOwnerIDs() devolvio un error inesperado: %v", err)
	}
	if len(found) != 1 || found[0].FCMToken != "token-1" {
		t.Fatalf("se esperaba un token registrado para el owner, se obtuvo: %+v", found)
	}
}

func TestRegisterTokenService_ReasignaElTokenAOtroOwner(t *testing.T) {
	repo := memory.NewDeviceTokenRepository()
	service := NewRegisterTokenService(repo)
	ctx := context.Background()

	ownerA := uuid.New()
	ownerB := uuid.New()

	if err := service.Execute(ctx, ownerA, "token-compartido"); err != nil {
		t.Fatalf("Execute() inicial devolvio un error inesperado: %v", err)
	}
	if err := service.Execute(ctx, ownerB, "token-compartido"); err != nil {
		t.Fatalf("Execute() de reasignacion devolvio un error inesperado: %v", err)
	}

	foundA, _ := repo.FindByOwnerIDs(ctx, []uuid.UUID{ownerA})
	if len(foundA) != 0 {
		t.Errorf("el owner original ya no deberia tener el token, se obtuvo: %+v", foundA)
	}

	foundB, _ := repo.FindByOwnerIDs(ctx, []uuid.UUID{ownerB})
	if len(foundB) != 1 {
		t.Fatalf("el nuevo owner deberia tener el token reasignado, se obtuvo: %+v", foundB)
	}
}
