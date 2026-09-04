package memory

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/matiasfelau/vinculo-vial-notification/internal/domain"
)

func TestDeviceTokenRepository_UpsertReasignaElOwnerSinDuplicar(t *testing.T) {
	repo := NewDeviceTokenRepository()
	ctx := context.Background()

	ownerA := uuid.New()
	ownerB := uuid.New()
	fcmToken := "token-compartido"

	original := &domain.DeviceToken{ID: uuid.New(), OwnerID: ownerA, FCMToken: fcmToken, UpdatedAt: time.Now()}
	if err := repo.Upsert(ctx, original); err != nil {
		t.Fatalf("Upsert() inicial devolvio un error inesperado: %v", err)
	}

	reassigned := &domain.DeviceToken{ID: uuid.New(), OwnerID: ownerB, FCMToken: fcmToken, UpdatedAt: time.Now()}
	if err := repo.Upsert(ctx, reassigned); err != nil {
		t.Fatalf("Upsert() de reasignacion devolvio un error inesperado: %v", err)
	}

	foundA, _ := repo.FindByOwnerIDs(ctx, []uuid.UUID{ownerA})
	if len(foundA) != 0 {
		t.Errorf("el owner original ya no deberia tener el token, se obtuvo: %+v", foundA)
	}

	foundB, _ := repo.FindByOwnerIDs(ctx, []uuid.UUID{ownerB})
	if len(foundB) != 1 || foundB[0].ID != original.ID {
		t.Fatalf("se esperaba la fila original reasignada, se obtuvo: %+v", foundB)
	}
}

func TestDeviceTokenRepository_DeleteEsIdempotenteYConAlcanceDeOwner(t *testing.T) {
	repo := NewDeviceTokenRepository()
	ctx := context.Background()

	owner := uuid.New()
	otro := uuid.New()

	if err := repo.Delete(ctx, owner, "no-existe"); err != nil {
		t.Fatalf("Delete() sobre un token inexistente no deberia fallar: %v", err)
	}

	token := &domain.DeviceToken{ID: uuid.New(), OwnerID: owner, FCMToken: "token-x", UpdatedAt: time.Now()}
	if err := repo.Upsert(ctx, token); err != nil {
		t.Fatalf("Upsert() devolvio un error inesperado: %v", err)
	}

	if err := repo.Delete(ctx, otro, "token-x"); err != nil {
		t.Fatalf("Delete() con owner ajeno no deberia fallar: %v", err)
	}
	found, _ := repo.FindByOwnerIDs(ctx, []uuid.UUID{owner})
	if len(found) != 1 {
		t.Fatalf("el token no deberia haberse borrado con un owner ajeno, se obtuvo: %+v", found)
	}

	if err := repo.Delete(ctx, owner, "token-x"); err != nil {
		t.Fatalf("Delete() con el owner correcto devolvio un error inesperado: %v", err)
	}
	found, _ = repo.FindByOwnerIDs(ctx, []uuid.UUID{owner})
	if len(found) != 0 {
		t.Fatalf("el token deberia haberse borrado con el owner correcto, se obtuvo: %+v", found)
	}
}

func TestDeviceTokenRepository_DeleteByOwnerIDBorraTodosSusTokens(t *testing.T) {
	repo := NewDeviceTokenRepository()
	ctx := context.Background()

	owner := uuid.New()
	otro := uuid.New()
	_ = repo.Upsert(ctx, &domain.DeviceToken{ID: uuid.New(), OwnerID: owner, FCMToken: "d1", UpdatedAt: time.Now()})
	_ = repo.Upsert(ctx, &domain.DeviceToken{ID: uuid.New(), OwnerID: owner, FCMToken: "d2", UpdatedAt: time.Now()})
	_ = repo.Upsert(ctx, &domain.DeviceToken{ID: uuid.New(), OwnerID: otro, FCMToken: "d3", UpdatedAt: time.Now()})

	if err := repo.DeleteByOwnerID(ctx, owner); err != nil {
		t.Fatalf("DeleteByOwnerID() devolvio un error inesperado: %v", err)
	}

	found, _ := repo.FindByOwnerIDs(ctx, []uuid.UUID{owner, otro})
	if len(found) != 1 || found[0].FCMToken != "d3" {
		t.Fatalf("solo deberia sobrevivir el token del otro owner, se obtuvo: %+v", found)
	}
}
