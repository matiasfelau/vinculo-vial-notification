package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/matiasfelau/vinculo-vial-notification/internal/domain"
)

func TestDeviceTokenRepository_UpsertCreaUnTokenNuevo(t *testing.T) {
	db := openTestDB(t)
	repo := NewDeviceTokenRepository(db)
	ctx := context.Background()

	owner := uuid.New()
	token := &domain.DeviceToken{ID: uuid.New(), OwnerID: owner, FCMToken: "token-1", UpdatedAt: time.Now()}

	if err := repo.Upsert(ctx, token); err != nil {
		t.Fatalf("Upsert() devolvio un error inesperado: %v", err)
	}

	found, err := repo.FindByOwnerIDs(ctx, []uuid.UUID{owner})
	if err != nil {
		t.Fatalf("FindByOwnerIDs() devolvio un error inesperado: %v", err)
	}
	if len(found) != 1 || found[0].FCMToken != "token-1" {
		t.Fatalf("se esperaba un token para el owner, se obtuvo: %+v", found)
	}
}

func TestDeviceTokenRepository_UpsertReasignaElOwnerSinDuplicar(t *testing.T) {
	db := openTestDB(t)
	repo := NewDeviceTokenRepository(db)
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

	foundA, err := repo.FindByOwnerIDs(ctx, []uuid.UUID{ownerA})
	if err != nil {
		t.Fatalf("FindByOwnerIDs(ownerA) devolvio un error inesperado: %v", err)
	}
	if len(foundA) != 0 {
		t.Errorf("el owner original ya no deberia tener el token, se obtuvo: %+v", foundA)
	}

	foundB, err := repo.FindByOwnerIDs(ctx, []uuid.UUID{ownerB})
	if err != nil {
		t.Fatalf("FindByOwnerIDs(ownerB) devolvio un error inesperado: %v", err)
	}
	if len(foundB) != 1 || foundB[0].FCMToken != fcmToken {
		t.Fatalf("el nuevo owner deberia tener exactamente un token reasignado, se obtuvo: %+v", foundB)
	}
	if foundB[0].ID != original.ID {
		t.Errorf("la reasignacion no debe duplicar la fila: ID = %v, se esperaba %v", foundB[0].ID, original.ID)
	}
}

func TestDeviceTokenRepository_DeleteEsIdempotenteSiNoExiste(t *testing.T) {
	db := openTestDB(t)
	repo := NewDeviceTokenRepository(db)
	ctx := context.Background()

	if err := repo.Delete(ctx, uuid.New(), "token-inexistente"); err != nil {
		t.Fatalf("Delete() sobre un token inexistente no deberia fallar: %v", err)
	}
}

func TestDeviceTokenRepository_DeleteNoBorraTokenDeOtroOwner(t *testing.T) {
	db := openTestDB(t)
	repo := NewDeviceTokenRepository(db)
	ctx := context.Background()

	owner := uuid.New()
	otro := uuid.New()
	token := &domain.DeviceToken{ID: uuid.New(), OwnerID: owner, FCMToken: "token-ajeno", UpdatedAt: time.Now()}
	if err := repo.Upsert(ctx, token); err != nil {
		t.Fatalf("Upsert() devolvio un error inesperado: %v", err)
	}

	if err := repo.Delete(ctx, otro, "token-ajeno"); err != nil {
		t.Fatalf("Delete() con owner ajeno no deberia fallar: %v", err)
	}

	found, err := repo.FindByOwnerIDs(ctx, []uuid.UUID{owner})
	if err != nil {
		t.Fatalf("FindByOwnerIDs() devolvio un error inesperado: %v", err)
	}
	if len(found) != 1 {
		t.Fatalf("el token del owner legitimo no deberia haberse borrado, se obtuvo: %+v", found)
	}
}

func TestDeviceTokenRepository_DeleteByTokensBorraEnLote(t *testing.T) {
	db := openTestDB(t)
	repo := NewDeviceTokenRepository(db)
	ctx := context.Background()

	owner := uuid.New()
	for _, tk := range []string{"a", "b", "c"} {
		if err := repo.Upsert(ctx, &domain.DeviceToken{ID: uuid.New(), OwnerID: owner, FCMToken: tk, UpdatedAt: time.Now()}); err != nil {
			t.Fatalf("Upsert(%q) devolvio un error inesperado: %v", tk, err)
		}
	}

	if err := repo.DeleteByTokens(ctx, []string{"a", "c"}); err != nil {
		t.Fatalf("DeleteByTokens() devolvio un error inesperado: %v", err)
	}

	found, err := repo.FindByOwnerIDs(ctx, []uuid.UUID{owner})
	if err != nil {
		t.Fatalf("FindByOwnerIDs() devolvio un error inesperado: %v", err)
	}
	if len(found) != 1 || found[0].FCMToken != "b" {
		t.Fatalf("se esperaba que solo quedara el token 'b', se obtuvo: %+v", found)
	}
}

func TestDeviceTokenRepository_DeleteByOwnerIDBorraTodosSusTokens(t *testing.T) {
	db := openTestDB(t)
	repo := NewDeviceTokenRepository(db)
	ctx := context.Background()

	owner := uuid.New()
	otro := uuid.New()
	if err := repo.Upsert(ctx, &domain.DeviceToken{ID: uuid.New(), OwnerID: owner, FCMToken: "d1", UpdatedAt: time.Now()}); err != nil {
		t.Fatalf("Upsert(d1) devolvio un error inesperado: %v", err)
	}
	if err := repo.Upsert(ctx, &domain.DeviceToken{ID: uuid.New(), OwnerID: owner, FCMToken: "d2", UpdatedAt: time.Now()}); err != nil {
		t.Fatalf("Upsert(d2) devolvio un error inesperado: %v", err)
	}
	if err := repo.Upsert(ctx, &domain.DeviceToken{ID: uuid.New(), OwnerID: otro, FCMToken: "d3", UpdatedAt: time.Now()}); err != nil {
		t.Fatalf("Upsert(d3) devolvio un error inesperado: %v", err)
	}

	if err := repo.DeleteByOwnerID(ctx, owner); err != nil {
		t.Fatalf("DeleteByOwnerID() devolvio un error inesperado: %v", err)
	}

	found, err := repo.FindByOwnerIDs(ctx, []uuid.UUID{owner, otro})
	if err != nil {
		t.Fatalf("FindByOwnerIDs() devolvio un error inesperado: %v", err)
	}
	if len(found) != 1 || found[0].FCMToken != "d3" {
		t.Fatalf("solo deberia sobrevivir el token del otro owner, se obtuvo: %+v", found)
	}
}
