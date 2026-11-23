package postgres

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"

	"github.com/matiasfelau/vinculo-vial-notification/internal/domain"
)

// openClosedTestDB devuelve una base cuya conexion ya se cerro: toda
// consulta posterior falla, como ante una base caida.
func openClosedTestDB(t *testing.T) *gorm.DB {
	t.Helper()

	db := openTestDB(t)
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatalf("no se pudo obtener el *sql.DB: %v", err)
	}
	if err := sqlDB.Close(); err != nil {
		t.Fatalf("no se pudo cerrar la base: %v", err)
	}
	return db
}

func TestRepositorios_ConLaBaseCaidaDevuelvenErrorEnCadaOperacion(t *testing.T) {
	db := openClosedTestDB(t)
	tokens := NewDeviceTokenRepository(db, time.Second)
	logs := NewNotificationLogRepository(db, time.Second)
	ctx := context.Background()
	owner := uuid.New()

	operations := map[string]func() error{
		"Upsert": func() error {
			return tokens.Upsert(ctx, &domain.DeviceToken{ID: uuid.New(), OwnerID: owner, FCMToken: "t", UpdatedAt: time.Now()})
		},
		"Delete":         func() error { return tokens.Delete(ctx, owner, "t") },
		"DeleteByOwner":  func() error { return tokens.DeleteByOwnerID(ctx, owner) },
		"DeleteByTokens": func() error { return tokens.DeleteByTokens(ctx, []string{"t"}) },
		"FindByOwnerIDs": func() error {
			found, err := tokens.FindByOwnerIDs(ctx, []uuid.UUID{owner})
			if found != nil {
				t.Errorf("FindByOwnerIDs devolvio tokens junto con el error: %v", found)
			}
			return err
		},
		"Register": func() error {
			err := logs.Register(ctx, &domain.NotificationLog{ID: uuid.New(), EventID: uuid.New(), Status: domain.StatusSent, CreatedAt: time.Now()})
			if err == domain.ErrEventAlreadyProcessed {
				t.Error("un fallo de la base no debe confundirse con un evento duplicado")
			}
			return err
		},
		"UpdateStatus": func() error { return logs.UpdateStatus(ctx, uuid.New(), domain.StatusSent) },
	}

	for name, operation := range operations {
		t.Run(name, func(t *testing.T) {
			if err := operation(); err == nil {
				t.Fatalf("%s con la base caida deberia devolver un error", name)
			}
		})
	}
}

func TestConnect_RechazaUnDSNInvalido(t *testing.T) {
	_, err := Connect("postgres://usuario:clave@host:puerto-invalido/db", PoolConfig{StatementTimeoutMS: 1000})

	if err == nil || !strings.Contains(err.Error(), "dsn invalido") {
		t.Fatalf("err = %v, se esperaba un error de DSN invalido", err)
	}
}

func TestConnect_DevuelveErrorSiLaBaseNoResponde(t *testing.T) {
	_, err := Connect("postgres://usuario:clave-secreta@127.0.0.1:1/db?sslmode=disable&connect_timeout=1", PoolConfig{StatementTimeoutMS: 1000})

	if err == nil {
		t.Fatal("se esperaba un error al conectar contra un puerto cerrado")
	}
	if strings.Contains(err.Error(), "clave-secreta") {
		t.Errorf("el error no debe incluir la contrasena: %v", err)
	}
}
