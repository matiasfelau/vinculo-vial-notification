package postgres

import (
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/glebarez/sqlite"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

var testDBCounter atomic.Int64

// openTestDB abre una base en memoria, aislada por test, para validar
// el esquema y las consultas sin depender de una instancia real de
// Postgres. El driver es puro Go (sin cgo) para no requerir un
// compilador de C en el entorno de pruebas.
//
// El nombre de la base debe ser unico por llamada: "cache=shared" con
// un DSN identico hace que SQLite reutilice la misma base en memoria
// entre tests distintos del mismo binario, filtrando filas de un test
// a otro.
func openTestDB(t *testing.T) *gorm.DB {
	t.Helper()

	dsn := fmt.Sprintf("file:testdb_%d?mode=memory&cache=shared", testDBCounter.Add(1))
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{
		TranslateError: true,
	})
	if err != nil {
		t.Fatalf("no se pudo abrir la base de prueba: %v", err)
	}

	if err := AutoMigrate(db); err != nil {
		t.Fatalf("AutoMigrate() devolvio un error inesperado: %v", err)
	}

	return db
}

func TestAutoMigrate_CreaElEsquemaSinErrores(t *testing.T) {
	openTestDB(t)
}

func TestAutoMigrate_FCMTokenEsUnico(t *testing.T) {
	db := openTestDB(t)

	ownerA := uuid.New()
	ownerB := uuid.New()
	token := "token-duplicado"

	first := DeviceTokenModel{ID: uuid.New(), OwnerID: ownerA, FCMToken: token}
	if err := db.Create(&first).Error; err != nil {
		t.Fatalf("no se pudo crear el primer token: %v", err)
	}

	second := DeviceTokenModel{ID: uuid.New(), OwnerID: ownerB, FCMToken: token}
	if err := db.Create(&second).Error; err == nil {
		t.Fatal("se esperaba un error por violacion del constraint unico de fcm_token")
	}
}

func TestAutoMigrate_EventIDEsUnico(t *testing.T) {
	db := openTestDB(t)

	eventID := uuid.New()

	first := NotificationLogModel{ID: uuid.New(), EventID: eventID, Status: "SENT"}
	if err := db.Create(&first).Error; err != nil {
		t.Fatalf("no se pudo crear el primer registro: %v", err)
	}

	second := NotificationLogModel{ID: uuid.New(), EventID: eventID, Status: "SENT"}
	if err := db.Create(&second).Error; err == nil {
		t.Fatal("se esperaba un error por violacion del constraint unico de event_id")
	}
}
