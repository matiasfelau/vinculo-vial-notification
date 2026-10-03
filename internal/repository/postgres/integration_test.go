//go:build integration

// Pruebas de integracion contra un Postgres real. Se excluyen del
// "go test ./..." comun: SQLite y el doble en memoria no reproducen la
// semantica del motor que sostiene la idempotencia (el bloqueo de un
// INSERT ... ON CONFLICT DO NOTHING sobre una fila sin confirmar).
//
// Requieren la variable TEST_DATABASE_URL apuntando a una base
// descartable. En CI la provee el servicio postgres del workflow.
//
//	TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/notification_test?sslmode=disable \
//	  go test -tags=integration -count=1 -run Integracion ./internal/repository/postgres/
package postgres

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"gorm.io/gorm"

	"github.com/matiasfelau/vinculo-vial-notification/internal/domain"
	"github.com/matiasfelau/vinculo-vial-notification/internal/service/dispatch"
)

const integrationQueryTimeout = 10 * time.Second

func integrationDSN(t *testing.T) string {
	t.Helper()

	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		// Con el tag integration el llamador pidio explicitamente estas
		// pruebas: saltearlas en silencio ocultaria un CI mal configurado.
		t.Fatal("TEST_DATABASE_URL no esta definida: las pruebas de integracion requieren un Postgres real")
	}
	return dsn
}

// openIntegrationDB conecta con Connect (la misma ruta que usa main) y
// migra el esquema. El statement_timeout es holgado: la entrega que
// espera sobre la clave unica debe poder esperar.
func openIntegrationDB(t *testing.T) *gorm.DB {
	t.Helper()

	db, err := Connect(integrationDSN(t), PoolConfig{
		StatementTimeoutMS: 10000,
		MaxOpenConns:       10,
		MaxIdleConns:       5,
		ConnMaxLifetime:    time.Minute,
	})
	if err != nil {
		t.Fatalf("no se pudo conectar al Postgres de prueba: %v", err)
	}
	if err := AutoMigrate(db); err != nil {
		t.Fatalf("AutoMigrate() devolvio un error inesperado: %v", err)
	}
	return db
}

// gatedDispatcher cuenta los envios y retiene Dispatch hasta que el
// test lo libere, para mantener abierta la transaccion de la primera
// entrega mientras llega la duplicada.
type gatedDispatcher struct {
	mu        sync.Mutex
	calls     int
	failFirst bool
	started   chan struct{}
	release   chan struct{}
}

func (g *gatedDispatcher) Dispatch(_ context.Context, tokens []string, _ string) ([]domain.DispatchOutcome, error) {
	g.mu.Lock()
	g.calls++
	attempt := g.calls
	g.mu.Unlock()

	g.started <- struct{}{}
	<-g.release

	if g.failFirst && attempt == 1 {
		return nil, errors.New("fcm no disponible")
	}

	outcomes := make([]domain.DispatchOutcome, len(tokens))
	for i, token := range tokens {
		outcomes[i] = domain.DispatchOutcome{Token: token, Success: true}
	}
	return outcomes, nil
}

func (g *gatedDispatcher) callCount() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.calls
}

// waitForBlockedInsert espera hasta que el motor reporte un backend
// bloqueado por un lock sobre el INSERT al log de notificaciones: es la
// prueba de que la entrega duplicada esta esperando en la clave unica,
// y no simplemente demorada.
func waitForBlockedInsert(t *testing.T, db *gorm.DB) {
	t.Helper()

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var blocked int64
		err := db.Raw(`SELECT count(*) FROM pg_stat_activity
			WHERE datname = current_database()
			  AND wait_event_type = 'Lock'
			  AND query ILIKE '%INSERT INTO "notification_log"%'`).Scan(&blocked).Error
		if err != nil {
			t.Fatalf("no se pudo consultar pg_stat_activity: %v", err)
		}
		if blocked > 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("la entrega duplicada nunca quedo bloqueada sobre la fila sin confirmar: el motor no esta esperando en la clave unica")
}

type integrationFixture struct {
	db      *gorm.DB
	service *dispatch.DispatchAlertService
	event   dispatch.AlertCreatedEvent
	gate    *gatedDispatcher
}

func newIntegrationFixture(t *testing.T, failFirst bool) integrationFixture {
	t.Helper()

	db := openIntegrationDB(t)
	tokens := NewDeviceTokenRepository(db, integrationQueryTimeout)

	owner := uuid.New()
	fcmToken := "token-" + owner.String()
	if err := tokens.Upsert(context.Background(), &domain.DeviceToken{ID: uuid.New(), OwnerID: owner, FCMToken: fcmToken, UpdatedAt: time.Now()}); err != nil {
		t.Fatalf("Upsert() devolvio un error inesperado: %v", err)
	}

	event := dispatch.AlertCreatedEvent{EventID: uuid.New(), OwnerIDs: []uuid.UUID{owner}, AlertType: "choque"}

	t.Cleanup(func() {
		db.Exec("DELETE FROM notification_log WHERE event_id = ?", event.EventID)
		db.Exec("DELETE FROM device_tokens WHERE owner_id = ?", owner)
	})

	gate := &gatedDispatcher{failFirst: failFirst, started: make(chan struct{}, 2), release: make(chan struct{})}
	service := dispatch.NewDispatchAlertService(NewUnitOfWork(db, integrationQueryTimeout), tokens, gate)

	return integrationFixture{db: db, service: service, event: event, gate: gate}
}

func (f integrationFixture) loggedRows(t *testing.T) (rows int64, status string) {
	t.Helper()

	if err := f.db.Raw("SELECT count(*) FROM notification_log WHERE event_id = ?", f.event.EventID).Scan(&rows).Error; err != nil {
		t.Fatalf("no se pudo contar el log: %v", err)
	}
	f.db.Raw("SELECT status FROM notification_log WHERE event_id = ?", f.event.EventID).Scan(&status)
	return rows, status
}

func TestIntegracion_DosEntregasConcurrentes_UnSoloEnvio(t *testing.T) {
	f := newIntegrationFixture(t, false)
	ctx := context.Background()

	first := make(chan error, 1)
	second := make(chan error, 1)

	go func() { first <- f.service.Execute(ctx, f.event) }()
	<-f.gate.started // la primera tiene su transaccion abierta, con el event_id sin confirmar

	go func() { second <- f.service.Execute(ctx, f.event) }()
	waitForBlockedInsert(t, f.db)

	select {
	case err := <-second:
		t.Fatalf("la entrega duplicada no deberia terminar mientras la primera no confirma (err=%v)", err)
	default:
	}

	close(f.gate.release)

	if err := <-first; err != nil {
		t.Fatalf("la primera entrega devolvio un error inesperado: %v", err)
	}
	if err := <-second; err != nil {
		t.Fatalf("la entrega duplicada deberia descartarse sin error, devolvio: %v", err)
	}

	if got := f.gate.callCount(); got != 1 {
		t.Fatalf("se esperaba un unico envio entre dos entregas concurrentes, hubo %d", got)
	}
	if rows, status := f.loggedRows(t); rows != 1 || status != string(domain.StatusSent) {
		t.Fatalf("se esperaba una fila SENT en el log, se obtuvo rows=%d status=%q", rows, status)
	}
}

func TestIntegracion_PrimeraEntregaFalla_LaDuplicadaEnEsperaTomaElRelevo(t *testing.T) {
	f := newIntegrationFixture(t, true)
	ctx := context.Background()

	first := make(chan error, 1)
	second := make(chan error, 1)

	go func() { first <- f.service.Execute(ctx, f.event) }()
	<-f.gate.started

	go func() { second <- f.service.Execute(ctx, f.event) }()
	waitForBlockedInsert(t, f.db)

	close(f.gate.release)

	if err := <-first; err == nil {
		t.Fatal("se esperaba que la primera entrega fallara (FCM caido)")
	}
	// La primera revirtio: la fila sin confirmar desaparece y el motor
	// destraba a la duplicada, que ahora si debe procesar y enviar.
	if err := <-second; err != nil {
		t.Fatalf("la entrega que esperaba deberia haber tomado el relevo y enviado, devolvio: %v", err)
	}

	if got := f.gate.callCount(); got != 2 {
		t.Fatalf("se esperaban 2 envios (el fallido y el de la entrega que tomo el relevo), hubo %d", got)
	}
	if rows, status := f.loggedRows(t); rows != 1 || status != string(domain.StatusSent) {
		t.Fatalf("se esperaba una fila SENT en el log, se obtuvo rows=%d status=%q", rows, status)
	}
}

func TestIntegracion_FalloDeFCM_NoDejaElEventIDRegistrado(t *testing.T) {
	f := newIntegrationFixture(t, true)
	close(f.gate.release) // sin retencion: el envio falla de inmediato
	ctx := context.Background()

	if err := f.service.Execute(ctx, f.event); err == nil {
		t.Fatal("se esperaba que el primer intento fallara")
	}
	if rows, _ := f.loggedRows(t); rows != 0 {
		t.Fatalf("tras el rollback no deberia quedar fila en el log, hay %d", rows)
	}

	// El reintento (NACK) no se descarta como duplicado: vuelve a enviar.
	if err := f.service.Execute(ctx, f.event); err != nil {
		t.Fatalf("el reintento deberia enviar con exito, devolvio: %v", err)
	}
	if got := f.gate.callCount(); got != 2 {
		t.Fatalf("se esperaban 2 envios, hubo %d", got)
	}
}

// El statement_timeout que Connect agrega al DSN debe llegar al motor, y
// el error que devuelve Postgres al dispararse debe llegar intacto como
// *pgconn.PgError con SQLSTATE 57014: es lo que la capa HTTP clasifica
// como 503.
func TestIntegracion_StatementTimeout_LlegaComoSQLState57014(t *testing.T) {
	db, err := Connect(integrationDSN(t), PoolConfig{StatementTimeoutMS: 200, MaxOpenConns: 2, MaxIdleConns: 1, ConnMaxLifetime: time.Minute})
	if err != nil {
		t.Fatalf("no se pudo conectar al Postgres de prueba: %v", err)
	}

	err = db.WithContext(context.Background()).Exec("SELECT pg_sleep(3)").Error
	if err == nil {
		t.Fatal("se esperaba que statement_timeout cortara la consulta")
	}

	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		t.Fatalf("el error deberia contener un *pgconn.PgError, se obtuvo %T: %v", err, err)
	}
	if pgErr.Code != "57014" {
		t.Fatalf("SQLSTATE = %q, se esperaba 57014 (query_canceled)", pgErr.Code)
	}
}
