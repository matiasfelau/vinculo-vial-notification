package main

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"

	"github.com/matiasfelau/vinculo-vial-notification/internal/repository/postgres"
	"github.com/matiasfelau/vinculo-vial-notification/pkg/config"
)

var testDBCounter atomic.Int64

func openInMemoryDB() (*gorm.DB, error) {
	dsn := fmt.Sprintf("file:maindb_%d?mode=memory&cache=shared", testDBCounter.Add(1))
	return gorm.Open(sqlite.Open(dsn), &gorm.Config{TranslateError: true})
}

// useInMemoryDB reemplaza la conexion a Postgres por una base SQLite en
// memoria, aislada por test.
func useInMemoryDB(t *testing.T) {
	t.Helper()

	original := connectDB
	t.Cleanup(func() { connectDB = original })

	connectDB = func(string, postgres.PoolConfig) (*gorm.DB, error) {
		return openInMemoryDB()
	}
}

// writeFirebaseCredentials escribe un service account con una clave RSA
// generada al vuelo: alcanza para inicializar el Admin SDK, que no toca
// la red hasta el primer envio.
func writeFirebaseCredentials(t *testing.T) string {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("no se pudo generar la clave de prueba: %v", err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatalf("no se pudo serializar la clave de prueba: %v", err)
	}

	content, err := json.Marshal(map[string]string{
		"type":           "service_account",
		"project_id":     "proyecto-de-prueba",
		"private_key_id": "clave-de-prueba",
		"private_key":    string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})),
		"client_email":   "prueba@proyecto-de-prueba.iam.gserviceaccount.com",
		"client_id":      "1",
		"token_uri":      "https://oauth2.googleapis.com/token",
	})
	if err != nil {
		t.Fatalf("no se pudo armar el JSON de credenciales: %v", err)
	}

	path := filepath.Join(t.TempDir(), "firebase.json")
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatalf("no se pudo escribir las credenciales: %v", err)
	}
	return path
}

func freePort(t *testing.T) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("no se pudo reservar un puerto: %v", err)
	}
	defer listener.Close()

	return fmt.Sprint(listener.Addr().(*net.TCPAddr).Port)
}

// testConfig arma una configuracion valida con el broker inalcanzable:
// los workers reintentan en segundo plano sin que haya un RabbitMQ.
func testConfig(t *testing.T) *config.Config {
	t.Helper()

	for _, name := range []string{"SMTP_HOST", "SMTP_PORT", "SMTP_FROM", "SMTP_TIMEOUT", "APP_ENV"} {
		t.Setenv(name, "")
	}
	t.Setenv("SMTP_HOST", "localhost")

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("config.Load() devolvio un error inesperado: %v", err)
	}

	cfg.Port = freePort(t)
	cfg.RabbitMQURL = "amqp://guest:guest@127.0.0.1:1/"
	cfg.FirebaseCredentialsPath = writeFirebaseCredentials(t)
	return cfg
}

func startRun(ctx context.Context, cfg *config.Config) <-chan error {
	done := make(chan error, 1)
	go func() { done <- run(ctx, cfg) }()
	return done
}

func waitRun(t *testing.T, done <-chan error, timeout time.Duration) error {
	t.Helper()

	select {
	case err := <-done:
		return err
	case <-time.After(timeout):
		t.Fatal("run no termino dentro del plazo")
		return nil
	}
}

func get(url string) (int, string) {
	client := &http.Client{Timeout: 2 * time.Second}
	response, err := client.Get(url)
	if err != nil {
		return 0, err.Error()
	}
	defer response.Body.Close()

	body, _ := io.ReadAll(response.Body)
	return response.StatusCode, string(body)
}

func waitForStatus(t *testing.T, url string, want int) string {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for {
		status, body := get(url)
		if status == want {
			return body
		}
		if time.Now().After(deadline) {
			t.Fatalf("GET %s = %d (%s), se esperaba %d", url, status, body, want)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestRun_LevantaLaAPIYSeApagaOrdenadamenteAlCancelarElContexto(t *testing.T) {
	useInMemoryDB(t)
	cfg := testConfig(t)
	base := "http://127.0.0.1:" + cfg.Port

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := startRun(ctx, cfg)

	if body := waitForStatus(t, base+"/ready", http.StatusOK); !strings.Contains(body, `"database":"up"`) {
		t.Errorf("/ready deberia informar la base arriba, cuerpo = %s", body)
	}

	// Sin broker, los siete consumidores figuran caidos: la instancia
	// queda lista para el balanceo pero degradada.
	body := waitForStatus(t, base+"/health/deps", http.StatusServiceUnavailable)
	for _, name := range []string{
		"rabbitmq_alert_created", "rabbitmq_sessions_revoked", "rabbitmq_alert_rejected",
		"rabbitmq_alert_expired", "rabbitmq_alert_seen", "rabbitmq_alert_pending_delivered",
		"rabbitmq_email_requested",
	} {
		if !strings.Contains(body, `"`+name+`":"down"`) {
			t.Errorf("/health/deps deberia informar %s caido, cuerpo = %s", name, body)
		}
	}

	// La ruta de tokens esta registrada: un POST sin el usuario
	// autenticado se rechaza en el middleware en lugar de dar 404.
	response, err := http.Post(base+"/api/v1/notifications/tokens", "application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatalf("no se pudo invocar la ruta de tokens: %v", err)
	}
	response.Body.Close()
	if response.StatusCode == http.StatusNotFound {
		t.Error("la ruta de tokens no esta registrada en el router")
	}

	cancel()
	if err := waitRun(t, done, 10*time.Second); err != nil {
		t.Fatalf("run() devolvio un error tras un apagado ordenado: %v", err)
	}

	if status, _ := get(base + "/ready"); status != 0 {
		t.Errorf("el servidor HTTP deberia estar cerrado tras el apagado, GET /ready = %d", status)
	}
}

func TestRun_DevuelveErrorSiNoPuedeConectarseALaBase(t *testing.T) {
	original := connectDB
	defer func() { connectDB = original }()
	connectDB = func(string, postgres.PoolConfig) (*gorm.DB, error) {
		return nil, errors.New("base inalcanzable")
	}

	err := run(context.Background(), testConfig(t))

	if err == nil || !strings.Contains(err.Error(), "no se pudo conectar a la base de datos") {
		t.Fatalf("err = %v, se esperaba el error de conexion a la base", err)
	}
	if !strings.Contains(err.Error(), "base inalcanzable") {
		t.Errorf("err = %v, deberia envolver la causa", err)
	}
}

func TestRun_DevuelveErrorSiLaMigracionFalla(t *testing.T) {
	original := connectDB
	defer func() { connectDB = original }()
	connectDB = func(string, postgres.PoolConfig) (*gorm.DB, error) {
		db, err := openInMemoryDB()
		if err != nil {
			return nil, err
		}
		sqlDB, err := db.DB()
		if err != nil {
			return nil, err
		}
		// Una conexion cerrada hace fallar el AutoMigrate.
		return db, sqlDB.Close()
	}

	err := run(context.Background(), testConfig(t))

	if err == nil || !strings.Contains(err.Error(), "no se pudo migrar el esquema") {
		t.Fatalf("err = %v, se esperaba el error de migracion", err)
	}
}

// notSQLPool oculta el *sql.DB subyacente: db.DB() no puede devolverlo.
type notSQLPool struct{ gorm.ConnPool }

func TestRun_DevuelveErrorSiNoPuedeObtenerElSQLDBSubyacente(t *testing.T) {
	original := connectDB
	defer func() { connectDB = original }()
	connectDB = func(string, postgres.PoolConfig) (*gorm.DB, error) {
		db, err := openInMemoryDB()
		if err != nil {
			return nil, err
		}
		// El AutoMigrate pasa (el pool sigue siendo un ConnPool valido), pero
		// db.DB() ya no encuentra el *sql.DB. GORM prioriza Statement.ConnPool sobre
		// ConnPool, asi que hay que reemplazar los dos.
		pool := notSQLPool{db.ConnPool}
		db.ConnPool = pool
		db.Statement.ConnPool = pool
		return db, nil
	}
	// Con tope de tiempo: si run no fallara, arrancaria el servidor y la prueba se colgaria.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	err := run(ctx, testConfig(t))

	if err == nil || !strings.Contains(err.Error(), "no se pudo obtener el *sql.DB subyacente") {
		t.Fatalf("err = %v, se esperaba el error del *sql.DB subyacente", err)
	}
}

func TestExitCode_EsCeroSiElServicioSeApagaEnOrden(t *testing.T) {
	if codigo := exitCode(func() error { return nil }); codigo != 0 {
		t.Errorf("codigo de salida = %d, se esperaba 0", codigo)
	}
}

func TestExitCode_EsUnoYLoguaElErrorSiElServicioFalla(t *testing.T) {
	var logs strings.Builder
	log.SetOutput(&logs)
	t.Cleanup(func() { log.SetOutput(os.Stderr) })

	codigo := exitCode(func() error { return errors.New("fallo el arranque de prueba") })

	if codigo != 1 {
		t.Errorf("codigo de salida = %d, se esperaba 1", codigo)
	}
	if !strings.Contains(logs.String(), "fallo el arranque de prueba") {
		t.Errorf("no se logueo el error de arranque: %q", logs.String())
	}
}

func TestExecute_FallaSiLaConfiguracionEsInvalida(t *testing.T) {
	// Un valor numerico que no lo es: config.Load rechaza la configuracion antes de
	// que execute llegue a arrancar nada (ni siquiera intenta conectarse a la base).
	t.Setenv("MAX_RETRIES", "no-es-un-numero")

	err := execute()

	if err == nil || !strings.Contains(err.Error(), "configuracion invalida") {
		t.Fatalf("err = %v, se esperaba el error de configuracion invalida", err)
	}
	if !strings.Contains(err.Error(), "MAX_RETRIES") {
		t.Errorf("err = %v, deberia nombrar la variable invalida", err)
	}
}

func TestExecute_ConConfiguracionValidaDevuelveElErrorDeArranque(t *testing.T) {
	// Sin tocar la red: la base falla al conectar, asi que el error tiene que venir
	// de run, no de la configuracion.
	original := connectDB
	defer func() { connectDB = original }()
	connectDB = func(string, postgres.PoolConfig) (*gorm.DB, error) {
		return nil, errors.New("base inalcanzable")
	}
	testConfig(t) // deja SMTP_HOST y el resto del entorno listos para config.Load

	err := execute()

	if err == nil || !strings.Contains(err.Error(), "no se pudo conectar a la base de datos") {
		t.Fatalf("err = %v, se esperaba el error de conexion a la base", err)
	}
}

func TestRun_DevuelveErrorSiLasCredencialesDeFirebaseNoExisten(t *testing.T) {
	useInMemoryDB(t)
	cfg := testConfig(t)
	cfg.FirebaseCredentialsPath = filepath.Join(t.TempDir(), "no-existe.json")

	err := run(context.Background(), cfg)

	if err == nil || !strings.Contains(err.Error(), "no se pudo inicializar el cliente de firebase") {
		t.Fatalf("err = %v, se esperaba el error del cliente de firebase", err)
	}
}

func TestRun_DevuelveErrorSiLaConfiguracionSMTPEsInvalida(t *testing.T) {
	useInMemoryDB(t)
	cfg := testConfig(t)
	cfg.SMTP.From = "esto no es un remitente"

	err := run(context.Background(), cfg)

	if err == nil || !strings.Contains(err.Error(), "no se pudo inicializar el cliente smtp") {
		t.Fatalf("err = %v, se esperaba el error del cliente smtp", err)
	}
}

func TestRun_DevuelveErrorSiElPuertoHTTPEstaOcupado(t *testing.T) {
	useInMemoryDB(t)
	cfg := testConfig(t)

	occupied, err := net.Listen("tcp", ":"+cfg.Port)
	if err != nil {
		t.Fatalf("no se pudo ocupar el puerto: %v", err)
	}
	defer occupied.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runErr := waitRun(t, startRun(ctx, cfg), 10*time.Second)

	if runErr == nil || !strings.Contains(runErr.Error(), "error al iniciar el servidor http") {
		t.Fatalf("err = %v, se esperaba el error del servidor http", runErr)
	}
}

func TestRun_ForzaLaSalidaSiUnWorkerNoTerminaDentroDelPlazo(t *testing.T) {
	useInMemoryDB(t)
	cfg := testConfig(t)

	// Un broker que acepta la conexion y nunca responde deja al worker
	// colgado en el handshake AMQP, sin atender la cancelacion.
	hung, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("no se pudo levantar el broker colgado: %v", err)
	}
	defer hung.Close()
	var held []net.Conn
	accepted := make(chan struct{})
	go func() {
		defer close(accepted)
		for {
			conn, err := hung.Accept()
			if err != nil {
				return
			}
			held = append(held, conn)
		}
	}()
	defer func() {
		hung.Close()
		<-accepted
		for _, conn := range held {
			conn.Close()
		}
	}()
	cfg.RabbitMQURL = "amqp://guest:guest@" + hung.Addr().String() + "/"

	originalTimeout := workerShutdownTimeout
	originalExit := exitFunc
	defer func() { workerShutdownTimeout = originalTimeout; exitFunc = originalExit }()
	workerShutdownTimeout = 100 * time.Millisecond
	exitCodes := make(chan int, 1)
	exitFunc = func(code int) { exitCodes <- code }

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := startRun(ctx, cfg)

	waitForStatus(t, "http://127.0.0.1:"+cfg.Port+"/ready", http.StatusOK)
	cancel()

	if err := waitRun(t, done, 10*time.Second); err != nil {
		t.Fatalf("run() devolvio un error inesperado: %v", err)
	}
	select {
	case code := <-exitCodes:
		if code != 1 {
			t.Errorf("codigo de salida = %d, se esperaba 1", code)
		}
	default:
		t.Fatal("se esperaba que run forzara la salida del proceso con exitFunc")
	}
}
