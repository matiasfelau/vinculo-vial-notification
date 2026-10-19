package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/matiasfelau/vinculo-vial-notification/internal/client/fcm"
	deliveryhttp "github.com/matiasfelau/vinculo-vial-notification/internal/delivery/http"
	deliveryworker "github.com/matiasfelau/vinculo-vial-notification/internal/delivery/worker"
	"github.com/matiasfelau/vinculo-vial-notification/internal/repository/postgres"
	"github.com/matiasfelau/vinculo-vial-notification/internal/service/dispatch"
	"github.com/matiasfelau/vinculo-vial-notification/internal/service/sessions"
	"github.com/matiasfelau/vinculo-vial-notification/internal/service/tokens"
	"github.com/matiasfelau/vinculo-vial-notification/pkg/breaker"
	"github.com/matiasfelau/vinculo-vial-notification/pkg/config"
)

const shutdownTimeout = 10 * time.Second

// workerShutdownTimeout acota la espera a que los workers terminen el
// mensaje en vuelo durante el apagado. Es una red de seguridad ante
// un worker que quedo colgado pese a los timeouts propios de sus
// dependencias: sin este limite, un SIGTERM podria no completar
// nunca.
const workerShutdownTimeout = 15 * time.Second

// exitFunc se indirecciona para poder testear runWorker sin terminar
// el proceso de pruebas.
var exitFunc = os.Exit

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("configuracion invalida: %v", err)
	}

	db, err := postgres.Connect(cfg.DatabaseURL, postgres.PoolConfig{
		StatementTimeoutMS: cfg.DBStatementTimeoutMS,
		MaxOpenConns:       cfg.DBMaxOpenConns,
		MaxIdleConns:       cfg.DBMaxIdleConns,
		ConnMaxLifetime:    time.Duration(cfg.DBConnMaxLifetimeMinutes) * time.Minute,
	})
	if err != nil {
		log.Fatalf("no se pudo conectar a la base de datos: %v", err)
	}

	if err := postgres.AutoMigrate(db); err != nil {
		log.Fatalf("no se pudo migrar el esquema: %v", err)
	}

	sqlDB, err := db.DB()
	if err != nil {
		log.Fatalf("no se pudo obtener el *sql.DB subyacente: %v", err)
	}

	queryTimeout := time.Duration(cfg.DBQueryTimeoutSeconds) * time.Second
	deviceTokenRepository := postgres.NewDeviceTokenRepository(db, queryTimeout)
	unitOfWork := postgres.NewUnitOfWork(db, queryTimeout)

	tokenHandler := deliveryhttp.NewTokenHandler(
		tokens.NewRegisterTokenService(deviceTokenRepository),
		tokens.NewDeleteTokenService(deviceTokenRepository),
	)

	alertBrokerStatus := &deliveryworker.ConnectionStatus{}
	sessionsBrokerStatus := &deliveryworker.ConnectionStatus{}
	rejectedBrokerStatus := &deliveryworker.ConnectionStatus{}
	expiredBrokerStatus := &deliveryworker.ConnectionStatus{}
	seenBrokerStatus := &deliveryworker.ConnectionStatus{}
	pendingDeliveredBrokerStatus := &deliveryworker.ConnectionStatus{}

	readinessHandler := deliveryhttp.NewReadinessHandler(sqlDB)
	depsHealthHandler := deliveryhttp.NewDepsHealthHandler(sqlDB, map[string]deliveryhttp.ConnectionChecker{
		"rabbitmq_alert_created":           alertBrokerStatus,
		"rabbitmq_sessions_revoked":        sessionsBrokerStatus,
		"rabbitmq_alert_rejected":          rejectedBrokerStatus,
		"rabbitmq_alert_expired":           expiredBrokerStatus,
		"rabbitmq_alert_seen":              seenBrokerStatus,
		"rabbitmq_alert_pending_delivered": pendingDeliveredBrokerStatus,
	})

	router := deliveryhttp.NewRouter()
	deliveryhttp.RegisterTokenRoutes(router, tokenHandler)
	deliveryhttp.RegisterReadinessRoute(router, readinessHandler)
	deliveryhttp.RegisterDepsHealthRoute(router, depsHealthHandler)

	server := &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      router,
		ReadTimeout:  time.Duration(cfg.HTTPReadTimeoutSeconds) * time.Second,
		WriteTimeout: time.Duration(cfg.HTTPWriteTimeoutSeconds) * time.Second,
		IdleTimeout:  time.Duration(cfg.HTTPIdleTimeoutSeconds) * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	messagingClient, err := fcm.NewMessagingClient(context.Background(), cfg.FirebaseCredentialsPath)
	if err != nil {
		log.Fatalf("no se pudo inicializar el cliente de firebase: %v", err)
	}
	fcmBreaker := breaker.New(breaker.Config{
		FailureThreshold: cfg.FCMBreakerFailureThreshold,
		Window:           time.Duration(cfg.FCMBreakerWindowSeconds) * time.Second,
		OpenDuration:     time.Duration(cfg.FCMBreakerOpenSeconds) * time.Second,
	})
	fcmDispatcher := fcm.NewDispatcher(messagingClient, cfg.FCMBatchSize, time.Duration(cfg.FCMTimeoutSeconds)*time.Second, fcmBreaker)

	dispatchAlertService := dispatch.NewDispatchAlertService(unitOfWork, deviceTokenRepository, fcmDispatcher)
	notifyIssuerService := dispatch.NewNotifyIssuerService(unitOfWork, deviceTokenRepository, fcmDispatcher)
	notifyInboxService := dispatch.NewNotifyInboxService(unitOfWork, deviceTokenRepository, fcmDispatcher)
	revokeSessionsService := sessions.NewRevokeSessionsService(unitOfWork)

	var workers sync.WaitGroup
	workers.Add(6)

	go runWorker(&workers, "alert.created", func() {
		deliveryworker.RunWithReconnect(ctx, cfg.RabbitMQURL, cfg.AlertQueue, cfg.RabbitMQPrefetch, alertBrokerStatus, func(channel deliveryworker.AMQPChannel) *deliveryworker.Consumer {
			deadLetters := deliveryworker.NewAMQPDeadLetterPublisher(channel)
			handler := deliveryworker.NewAlertCreatedHandler(dispatchAlertService, deadLetters, cfg.AlertQueue, int64(cfg.MaxRetries))
			return deliveryworker.NewConsumer(channel, cfg.AlertQueue, handler)
		})
	})

	go runWorker(&workers, "user.sessions.revoked", func() {
		deliveryworker.RunWithReconnect(ctx, cfg.RabbitMQURL, cfg.SessionsRevokedQueue, cfg.RabbitMQPrefetch, sessionsBrokerStatus, func(channel deliveryworker.AMQPChannel) *deliveryworker.Consumer {
			deadLetters := deliveryworker.NewAMQPDeadLetterPublisher(channel)
			handler := deliveryworker.NewSessionsRevokedHandler(revokeSessionsService, deadLetters, cfg.SessionsRevokedQueue, int64(cfg.MaxRetries))
			return deliveryworker.NewConsumer(channel, cfg.SessionsRevokedQueue, handler)
		})
	})

	go runWorker(&workers, "alert.pending.delivered", func() {
		deliveryworker.RunWithReconnect(ctx, cfg.RabbitMQURL, cfg.PendingDeliveredQueue, cfg.RabbitMQPrefetch, pendingDeliveredBrokerStatus, func(channel deliveryworker.AMQPChannel) *deliveryworker.Consumer {
			deadLetters := deliveryworker.NewAMQPDeadLetterPublisher(channel)
			handler := deliveryworker.NewPendingDeliveredHandler(notifyInboxService, deadLetters, cfg.PendingDeliveredQueue, int64(cfg.MaxRetries))
			return deliveryworker.NewConsumer(channel, cfg.PendingDeliveredQueue, handler)
		})
	})

	issuerNoticeWorkers := []struct {
		eventType  string
		queue      string
		status     *deliveryworker.ConnectionStatus
		newHandler func(deadLetters *deliveryworker.AMQPDeadLetterPublisher) *deliveryworker.IssuerNoticeHandler
	}{
		{"alert.rejected", cfg.AlertRejectedQueue, rejectedBrokerStatus, func(dl *deliveryworker.AMQPDeadLetterPublisher) *deliveryworker.IssuerNoticeHandler {
			return deliveryworker.NewAlertRejectedHandler(notifyIssuerService, dl, cfg.AlertRejectedQueue, int64(cfg.MaxRetries))
		}},
		{"alert.expired", cfg.AlertExpiredQueue, expiredBrokerStatus, func(dl *deliveryworker.AMQPDeadLetterPublisher) *deliveryworker.IssuerNoticeHandler {
			return deliveryworker.NewAlertExpiredHandler(notifyIssuerService, dl, cfg.AlertExpiredQueue, int64(cfg.MaxRetries))
		}},
		{"alert.seen", cfg.AlertSeenQueue, seenBrokerStatus, func(dl *deliveryworker.AMQPDeadLetterPublisher) *deliveryworker.IssuerNoticeHandler {
			return deliveryworker.NewAlertSeenHandler(notifyIssuerService, dl, cfg.AlertSeenQueue, int64(cfg.MaxRetries))
		}},
	}
	for _, w := range issuerNoticeWorkers {
		go runWorker(&workers, w.eventType, func() {
			deliveryworker.RunWithReconnect(ctx, cfg.RabbitMQURL, w.queue, cfg.RabbitMQPrefetch, w.status, func(channel deliveryworker.AMQPChannel) *deliveryworker.Consumer {
				return deliveryworker.NewConsumer(channel, w.queue, w.newHandler(deliveryworker.NewAMQPDeadLetterPublisher(channel)))
			})
		})
	}

	serverErrors := make(chan error, 1)
	go func() {
		log.Printf("servidor http escuchando en %s", server.Addr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErrors <- err
			return
		}
		serverErrors <- nil
	}()

	select {
	case err := <-serverErrors:
		if err != nil {
			log.Fatalf("error al iniciar el servidor http: %v", err)
		}
	case <-ctx.Done():
		stop()
		log.Println("senial de apagado recibida, cerrando servidor http")

		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()

		if err := server.Shutdown(shutdownCtx); err != nil {
			log.Fatalf("error durante el apagado del servidor http: %v", err)
		}

		log.Println("servidor http detenido correctamente")
	}

	log.Println("esperando a que los workers terminen el mensaje en vuelo")
	if !waitForWorkers(&workers, workerShutdownTimeout) {
		log.Printf("los workers no terminaron dentro de %s, forzando la salida del proceso", workerShutdownTimeout)
		exitFunc(1)
		return
	}
	log.Println("workers detenidos correctamente")
}

// waitForWorkers espera a que wg termine, acotando la espera a
// timeout. Devuelve false si el plazo se agoto sin que los workers
// terminaran: un contenedor que nunca completa su apagado ante
// SIGTERM se queda colgado en el orquestador en lugar de reiniciarse.
func waitForWorkers(wg *sync.WaitGroup, timeout time.Duration) bool {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return true
	case <-time.After(timeout):
		return false
	}
}

// runWorker ejecuta run y garantiza que un panico dentro del worker
// termine el proceso completo con codigo distinto de cero. Un
// contenedor que sigue respondiendo HTTP pero dejo de consumir la
// cola es el peor estado posible: parece sano y no notifica a nadie.
func runWorker(wg *sync.WaitGroup, name string, run func()) {
	defer wg.Done()
	defer func() {
		if r := recover(); r != nil {
			log.Printf("panico en el worker de %s, terminando el proceso: %v", name, r)
			exitFunc(1)
		}
	}()

	run()
}
