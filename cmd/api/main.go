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

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/matiasfelau/vinculo-vial-notification/internal/client/fcm"
	deliveryhttp "github.com/matiasfelau/vinculo-vial-notification/internal/delivery/http"
	deliveryworker "github.com/matiasfelau/vinculo-vial-notification/internal/delivery/worker"
	"github.com/matiasfelau/vinculo-vial-notification/internal/repository/postgres"
	"github.com/matiasfelau/vinculo-vial-notification/internal/service/dispatch"
	"github.com/matiasfelau/vinculo-vial-notification/internal/service/sessions"
	"github.com/matiasfelau/vinculo-vial-notification/internal/service/tokens"
	"github.com/matiasfelau/vinculo-vial-notification/pkg/config"
)

const shutdownTimeout = 10 * time.Second

// exitFunc se indirecciona para poder testear runWorker sin terminar
// el proceso de pruebas.
var exitFunc = os.Exit

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("configuracion invalida: %v", err)
	}

	db, err := postgres.Connect(cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("no se pudo conectar a la base de datos: %v", err)
	}

	if err := postgres.AutoMigrate(db); err != nil {
		log.Fatalf("no se pudo migrar el esquema: %v", err)
	}

	deviceTokenRepository := postgres.NewDeviceTokenRepository(db)
	notificationLogRepository := postgres.NewNotificationLogRepository(db)

	tokenHandler := deliveryhttp.NewTokenHandler(
		tokens.NewRegisterTokenService(deviceTokenRepository),
		tokens.NewDeleteTokenService(deviceTokenRepository),
	)

	router := deliveryhttp.NewRouter()
	deliveryhttp.RegisterTokenRoutes(router, tokenHandler)

	server := &http.Server{
		Addr:    ":" + cfg.Port,
		Handler: router,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	messagingClient, err := fcm.NewMessagingClient(context.Background(), cfg.FirebaseCredentialsPath)
	if err != nil {
		log.Fatalf("no se pudo inicializar el cliente de firebase: %v", err)
	}
	fcmDispatcher := fcm.NewDispatcher(messagingClient, cfg.FCMBatchSize)

	dispatchAlertService := dispatch.NewDispatchAlertService(notificationLogRepository, deviceTokenRepository, fcmDispatcher)
	revokeSessionsService := sessions.NewRevokeSessionsService(notificationLogRepository, deviceTokenRepository)

	var workers sync.WaitGroup
	workers.Add(2)

	go runWorker(&workers, "alert.created", func() {
		deliveryworker.RunWithReconnect(ctx, cfg.RabbitMQURL, cfg.AlertQueue, func(channel *amqp.Channel) *deliveryworker.Consumer {
			deadLetters := deliveryworker.NewAMQPDeadLetterPublisher(channel)
			handler := deliveryworker.NewAlertCreatedHandler(dispatchAlertService, deadLetters, int64(cfg.MaxRetries))
			return deliveryworker.NewConsumer(channel, cfg.AlertQueue, handler)
		})
	})

	go runWorker(&workers, "user.sessions.revoked", func() {
		deliveryworker.RunWithReconnect(ctx, cfg.RabbitMQURL, cfg.SessionsRevokedQueue, func(channel *amqp.Channel) *deliveryworker.Consumer {
			deadLetters := deliveryworker.NewAMQPDeadLetterPublisher(channel)
			handler := deliveryworker.NewSessionsRevokedHandler(revokeSessionsService, deadLetters, int64(cfg.MaxRetries))
			return deliveryworker.NewConsumer(channel, cfg.SessionsRevokedQueue, handler)
		})
	})

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
	workers.Wait()
	log.Println("workers detenidos correctamente")
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
