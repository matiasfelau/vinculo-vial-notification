package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/matiasfelau/vinculo-vial-notification/internal/client/fcm"
	deliveryhttp "github.com/matiasfelau/vinculo-vial-notification/internal/delivery/http"
	deliveryworker "github.com/matiasfelau/vinculo-vial-notification/internal/delivery/worker"
	"github.com/matiasfelau/vinculo-vial-notification/internal/repository/postgres"
	"github.com/matiasfelau/vinculo-vial-notification/internal/service/dispatch"
	"github.com/matiasfelau/vinculo-vial-notification/internal/service/tokens"
	"github.com/matiasfelau/vinculo-vial-notification/pkg/config"
)

const shutdownTimeout = 10 * time.Second

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
	deliveryhttp.RegisterTokenRoutes(router, tokenHandler, deliveryhttp.NewDailyRateLimiter())

	server := &http.Server{
		Addr:    ":" + cfg.Port,
		Handler: router,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	amqpConn, err := amqp.Dial(cfg.RabbitMQURL)
	if err != nil {
		log.Fatalf("no se pudo conectar a rabbitmq: %v", err)
	}
	defer amqpConn.Close()

	amqpChannel, err := amqpConn.Channel()
	if err != nil {
		log.Fatalf("no se pudo abrir el canal de rabbitmq: %v", err)
	}
	defer amqpChannel.Close()

	messagingClient, err := fcm.NewMessagingClient(context.Background(), cfg.FirebaseCredentialsJSON)
	if err != nil {
		log.Fatalf("no se pudo inicializar el cliente de firebase: %v", err)
	}
	fcmDispatcher := fcm.NewDispatcher(messagingClient, cfg.FCMBatchSize)

	dispatchAlertService := dispatch.NewDispatchAlertService(notificationLogRepository, deviceTokenRepository, fcmDispatcher)
	deadLetterPublisher := deliveryworker.NewAMQPDeadLetterPublisher(amqpChannel)
	alertCreatedHandler := deliveryworker.NewAlertCreatedHandler(dispatchAlertService, deadLetterPublisher)
	alertCreatedConsumer := deliveryworker.NewAlertCreatedConsumer(amqpChannel, cfg.AlertQueue, alertCreatedHandler)

	go func() {
		if err := alertCreatedConsumer.Run(ctx); err != nil {
			log.Fatalf("el worker de alert.created termino con un error: %v", err)
		}
	}()

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
}
