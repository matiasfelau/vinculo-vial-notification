package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	deliveryhttp "github.com/matiasfelau/vinculo-vial-notification/internal/delivery/http"
	"github.com/matiasfelau/vinculo-vial-notification/pkg/config"
)

const shutdownTimeout = 10 * time.Second

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("configuracion invalida: %v", err)
	}

	router := deliveryhttp.NewRouter()
	server := &http.Server{
		Addr:    ":" + cfg.Port,
		Handler: router,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

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
