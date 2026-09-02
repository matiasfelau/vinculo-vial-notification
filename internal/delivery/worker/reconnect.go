package worker

import (
	"context"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	minReconnectBackoff = 1 * time.Second
	maxReconnectBackoff = 30 * time.Second
)

// ConsumerFactory arma un Consumer sobre un canal AMQP recien
// abierto. Se invoca en cada intento de conexion exitoso: nunca debe
// declarar exchanges, colas ni bindings, solo consumir y publicar
// sobre la topologia que ya declaro vinculo-vial-core.
type ConsumerFactory func(channel *amqp.Channel) *Consumer

// RunWithReconnect mantiene un consumer activo ante caidas del
// broker. Ante un error de conexion o la perdida del canal, espera un
// backoff exponencial acotado y reintenta, sin volver a declarar
// topologia. Devuelve cuando el contexto se cancela, una vez que el
// consumer termino de procesar el mensaje en vuelo.
func RunWithReconnect(ctx context.Context, amqpURL, queue string, factory ConsumerFactory) {
	backoff := minReconnectBackoff

	for ctx.Err() == nil {
		conn, channel, err := dial(amqpURL)
		if err != nil {
			log.Printf("no se pudo conectar a rabbitmq para %s: %v", queue, err)
			if !sleepOrDone(ctx, backoff) {
				return
			}
			backoff = nextBackoff(backoff)
			continue
		}

		backoff = minReconnectBackoff

		consumer := factory(channel)
		runErr := consumer.Run(ctx)

		channel.Close()
		conn.Close()

		if ctx.Err() != nil {
			return
		}

		if runErr != nil {
			log.Printf("el consumer de %s se interrumpio, reconectando: %v", queue, runErr)
		}

		if !sleepOrDone(ctx, backoff) {
			return
		}
		backoff = nextBackoff(backoff)
	}
}

func dial(amqpURL string) (*amqp.Connection, *amqp.Channel, error) {
	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		return nil, nil, err
	}

	channel, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, nil, err
	}

	return conn, channel, nil
}

// sleepOrDone espera d, o hasta que el contexto se cancele. Devuelve
// false si la espera termino por cancelacion.
func sleepOrDone(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func nextBackoff(current time.Duration) time.Duration {
	next := current * 2
	if next > maxReconnectBackoff {
		return maxReconnectBackoff
	}
	return next
}
