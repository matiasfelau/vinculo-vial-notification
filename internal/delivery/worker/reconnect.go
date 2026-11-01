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
type ConsumerFactory func(channel AMQPChannel) *Consumer

// RunWithReconnect mantiene un consumer activo ante caidas del
// broker. Ante un error de conexion o la perdida del canal, espera un
// backoff exponencial acotado y reintenta, sin volver a declarar
// topologia. Devuelve cuando el contexto se cancela, una vez que el
// consumer termino de procesar el mensaje en vuelo.
//
// prefetch limita, via basic.qos, cuantos mensajes sin ACK puede
// tener en vuelo el canal a la vez. Sin este limite el broker empuja
// toda la cola pendiente al proceso: con ACK manual, eso implica
// memoria sin techo y, ante un crash, devuelve a la cola todo ese
// lote en vuelo de una vez.
//
// status, si no es nil, se actualiza en cada transicion de conexion:
// el endpoint /ready lo consulta para decidir si esta instancia debe
// salir del balanceo. Puede pasarse nil cuando el llamador no
// necesita observar el estado (por ejemplo, en tests).
func RunWithReconnect(ctx context.Context, amqpURL, queue string, prefetch int, status *ConnectionStatus, factory ConsumerFactory) {
	backoff := minReconnectBackoff
	defer setConnected(status, false)

	for ctx.Err() == nil {
		conn, channel, err := dial(amqpURL, prefetch)
		if err != nil {
			setConnected(status, false)
			log.Printf("no se pudo conectar a rabbitmq para %s: %v", queue, err)
			if !sleepOrDone(ctx, backoff) {
				return
			}
			backoff = nextBackoff(backoff)
			continue
		}

		setConnected(status, true)
		backoff = minReconnectBackoff

		consumer := factory(channel)
		runErr := consumer.Run(ctx)

		setConnected(status, false)
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

func dial(amqpURL string, prefetch int) (*amqp.Connection, AMQPChannel, error) {
	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		return nil, nil, err
	}

	rawChannel, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, nil, err
	}

	channel := newRealAMQPChannel(rawChannel)

	if err := configureChannel(channel, prefetch); err != nil {
		channel.Close()
		conn.Close()
		return nil, nil, err
	}

	return conn, channel, nil
}

// configureChannel aplica sobre channel la configuracion que todo
// consumer o publisher de este servicio necesita: prefetch acotado
// (basic.qos), modo confirm (requerido para que
// AMQPDeadLetterPublisher pueda esperar la confirmacion del broker
// antes de que el handler ACKee el mensaje original) y el listener de
// devoluciones. Separado de dial para poder probarlo con un doble del
// canal, sin depender de un broker real.
func configureChannel(channel AMQPChannel, prefetch int) error {
	if err := channel.Qos(prefetch, 0, false); err != nil {
		return err
	}

	if err := channel.Confirm(false); err != nil {
		return err
	}

	go logUnroutableReturns(channel.NotifyReturn(make(chan amqp.Return, 1)))

	return nil
}

// logUnroutableReturns consume las devoluciones de mensajes
// mandatory=true que el broker no pudo rutear (por ejemplo, la DLQ
// sin una cola bindeada: un problema de topologia que este servicio
// no declara y no puede resolver). Nunca loguea el cuerpo del
// mensaje, solo el motivo y el destino de la publicacion. Termina
// sola cuando el canal AMQP se cierra.
func logUnroutableReturns(returns <-chan amqp.Return) {
	for ret := range returns {
		log.Printf("aviso: mensaje no ruteable devuelto por el broker: exchange=%s routing_key=%s code=%d reason=%q", ret.Exchange, ret.RoutingKey, ret.ReplyCode, ret.ReplyText)
	}
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
