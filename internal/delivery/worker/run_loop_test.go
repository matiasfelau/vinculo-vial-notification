package worker

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// scriptedChannel es un canal AMQP cuyo Consume entrega lo que el test
// decida: un canal de deliveries propio o un error.
type scriptedChannel struct {
	fakeAMQPChannel

	deliveries chan amqp.Delivery
	consumeErr error

	closed atomic.Bool
}

func (c *scriptedChannel) Consume(string, string, bool, bool, bool, bool, amqp.Table) (<-chan amqp.Delivery, error) {
	if c.consumeErr != nil {
		return nil, c.consumeErr
	}
	return c.deliveries, nil
}

func (c *scriptedChannel) Close() error {
	c.closed.Store(true)
	return nil
}

type closeRecorder struct{ closed atomic.Bool }

func (c *closeRecorder) Close() error {
	c.closed.Store(true)
	return nil
}

// recordingHandler registra los cuerpos recibidos y devuelve un
// resultado fijo.
type recordingHandler struct {
	mu      sync.Mutex
	bodies  []string
	retries []int64
	outcome Outcome
}

func (h *recordingHandler) Handle(_ context.Context, body []byte, retries int64) Outcome {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.bodies = append(h.bodies, string(body))
	h.retries = append(h.retries, retries)
	return h.outcome
}

func (h *recordingHandler) received() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.bodies...)
}

func TestConsumerRun_PropagaElErrorDeConsume(t *testing.T) {
	channel := &scriptedChannel{consumeErr: errors.New("cola inexistente")}
	consumer := NewConsumer(channel, "cola.q", &recordingHandler{})

	err := consumer.Run(context.Background())

	if err == nil || err.Error() != "cola inexistente" {
		t.Fatalf("err = %v, se esperaba el error de Consume", err)
	}
}

func TestConsumerRun_ProcesaLosDeliveriesYAckeaSegunElResultado(t *testing.T) {
	deliveries := make(chan amqp.Delivery, 2)
	channel := &scriptedChannel{deliveries: deliveries}
	handler := &recordingHandler{outcome: OutcomeAck}
	consumer := NewConsumer(channel, "cola.q", handler)

	ack := &fakeAcknowledger{}
	deliveries <- amqp.Delivery{Acknowledger: ack, DeliveryTag: 1, Body: []byte("uno")}
	deliveries <- amqp.Delivery{Acknowledger: ack, DeliveryTag: 2, Body: []byte("dos"), Headers: amqp.Table{
		"x-death": []interface{}{amqp.Table{"queue": "cola.q", "count": int64(2)}},
	}}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- consumer.Run(ctx) }()

	waitUntil(t, func() bool { return len(handler.received()) == 2 })
	cancel()

	if err := <-done; err != nil {
		t.Fatalf("Run() devolvio %v tras cancelar el contexto, se esperaba nil", err)
	}
	if got := handler.received(); got[0] != "uno" || got[1] != "dos" {
		t.Errorf("el consumer deberia entregar los mensajes en orden, se obtuvo %v", got)
	}
	if handler.retries[1] != 2 {
		t.Errorf("retries del segundo mensaje = %d, se esperaba 2 (x-death de la cola)", handler.retries[1])
	}
	if len(ack.acked) != 2 {
		t.Errorf("acked = %v, se esperaban los dos mensajes confirmados", ack.acked)
	}
}

func TestConsumerRun_DevuelveErrConnectionLostSiSeCierraElCanalSinCancelar(t *testing.T) {
	deliveries := make(chan amqp.Delivery)
	close(deliveries)
	consumer := NewConsumer(&scriptedChannel{deliveries: deliveries}, "cola.q", &recordingHandler{})

	err := consumer.Run(context.Background())

	if !errors.Is(err, ErrConnectionLost) {
		t.Fatalf("err = %v, se esperaba ErrConnectionLost", err)
	}
}

func TestConsumerRun_NoEsErrorQueSeCierreElCanalCuandoSePidioElApagado(t *testing.T) {
	deliveries := make(chan amqp.Delivery)
	close(deliveries)
	consumer := NewConsumer(&scriptedChannel{deliveries: deliveries}, "cola.q", &recordingHandler{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := consumer.Run(ctx); err != nil {
		t.Fatalf("err = %v, un apagado pedido no es una perdida de conexion", err)
	}
}

// failingAcknowledger simula un canal que se cayo entre el
// procesamiento y la confirmacion.
type failingAcknowledger struct{ err error }

func (f failingAcknowledger) Ack(uint64, bool) error        { return f.err }
func (f failingAcknowledger) Nack(uint64, bool, bool) error { return f.err }
func (f failingAcknowledger) Reject(uint64, bool) error     { return f.err }

func TestConsumerProcess_LoguraSinElCuerpoSiNoPuedeConfirmarORechazar(t *testing.T) {
	cases := []struct {
		name    string
		outcome Outcome
		want    string
	}{
		{"ack", OutcomeAck, "no se pudo confirmar el mensaje"},
		{"nack", OutcomeNackNoRequeue, "no se pudo rechazar el mensaje"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			logs := captureWorkerLogs(t)
			consumer := NewConsumer(nil, "cola.q", &recordingHandler{outcome: tc.outcome})

			consumer.process(context.Background(), amqp.Delivery{
				Acknowledger: failingAcknowledger{err: errors.New("canal cerrado")},
				Body:         []byte("contenido-sensible@example.com"),
			})

			output := logs()
			if !strings.Contains(output, tc.want) || !strings.Contains(output, "canal cerrado") {
				t.Errorf("el log deberia explicar el fallo, se obtuvo %q", output)
			}
			if strings.Contains(output, "contenido-sensible") {
				t.Errorf("el log no debe incluir el cuerpo del mensaje: %q", output)
			}
		})
	}
}

func waitUntil(t *testing.T, condition func() bool) {
	t.Helper()

	deadline := time.Now().Add(3 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("la condicion esperada no se cumplio a tiempo")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// stubBroker reemplaza connectBroker durante el test.
func stubBroker(t *testing.T, connect func(attempt int) (io.Closer, AMQPChannel, error)) *atomic.Int32 {
	t.Helper()

	original := connectBroker
	t.Cleanup(func() { connectBroker = original })

	var attempts atomic.Int32
	connectBroker = func(string, int) (io.Closer, AMQPChannel, error) {
		return connect(int(attempts.Add(1)))
	}
	return &attempts
}

func TestRunWithReconnect_SinBrokerReintentaHastaQueSeCancelaYQuedaDesconectado(t *testing.T) {
	logs := captureWorkerLogs(t)
	ctx, cancel := context.WithCancel(context.Background())
	status := &ConnectionStatus{}
	status.store(true)

	attempts := stubBroker(t, func(int) (io.Closer, AMQPChannel, error) {
		cancel()
		return nil, nil, errors.New("conexion rechazada")
	})

	RunWithReconnect(ctx, "amqp://broker", "cola.q", 5, status, func(AMQPChannel) *Consumer {
		t.Fatal("la fabrica no debe invocarse sin conexion")
		return nil
	})

	if attempts.Load() != 1 {
		t.Errorf("intentos = %d, se esperaba 1 antes de que se cancele la espera", attempts.Load())
	}
	if status.Connected() {
		t.Error("sin broker el estado deberia quedar desconectado")
	}
	if !strings.Contains(logs(), "no se pudo conectar a rabbitmq para cola.q") {
		t.Errorf("el log deberia indicar la cola sin conexion, se obtuvo %q", logs())
	}
}

func TestRunWithReconnect_ConsumeHastaElApagadoYCierraCanalYConexion(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	status := &ConnectionStatus{}
	conn := &closeRecorder{}
	channel := &scriptedChannel{deliveries: make(chan amqp.Delivery)}
	handler := &recordingHandler{outcome: OutcomeAck}
	stubBroker(t, func(int) (io.Closer, AMQPChannel, error) { return conn, channel, nil })

	done := make(chan struct{})
	go func() {
		defer close(done)
		RunWithReconnect(ctx, "amqp://broker", "cola.q", 5, status, func(ch AMQPChannel) *Consumer {
			return NewConsumer(ch, "cola.q", handler)
		})
	}()

	waitUntil(t, status.Connected)
	channel.deliveries <- amqp.Delivery{Acknowledger: &fakeAcknowledger{}, Body: []byte("mensaje")}
	waitUntil(t, func() bool { return len(handler.received()) == 1 })
	cancel()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("RunWithReconnect no devolvio tras cancelar el contexto")
	}
	if status.Connected() {
		t.Error("tras el apagado el estado deberia quedar desconectado")
	}
	if !conn.closed.Load() || !channel.closed.Load() {
		t.Errorf("deberian cerrarse la conexion (%v) y el canal (%v)", conn.closed.Load(), channel.closed.Load())
	}
}

func TestRunWithReconnect_TrasPerderLaConexionEsperaElBackoffYReconecta(t *testing.T) {
	logs := captureWorkerLogs(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	status := &ConnectionStatus{}
	handler := &recordingHandler{outcome: OutcomeAck}
	secondChannel := &scriptedChannel{deliveries: make(chan amqp.Delivery, 1)}
	attempts := stubBroker(t, func(attempt int) (io.Closer, AMQPChannel, error) {
		if attempt == 1 {
			lost := make(chan amqp.Delivery)
			close(lost)
			return &closeRecorder{}, &scriptedChannel{deliveries: lost}, nil
		}
		secondChannel.deliveries <- amqp.Delivery{Acknowledger: &fakeAcknowledger{}, Body: []byte("tras-reconectar")}
		return &closeRecorder{}, secondChannel, nil
	})

	done := make(chan struct{})
	go func() {
		defer close(done)
		RunWithReconnect(ctx, "amqp://broker", "cola.q", 5, status, func(ch AMQPChannel) *Consumer {
			return NewConsumer(ch, "cola.q", handler)
		})
	}()

	waitUntil(t, func() bool { return len(handler.received()) == 1 })
	cancel()
	<-done

	if attempts.Load() != 2 {
		t.Errorf("intentos = %d, se esperaban 2 (la caida y la reconexion)", attempts.Load())
	}
	if got := handler.received(); got[0] != "tras-reconectar" {
		t.Errorf("el consumer deberia retomar tras reconectar, se obtuvo %v", got)
	}
	if !strings.Contains(logs(), "el consumer de cola.q se interrumpio, reconectando") {
		t.Errorf("el log deberia registrar la caida, se obtuvo %q", logs())
	}
}

func TestRunWithReconnect_SeCancelaDuranteElBackoffTrasUnaCaida(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	attempts := stubBroker(t, func(int) (io.Closer, AMQPChannel, error) {
		lost := make(chan amqp.Delivery)
		close(lost)
		// La cancelacion llega mientras el worker espera el backoff.
		time.AfterFunc(50*time.Millisecond, cancel)
		return &closeRecorder{}, &scriptedChannel{deliveries: lost}, nil
	})

	done := make(chan struct{})
	go func() {
		defer close(done)
		RunWithReconnect(ctx, "amqp://broker", "cola.q", 5, nil, func(ch AMQPChannel) *Consumer {
			return NewConsumer(ch, "cola.q", &recordingHandler{})
		})
	}()

	select {
	case <-done:
	case <-time.After(minReconnectBackoff / 2):
		t.Fatal("RunWithReconnect deberia salir al cancelarse el contexto sin esperar el backoff completo")
	}
	if attempts.Load() != 1 {
		t.Errorf("intentos = %d, se esperaba 1: no debe reconectar tras cancelarse", attempts.Load())
	}
}

func TestConnectBroker_DevuelveErrorSiElBrokerNoResponde(t *testing.T) {
	conn, channel, err := connectBroker("amqp://guest:guest@127.0.0.1:1/", 10)

	if err == nil {
		t.Fatal("se esperaba un error al conectar contra un puerto cerrado")
	}
	if conn != nil || channel != nil {
		t.Errorf("ante un error no deben devolverse recursos, conn=%v channel=%v", conn, channel)
	}
}

func TestConnectionStatus_SetConnectedToleraStatusNil(t *testing.T) {
	setConnected(nil, true)
}
