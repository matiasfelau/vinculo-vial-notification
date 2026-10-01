package worker

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestNextBackoff_DuplicaHastaElTope(t *testing.T) {
	backoff := minReconnectBackoff
	for i := 0; i < 10; i++ {
		backoff = nextBackoff(backoff)
	}
	if backoff != maxReconnectBackoff {
		t.Fatalf("backoff = %v, se esperaba que se estabilizara en el tope %v", backoff, maxReconnectBackoff)
	}
}

func TestNextBackoff_NuncaSuperaElTope(t *testing.T) {
	if got := nextBackoff(maxReconnectBackoff); got != maxReconnectBackoff {
		t.Fatalf("nextBackoff(max) = %v, se esperaba que se mantuviera en %v", got, maxReconnectBackoff)
	}
}

func TestSleepOrDone_DevuelveTrueTrasElTiempo(t *testing.T) {
	ctx := context.Background()
	start := time.Now()

	if !sleepOrDone(ctx, 10*time.Millisecond) {
		t.Fatal("se esperaba true al completar la espera")
	}
	if elapsed := time.Since(start); elapsed < 10*time.Millisecond {
		t.Fatalf("la espera termino demasiado rapido: %v", elapsed)
	}
}

func TestSleepOrDone_DevuelveFalseAnteCancelacion(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if sleepOrDone(ctx, time.Minute) {
		t.Fatal("se esperaba false ante un contexto ya cancelado")
	}
}

func TestConfigureChannel_AplicaQosConElPrefetchConfigurado(t *testing.T) {
	channel := &fakeAMQPChannel{}

	if err := configureChannel(channel, 42); err != nil {
		t.Fatalf("configureChannel() devolvio un error inesperado: %v", err)
	}

	if len(channel.qosCalls) != 1 {
		t.Fatalf("se esperaba 1 llamada a Qos, se hicieron %d", len(channel.qosCalls))
	}
	if channel.qosCalls[0].prefetchCount != 42 {
		t.Errorf("prefetchCount = %d, se esperaba %d", channel.qosCalls[0].prefetchCount, 42)
	}
	if channel.qosCalls[0].global {
		t.Error("global deberia ser false: el prefetch es por canal, no por conexion")
	}
	if channel.confirmCalls != 1 {
		t.Errorf("Confirm() deberia haberse invocado exactamente una vez, se invoco %d veces", channel.confirmCalls)
	}
	if channel.notifyReturnCalls != 1 {
		t.Errorf("NotifyReturn() deberia haberse invocado exactamente una vez, se invoco %d veces", channel.notifyReturnCalls)
	}
}

func TestConfigureChannel_PropagaElErrorDeQosSinLlamarAConfirm(t *testing.T) {
	channel := &fakeAMQPChannel{qosErr: errors.New("qos no soportado")}

	if err := configureChannel(channel, 10); err == nil {
		t.Fatal("se esperaba que configureChannel propagara el error de Qos")
	}
	if channel.confirmCalls != 0 {
		t.Errorf("Confirm() no deberia invocarse si Qos fallo, se invoco %d veces", channel.confirmCalls)
	}
}

func TestConfigureChannel_PropagaElErrorDeConfirm(t *testing.T) {
	channel := &fakeAMQPChannel{confirmErr: errors.New("confirm no soportado")}

	if err := configureChannel(channel, 10); err == nil {
		t.Fatal("se esperaba que configureChannel propagara el error de Confirm")
	}
}
