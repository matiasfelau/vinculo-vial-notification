package main

import (
	"sync"
	"testing"
	"time"
)

func TestRunWorker_PanicoTerminaElProcesoConCodigoDistintoDeCero(t *testing.T) {
	original := exitFunc
	defer func() { exitFunc = original }()

	var exitCode int
	var called bool
	exitFunc = func(code int) {
		called = true
		exitCode = code
	}

	var wg sync.WaitGroup
	wg.Add(1)

	runWorker(&wg, "test", func() {
		panic("fallo simulado del worker")
	})

	if !called {
		t.Fatal("se esperaba que un panico invocara exitFunc")
	}
	if exitCode == 0 {
		t.Fatalf("exitCode = %d, se esperaba un codigo distinto de cero", exitCode)
	}
}

func TestRunWorker_SinPanicoNoInvocaExit(t *testing.T) {
	original := exitFunc
	defer func() { exitFunc = original }()

	called := false
	exitFunc = func(int) { called = true }

	var wg sync.WaitGroup
	wg.Add(1)

	ran := false
	runWorker(&wg, "test", func() { ran = true })

	if !ran {
		t.Fatal("se esperaba que run() se ejecutara")
	}
	if called {
		t.Fatal("no deberia invocarse exitFunc sin panico")
	}
}

func TestRunWorker_SiempreLlamaWgDone(t *testing.T) {
	original := exitFunc
	defer func() { exitFunc = original }()
	exitFunc = func(int) {}

	var wg sync.WaitGroup
	wg.Add(1)

	runWorker(&wg, "test", func() { panic("boom") })

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("wg.Done() deberia haberse llamado incluso tras un panico")
	}
}
