package worker

import "testing"

func TestConnectionStatus_ValorCeroReportaNoConectado(t *testing.T) {
	var status ConnectionStatus

	if status.Connected() {
		t.Fatal("el valor cero de ConnectionStatus deberia reportar no conectado")
	}
}

func TestConnectionStatus_StoreActualizaElEstado(t *testing.T) {
	var status ConnectionStatus

	status.store(true)
	if !status.Connected() {
		t.Fatal("se esperaba que Connected() reflejara el store(true)")
	}

	status.store(false)
	if status.Connected() {
		t.Fatal("se esperaba que Connected() reflejara el store(false)")
	}
}

func TestSetConnected_ToleraUnStatusNil(t *testing.T) {
	// No debe entrar en panico: RunWithReconnect puede invocarse sin
	// que el llamador necesite observar el estado de conexion.
	setConnected(nil, true)
}
