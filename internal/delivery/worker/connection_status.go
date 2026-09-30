package worker

import "sync/atomic"

// ConnectionStatus expone, de forma segura para uso concurrente, si un
// consumer esta conectado al broker en este momento. RunWithReconnect
// la actualiza en cada transicion de conexion; el endpoint /ready la
// consulta para decidir si esta instancia debe salir del balanceo.
//
// El valor cero (sin store previo) reporta "no conectado": antes del
// primer intento de conexion, la instancia no deberia figurar como
// lista.
type ConnectionStatus struct {
	connected atomic.Bool
}

// Connected satisface la interfaz que consume el handler de
// readiness.
func (s *ConnectionStatus) Connected() bool {
	return s.connected.Load()
}

// store actualiza el estado. Es un metodo interno del paquete: solo
// RunWithReconnect debe escribir este estado, ya que es quien conoce
// el ciclo de vida real de la conexion.
func (s *ConnectionStatus) store(connected bool) {
	s.connected.Store(connected)
}

// setConnected es una funcion libre en lugar de un metodo directo
// sobre *ConnectionStatus para tolerar un status nil sin que cada
// llamador tenga que chequearlo: en las pruebas de RunWithReconnect
// que no necesitan observar el estado de conexion, pasar nil es
// valido.
func setConnected(status *ConnectionStatus, connected bool) {
	if status == nil {
		return
	}
	status.store(connected)
}
