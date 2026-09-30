package http

import (
	"context"
	"errors"
	"net"
	"net/http"
)

// classifyDependencyError aplica la taxonomia de errores acordada entre
// los servicios de la plataforma para el trato de una dependencia
// propia (aca, la base de datos): una dependencia caida, inalcanzable
// o que no responde a tiempo es 503 -- el problema es nuestro, y vale
// la pena que el cliente reintente. Cualquier otro error de la
// dependencia (una consulta invalida, una respuesta que no se esperaba)
// es 502. Ninguno de los dos casos es un 4xx de negocio: un circuit
// breaker que cuente fallas no debe tratarlos como tal.
func classifyDependencyError(err error) int {
	if isDependencyUnavailable(err) {
		return http.StatusServiceUnavailable
	}
	return http.StatusBadGateway
}

// isDependencyUnavailable distingue el caso "no llegamos a tiempo o no
// se pudo conectar" del resto. Cubre tanto los timeouts propios que
// este servicio impone (context.DeadlineExceeded) como los errores de
// red que reporta el driver al no poder alcanzar la base.
func isDependencyUnavailable(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return true
	}

	var netErr net.Error
	return errors.As(err, &netErr)
}
