package http

import (
	"context"
	"errors"
	"net"
	"net/http"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
)

// postgresAvailabilityPrefixes son los prefijos de SQLSTATE de
// Postgres que indican un problema de disponibilidad del motor -- no
// una consulta invalida ni un defecto propio -- segun la seccion
// Postgres del contrato de resiliencia (identico en los 6 repos):
//
//	08  Connection Exception : no se pudo establecer o se perdio la conexion.
//	53  Insufficient Resources: sin memoria, disco lleno, limite de conexiones.
//	57P Operator Intervention (subclase P): admin_shutdown, crash_shutdown,
//	    cannot_connect_now, database_dropped, idle_session_timeout.
//
// 57014 (query_canceled, el codigo que Postgres devuelve cuando
// statement_timeout corta una consulta en curso) se chequea aparte,
// como codigo exacto: es el unico miembro de la clase 57 que cuenta
// -- el generico 57000 (operator_intervention) queda deliberadamente
// afuera, igual que el resto de la clase.
var postgresAvailabilityPrefixes = []string{"08", "53", "57P"}

const postgresQueryCanceled = "57014"

// classifyDependencyError aplica la taxonomia de errores acordada
// entre los servicios de la plataforma para el trato de una
// dependencia propia (aca, la base de datos):
//
//	503  la base esta caida, inalcanzable, sin recursos, o no respondio
//	     a tiempo (incluido el query_canceled que deja statement_timeout).
//	     El problema es nuestro y vale la pena que el cliente reintente.
//	500  cualquier otro error: una consulta invalida, una violacion de
//	     constraint no esperada, un error de driver. Es un defecto
//	     propio, no una falla transitoria de disponibilidad: reintentar
//	     no lo arregla, y tratarlo como reintentable (502) convertiria
//	     un bug deterministico en un loop de reintentos del cliente.
//
// Ninguno de los dos casos es un 4xx de negocio: un circuit breaker
// que cuente fallas no debe tratarlos como tal.
func classifyDependencyError(err error) int {
	if isDependencyUnavailable(err) {
		return http.StatusServiceUnavailable
	}
	return http.StatusInternalServerError
}

// isDependencyUnavailable distingue el caso "no llegamos a tiempo, no
// se pudo conectar, o el motor no tiene recursos" del resto. Cubre:
//   - los timeouts propios que este servicio impone (context.DeadlineExceeded,
//     via DB_QUERY_TIMEOUT_SECONDS);
//   - los errores de red que reporta el driver al no poder alcanzar la base;
//   - el SQLSTATE que Postgres devuelve cuando statement_timeout corta
//     una consulta en curso (57014) y el resto de los SQLSTATE de
//     disponibilidad del motor (ver postgresAvailabilityPrefixes).
func isDependencyUnavailable(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return true
	}

	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return isPostgresAvailabilityError(pgErr.Code)
	}

	return false
}

// isPostgresAvailabilityError indica si sqlState es un SQLSTATE de
// disponibilidad de Postgres segun la seccion Postgres del contrato
// de resiliencia: 08xxx, 53xxx, 57014 o 57P0x.
func isPostgresAvailabilityError(sqlState string) bool {
	if sqlState == postgresQueryCanceled {
		return true
	}
	for _, prefix := range postgresAvailabilityPrefixes {
		if strings.HasPrefix(sqlState, prefix) {
			return true
		}
	}
	return false
}
