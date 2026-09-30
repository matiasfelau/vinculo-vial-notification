package postgres

import (
	"context"
	"time"
)

// withQueryTimeout deriva, a partir de ctx, un contexto acotado a
// timeout para una unica consulta. Es la contraparte, del lado de Go,
// de statement_timeout: cubre el caso en que el tiempo se va antes de
// llegar a ejecutar la consulta (por ejemplo, esperando una conexion
// libre del pool), que el statement_timeout del motor no llega a
// cubrir porque todavia no hay una consulta en curso.
//
// ctx puede no traer deadline propio: la request HTTP y el ctx de
// larga vida de los workers no lo tienen. Sin este limite, una
// consulta podria quedar esperando indefinidamente por una conexion
// del pool, con el mismo riesgo que ya se corrigio para el despacho a
// FCM (S1-FCM).
func withQueryTimeout(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, timeout)
}
