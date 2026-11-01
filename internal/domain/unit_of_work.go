package domain

import "context"

// TxRepositories agrupa los repositorios ligados a una misma
// transaccion. Todo lo que se haga con ellos dentro de
// UnitOfWork.Do se confirma o se revierte en conjunto.
type TxRepositories struct {
	NotificationLogs NotificationLogRepository
	DeviceTokens     DeviceTokenRepository
}

// UnitOfWork ejecuta una operacion dentro de una transaccion. Es la
// pieza que hace efectiva la idempotencia por event_id: el registro
// del evento y su efecto (borrado de tokens, envio de la push) se
// confirman juntos o no se confirma ninguno.
type UnitOfWork interface {
	// Do ejecuta fn dentro de una transaccion. Si fn devuelve un error,
	// la transaccion se revierte y Do devuelve ese mismo error; si
	// devuelve nil, se confirma.
	//
	// El registro de un event_id ya confirmado por otra transaccion
	// devuelve ErrEventAlreadyProcessed. Si otra transaccion todavia
	// abierta registro el mismo event_id, el registro espera a que esa
	// transaccion termine: si confirma, el evento es un duplicado; si
	// revierte, esta transaccion procede.
	Do(ctx context.Context, fn func(tx TxRepositories) error) error
}
