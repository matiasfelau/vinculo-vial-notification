# vinculo-vial-notification

Servicio de notificaciones push de la plataforma Vinculo Vial. Expone una
API HTTP para el registro y baja de tokens de dispositivo (FCM) y ejecuta,
en el mismo proceso, un worker que consume eventos de RabbitMQ para
despachar notificaciones push a traves de Firebase Cloud Messaging.

## Responsabilidades

- Alta y baja de tokens FCM por usuario (`/api/v1/notifications/tokens`).
- Consumo del evento `alert.created` y despacho de la push correspondiente
  a los destinatarios de una alerta (con `alert_id` en `data` para el deep
  link, si el evento lo trae) y, ademas, un aviso al emisor.
- Consumo de `alert.rejected`, `alert.expired` y `alert.seen` y aviso push
  al emisor de la alerta, que viaja en el `actor` del envelope. Los textos
  son genericos: nunca incluyen el motivo de un rechazo. Si el emisor no
  tiene tokens, el mensaje se confirma sin error.
- Consumo del evento `user.sessions.revoked` y limpieza de tokens ante la
  revocacion de sesiones de un usuario.
- Deduplicacion de eventos por `eventId` para garantizar entrega efectiva
  una unica vez pese a la semantica at-least-once de RabbitMQ.

## Arquitectura

El servicio sigue Clean Architecture con los siguientes limites:

```
/cmd/api             punto de entrada: arranque de API y worker
/internal/domain     entidades e interfaces de dominio (sin dependencias externas)
/internal/service    casos de uso
/internal/repository capa de persistencia (Postgres via GORM)
/internal/delivery   adaptadores de entrada (HTTP y consumidores de RabbitMQ)
/internal/client     adaptadores de salida (Firebase Admin SDK)
/pkg/config          carga de configuracion desde variables de entorno
```

## Requisitos

- Go 1.26 o superior.
- Docker y Docker Compose para la ejecucion en contenedores.
- Red externa `vinculo-vial` ya creada (compartida con el resto de la
  plataforma) y broker RabbitMQ con la topologia declarada por
  `vinculo-vial-core` en ejecucion.

## Configuracion

Las variables de entorno requeridas estan documentadas en `.env.example`.
Para desarrollo local, copiar ese archivo a `.env` y completar los valores
marcados como `REPLACE_WITH_...`.

## Comandos

```
go run ./cmd/api
go test ./...
CGO_ENABLED=0 go build -o notification ./cmd/api
docker compose up --build
```

## Avisos al emisor (US-09)

Los cuatro avisos usan el titulo `ViVIAna` y `data`
`{ "type": "SENT_UPDATED", "alert_id": "<uuid>" }`, con la que la app abre
el chat del emisor:

| Evento | Cola (variable) | Cuerpo |
|---|---|---|
| `alert.created` | `ALERT_QUEUE` | Tu alerta fue validada. Le avisé al dueño. |
| `alert.seen` | `ALERT_SEEN_QUEUE` | El dueño leyó tu mensaje. |
| `alert.rejected` | `ALERT_REJECTED_QUEUE` | No pudimos validar tu alerta. |
| `alert.expired` | `ALERT_EXPIRED_QUEUE` | Tu alerta no tuvo respuesta. |

Variables nuevas (ver `.env.example`): `ALERT_REJECTED_QUEUE`
(`notification.alert-rejected.q`), `ALERT_EXPIRED_QUEUE`
(`notification.alert-expired.q`) y `ALERT_SEEN_QUEUE`
(`notification.alert-seen.q`). Las colas, con su `.retry` y su `.dlq`, las
declara `vinculo-vial-core`: deben existir antes de arrancar el servicio.
Reintentos y DLQ funcionan igual que en los consumidores existentes
(`MAX_RETRIES`, DLQ ruteada por el nombre de la cola de trabajo).

Con `alert.created`, el aviso a los dueños y el aviso al emisor son dos
envios independientes, cada uno con su propia idempotencia: si falla uno y
el mensaje se reintenta, el otro no se repite. Un `alert.created` sin
`alert_id` (alertas anteriores a US-09) se procesa igual, pero sin
`alert_id` en `data`.

Privacidad: el `actor` solo se usa para buscar los tokens del emisor. No se
guarda en `notification_log`, no viaja en el contenido de la push y no se
loguea junto al payload.

## Pruebas de integracion

`go test ./...` corre solo pruebas unitarias (SQLite y dobles en memoria).
La garantia central de idempotencia -dos entregas concurrentes del mismo
`event_id` producen un solo envio- depende del bloqueo que hace Postgres
sobre una fila sin confirmar, y se prueba contra un Postgres real, detras
del build tag `integration`:

```
docker run -d --name pg-it -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=notification_test -p 5432:5432 postgres:16-alpine
TEST_DATABASE_URL="postgres://postgres:postgres@localhost:5432/notification_test?sslmode=disable" \
  go test -tags=integration -count=1 -run Integracion ./internal/repository/postgres/
```

En CI las corre un paso aparte del workflow, contra un servicio `postgres`.

## Salud del servicio

`GET /health` responde `200 OK` mientras el proceso este activo. Es una
sonda de liveness pura: no verifica dependencias externas.

`GET /ready` chequea **solo la base de datos propia**. Devuelve `200 OK` con
`{"status":"ready","checks":{"database":"up"}}`, o `503 Service Unavailable`
con `{"status":"not_ready","checks":{"database":"down"}}` si el ping a la
base falla (timeout de 2s). Es el path del healthcheck activo de Kong: si
falla, Kong saca la instancia del balanceo. Por eso no incluye RabbitMQ:
registrar y dar de baja tokens FCM opera exclusivamente sobre la base, y un
broker caido no debe dejar a la instancia sin servir esas rutas.

`GET /health/deps` expone el estado de todas las dependencias: la base y
las conexiones a RabbitMQ (`rabbitmq_alert_created`,
`rabbitmq_sessions_revoked`, `rabbitmq_alert_rejected`,
`rabbitmq_alert_expired` y `rabbitmq_alert_seen`, una por cada worker). Mismo formato de
respuesta que `/ready`: `200 OK` con `"status":"ready"` si todas estan
arriba, o `503` con `"status":"degraded"` y el detalle por dependencia si
alguna esta caida (la base o cualquiera de los workers). Usa `degraded` y
no `not_ready` a proposito: la instancia sigue lista para el balanceo (eso
lo dice `/ready`), pero opera degradada. Es informativo y **no afecta el
balanceo de Kong**: sirve para diagnosticar que un worker dejo de consumir
aunque la API siga respondiendo.
