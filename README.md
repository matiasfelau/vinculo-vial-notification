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
- Consumo de `alert.pending.delivered` y un unico push a cada dueno de
  `owner_ids` ("Tenés alertas sin leer en el buzón."). Nunca al emisor.
- Consumo del evento `user.sessions.revoked` y limpieza de tokens ante la
  revocacion de sesiones de un usuario.
- Consumo de `email.requested` y envio por SMTP de los codigos y avisos de
  Identity (verificacion de email, recuperacion de contraseña), con
  plantillas en español. Ver "Canal de email (US-13)".
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
/internal/client     adaptadores de salida (Firebase Admin SDK, SMTP)
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
| `alert.created` | `ALERT_QUEUE` | Tu alerta fue validada. |
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

Un `alert.created` con `owner_ids: []` es valido (la alerta se valido antes
de que la patente tenga duenos): se avisa solo al emisor y no va a la DLQ.

Privacidad: el `actor` solo se usa para buscar los tokens del emisor. No se
guarda en `notification_log`, no viaja en el contenido de la push y no se
loguea junto al payload.

## Eventos sin `actor` (US-11)

Cuando un emisor se da de baja, sus alertas quedan con `issuer_id = NULL` y
todo evento cuyo `actor` era el emisor sale **sin `actor`** (ausente, `null`
o vacio). Eso no es un error: no hay a quien avisar y nunca lleva a la DLQ.

| Evento | Sin `actor` |
|---|---|
| `alert.created` con `owner_ids` | Avisa solo a los dueños |
| `alert.created` sin `owner_ids` | Ack sin push |
| `alert.seen`, `alert.expired`, `alert.rejected` | Ack sin push |

Un `actor` presente que no es un UUID sigue siendo invalido: en
`alert.seen`, `alert.expired`, `alert.rejected` y en `alert.created` sin
`owner_ids` va directo a la DLQ. Un `alert.created` con `owner_ids` y un
`actor` que no es UUID avisa a los dueños y omite el aviso al emisor, como
antes. El resto de la validacion no cambia: un payload invalido va a la DLQ
con o sin `actor`.

La baja no tiene un evento propio para este servicio. Identity publica
`user.sessions.revoked` junto con `user.deleted` (con el usuario en `actor`),
y el consumidor existente de `SESSIONS_REVOKED_QUEUE` borra todos los
device tokens de ese usuario. `user.sessions.revoked` sigue exigiendo un
`actor` UUID valido: sin el no se sabe de quien borrar los tokens. Un token
que la app registre dentro de los `ACCESS_TOKEN_TTL_MINUTES` posteriores a
la baja queda huerfano, pero nunca recibe pushes porque ya no hay eventos de
ese usuario.

## Alertas pendientes entregadas (US-10)

Cuando un usuario vincula un vehiculo cuya patente tenia alertas pendientes,
Alert publica `alert.pending.delivered` con `{ owner_ids, count }` y sin
`actor`. Este servicio lo consume desde `ALERT_PENDING_DELIVERED_QUEUE`
(`notification.alert-pending-delivered.q`, con su `.retry` y su `.dlq`
declaradas por `vinculo-vial-core`) y manda un unico push a cada dueno:

| Titulo | Cuerpo | `data` |
|---|---|---|
| Alerta de Vinculo Vial | Tenés alertas sin leer en el buzón. | `{ "type": "INBOX_UPDATED" }` |

`count` se ignora: el aviso es generico. Si ningun dueno tiene tokens, el
mensaje se confirma sin error. Un `owner_ids` ausente o vacio, o con un valor
que no es UUID, es un mensaje invalido y va directo a la DLQ. Reintentos,
`MAX_RETRIES` y DLQ funcionan igual que en los demas consumidores.

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

Las pruebas del worker (`internal/delivery/worker`) tambien llevan el tag
`integration` y necesitan un RabbitMQ con la topologia de Core (con
`RETRY_TTL_MS` bajo, por ejemplo 1000) y un Mailpit:

- `dlq_integration_test.go`: el ciclo real de retry y DLQ de las colas de
  alert.created y user.sessions.revoked.
- `email_integration_test.go`: el mismo ciclo para `email.requested`, cuya
  DLQ debe quedar con `to` y `code` en `null`, y ademas publica un
  `email.requested` por cada plantilla, lo consume el worker real, lo envia
  el adaptador SMTP real y verifica en la API de Mailpit el asunto, el
  remitente, el destinatario y los dos cuerpos.

```
docker run -d --name mailpit -p 127.0.0.1:1025:1025 -p 127.0.0.1:8025:8025 axllent/mailpit:v1.33.1
TEST_RABBITMQ_URL=amqp://guest:guest@localhost:5672/ \
  TEST_SMTP_ADDR=localhost:1025 TEST_MAILPIT_API=http://127.0.0.1:8025 \
  go test -tags=integration -count=1 -run Integracion ./internal/delivery/worker/
```

En el CI de este repo solo corre el paso de Postgres; estas dos suites se
corren a mano.

## CI

`.github/workflows/ci.yml` corre en todo `pull_request` hacia `dev` o `main`
(y en `push` a esas ramas). Tiene dos jobs:

| Job | Que hace | Corta? |
|---|---|---|
| `build-and-test` | compila, `gofmt`, `go vet` (tambien con el tag `integration`), pruebas con `-race` y cobertura, integracion contra Postgres y `diff-cover` | Si |
| `security` | `govulncheck`, Trivy `fs` y Semgrep | `govulncheck` y Trivy si; Semgrep no (informativo, pasa a gate en v0.14) |

Las acciones de terceros estan fijadas por SHA de commit (la version va en un
comentario) y el workflow declara `permissions: contents: read`. Dependabot
actualiza semanalmente `gomod`, `github-actions` y `docker`.

### Cobertura

- **Un unico reporte** con las pruebas unitarias y las de integracion de
  Postgres, unidos con `gocovmerge` y convertidos a Cobertura XML. Cada perfil
  usa `-coverpkg=./...`: un codigo cubierto desde otro paquete cuenta.
- **Gate sobre el codigo nuevo:** `diff-cover` exige 85 % o mas de las lineas
  que el PR agrega o cambia respecto de la rama base.
- **Gate sobre el total:** el job falla si la cobertura de sentencias baja de
  85 %. Hoy mide 94,7 %. Se publica ademas en el resumen del job y como
  artefacto `cobertura`. Ninguna rama puede bajar el total de ese minimo.
- **Exclusiones:** ninguna. No hay codigo generado y el escrito a mano cuenta
  entero. `main` queda en una linea (`exitFunc(exitCode(execute))`): `execute`
  carga la configuracion y llama a `runUntilSignal`, que atrapa
  SIGINT/SIGTERM y llama a `run(ctx, cfg)`, que arma las dependencias, levanta
  la API y los workers, y se prueba con SQLite en memoria y un broker
  inalcanzable. Lo que queda sin cubrir en el CI:
  - la llamada de `main()` (termina en `os.Exit`);
  - las fabricas de consumidores dentro de `run` y `dial` /
    `realAMQPChannel` en `internal/delivery/worker`, que solo se ejecutan con
    un RabbitMQ real (las cubren las suites de integracion del worker, que se
    corren a mano);
  - ramas defensivas que no se pueden provocar: fallas de `crypto/rand`,
    errores de escritura sobre un `bytes.Buffer` y los `panic` de arranque por
    plantillas o zona horaria embebidas invalidas.

### Seguridad

`govulncheck` falla ante vulnerabilidades alcanzables y Trivy ante HIGH o
CRITICAL con correccion disponible (tambien busca secretos y errores de
configuracion). Las excepciones de Trivy viven solo en `.trivyignore`, cada una
con su motivo y su fecha de revision; hoy no hay ninguna.

La imagen corre como el usuario sin privilegios `65532:65532`, como el resto de
los servicios. El archivo `firebase-credentials.json` que se monta en
`/app` tiene que ser legible por ese usuario (por ejemplo, permisos `0644`).

El CI instala la version de Go que dice `go.mod`, asi que esa linea tiene que
estar en un parche sin vulnerabilidades de la biblioteca estandar: hoy es
1.26.11.

### Correr en local

```bash
# Cobertura (perfiles unitario y de integracion, unidos y convertidos a Cobertura XML)
go test ./... -count=1 -covermode=atomic -coverpkg=./... -coverprofile=coverage-unit.out
TEST_DATABASE_URL="postgres://postgres:postgres@localhost:5432/notification_test?sslmode=disable" \
  go test -tags=integration -count=1 -run Integracion -covermode=atomic -coverpkg=./... \
  -coverprofile=coverage-integration.out ./internal/repository/postgres/
go install github.com/wadey/gocovmerge@b5bfa59ec0ad
go install github.com/boumenot/gocover-cobertura@v1.5.0
gocovmerge coverage-unit.out coverage-integration.out > coverage.out
GOFLAGS=-tags=integration gocover-cobertura < coverage.out > coverage.xml
go tool cover -func=coverage.out | tail -n 1

# Gate sobre el codigo nuevo (con los cambios commiteados y origin/dev actualizado)
pip install diff-cover==11.0.2
diff-cover coverage.xml --compare-branch=origin/dev --fail-under=85

# Seguridad
go install golang.org/x/vuln/cmd/govulncheck@v1.9.0
govulncheck ./...
docker run --rm -v "$PWD:/src" aquasec/trivy:latest fs --scanners vuln,secret,misconfig \
  --severity HIGH,CRITICAL --ignore-unfixed --ignorefile /src/.trivyignore /src
```

## Canal de email (US-13)

Notification envia por email los codigos y avisos de Identity. Consume
`email.requested` de `notification.email-requested.q` (con su `.retry` y su
`.dlq`, declaradas por Core) y lo entrega por SMTP: Mailpit en local, el
endpoint SMTP de SES en AWS, solo con cambio de configuracion.

```
{ "template": "<plantilla>", "to": "<email>", "code": "<6 digitos>" | null, "expires_at": "RFC3339" | null }
```

| Plantilla | Asunto | `code` / `expires_at` |
|---|---|---|
| `EMAIL_VERIFICATION` | Tu código de Vinculo Vial | Obligatorios |
| `ACCOUNT_EXISTS` | Ya tenés una cuenta en Vinculo Vial | No se usan |
| `PASSWORD_RESET` | Recuperá tu contraseña | Obligatorios |
| `PASSWORD_CHANGED` | Cambiaste tu contraseña | No se usan; la fecha es la del evento, en `America/Argentina/Buenos_Aires` |

Las plantillas (texto plano y HTML minimo, en español) estan en
`internal/service/email/templates/`, incrustadas en el binario. No llevan
imagenes remotas, enlaces ni seguimiento. El vencimiento se muestra como los
minutos que faltan al momento del envio.

**Comportamiento del consumidor**

- **Idempotente por `event_id`**, como el resto: el registro del evento y
  el envio van en una misma transaccion que solo se confirma si el servidor
  SMTP acepto el mensaje. Si el servidor acepta y el commit falla, el
  reintento lo vuelve a enviar (at-least-once).
- **Vencido:** si `expires_at` ya paso, se confirma sin enviar y sin DLQ.
- **Falla transitoria** (timeout, conexion, respuesta 4xx, circuito abierto,
  y tambien un 5xx en el saludo, STARTTLS o la autenticacion, que son
  problemas de la conexion o de la configuracion y no de un mensaje): va a
  `.retry`; agotado `MAX_RETRIES`, a la DLQ.
- **Rechazo permanente** del servidor (5xx a `MAIL`, `RCPT` o `DATA`): se
  confirma, se registra como `FAILED` y se loguea con el codigo de la
  respuesta, sin destinatario ni el texto del servidor (que suele repetirlo).
  No abre el breaker.
- **Pedido invalido** (plantilla desconocida, `to` mal formado, falta `code`
  o `expires_at` donde corresponde, payload ilegible): va directo a la DLQ,
  sin reintentos.
- **El `actor` no se usa:** es nulo en `EMAIL_VERIFICATION` y
  `ACCOUNT_EXISTS` porque todavia no hay cuenta.

**Datos personales.** Nunca se loguean `to` ni `code`, ni se incluyen en un
error. Al publicar un `email.requested` en la DLQ, `to` y `code` van en
`null`; el resto del envelope se conserva para diagnosticar. Si el cuerpo es
tan ilegible que no se puede redactar, se publica `{}`. El registro de
idempotencia (`notification_log`) guarda solo `event_id` y el resultado.

**Resiliencia.** El adaptador (`internal/client/smtp`) acota toda la
operacion con `SMTP_TIMEOUT` y se protege con un circuit breaker propio, con
el mismo criterio que el de FCM: cuentan como falla los timeouts, los errores
de conexion y los 4xx; con el circuito abierto el envio falla sin tocar la
red y el mensaje vuelve por `.retry`. Con `SMTP_STARTTLS=true` exige TLS y
verifica el certificado del servidor: si no ofrece STARTTLS, el envio falla
y nunca baja a texto plano. Autentica con `AUTH PLAIN` solo si hay
credenciales. No soporta TLS implicito (puerto 465); SES usa el 587 con
STARTTLS.

**Mailpit (local).** `docker compose up` levanta `axllent/mailpit` junto al
servicio: el SMTP escucha en `mailpit:1025` solo dentro de la red interna
`notification-internal` (no se publica al host) y la bandeja web en
<http://127.0.0.1:8025>, publicada unicamente en el loopback porque los
mails contienen codigos de un solo uso. Los mensajes viven en memoria.

**Variables** (documentadas en `.env.example`)

| Variable | Default | Nota |
|---|---|---|
| `EMAIL_REQUESTED_QUEUE` | `notification.email-requested.q` | Tiene que coincidir con la de Core |
| `APP_ENV` | vacio | Con `production`, la configuracion rechaza `SMTP_STARTTLS` distinto de `true` |
| `SMTP_HOST`, `SMTP_PORT` | `mailpit`, `1025` | |
| `SMTP_USERNAME`, `SMTP_PASSWORD` | vacios | Secretos; se definen juntos o ninguno |
| `SMTP_FROM` | `Vinculo Vial <no-responder@vinculovial.local>` | |
| `SMTP_STARTTLS` | `false` | Obligatorio `true` con `APP_ENV=production` |
| `SMTP_TIMEOUT` | `5s` | Formato de duracion de Go |
| `SMTP_BREAKER_FAILURE_THRESHOLD` / `_WINDOW_SECONDS` / `_OPEN_SECONDS` | `5` / `60` / `30` | Igual que FCM |

`user.sessions.revoked` con `reason: "PASSWORD_RESET"` (recuperacion de
contraseña) se trata como cualquier otro motivo: el `reason` es informativo
y solo importa el `actor`, cuyos device tokens se borran.

## Release

La imagen se publica en Docker Hub con `.github/workflows/publish.yml`, que se dispara con un tag de versión sobre `main`:

```
git checkout main && git pull
git tag -a v0.4.0 -m "v0.4.0" && git push origin v0.4.0
```

El job `test` corre la compilación, el análisis estático y las pruebas unitarias y de integración. Si pasan, el job `publish` sube `felaufernandezbottaro/vinculo-vial-notification` con dos tags: la versión sin la `v` y `latest`. Un tag que no apunta a un commit de `main` no se publica. Requiere los secrets `DOCKERHUB_USERNAME` y `DOCKERHUB_TOKEN` en el repo.

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
`rabbitmq_alert_expired`, `rabbitmq_alert_seen`,
`rabbitmq_alert_pending_delivered` y `rabbitmq_email_requested`, una por
cada worker). Mismo formato de
respuesta que `/ready`: `200 OK` con `"status":"ready"` si todas estan
arriba, o `503` con `"status":"degraded"` y el detalle por dependencia si
alguna esta caida (la base o cualquiera de los workers). Usa `degraded` y
no `not_ready` a proposito: la instancia sigue lista para el balanceo (eso
lo dice `/ready`), pero opera degradada. Es informativo y **no afecta el
balanceo de Kong**: sirve para diagnosticar que un worker dejo de consumir
aunque la API siga respondiendo.
