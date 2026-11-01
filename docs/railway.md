# Despliegue en Railway: Kafka y WM_Central (con su BD)

El enunciado pide **WM_Central, la BD y Kafka en Railway**, y los WM_WS y WM_FO
en los PCs del laboratorio. Esta guía cubre la parte de Railway.

```
 PC LAB 1                        RAILWAY                         PC LAB 2
 WM_FO ──Kafka──►  kafka   (TCP proxy :9092)  ◄──Kafka──  WM_WS_E
                     ▲                                     │ socket
                     │ Kafka                               ▼
                   wm-central (TCP proxy :9000) ◄─socket── WM_WS_M
                     │  panel web (https, puerto 8080)
                     └─ volumen /data  →  wm_central.db (SQLite)
```

Dos servicios en el mismo proyecto de Railway:

| Servicio | De dónde sale | Puertos públicos |
|---|---|---|
| `kafka` | imagen `apache/kafka:3.9.1` (la misma que en local) | TCP proxy → 9092 |
| `wm-central` | este repositorio de GitHub (`WM_Central/Dockerfile`) | TCP proxy → 9000 (Monitores) y dominio web → 8080 (panel) |

**Por qué TCP proxy:** Railway da por defecto direcciones web (HTTP). Kafka y
nuestros sockets no son HTTP, son TCP. El *TCP proxy* da una dirección pública
`algo.proxy.rlwy.net:PUERTO` que reenvía la conexión tal cual al puerto del
servicio.

---

## 0. Antes de empezar

1. **Cuenta de Railway.** Entra en https://railway.com con la cuenta de GitHub.
2. **Quién crea el proyecto.** El repositorio es de Rubén (`ral44-ua`). Para que
   Railway lo pueda desplegar en cada `push`, la *GitHub App* de Railway tiene
   que tener acceso al repo, y eso solo lo puede dar su dueño. Lo más sencillo:
   - Rubén crea el proyecto en Railway con su cuenta, y
   - en *Project Settings → Members* invita a David.
3. **Plan.** El plan de prueba (*Trial*) tiene crédito limitado; para la
   corrección el proyecto tiene que estar encendido. Mirad el consumo en
   *Usage* y apagad (*Remove deployment*) lo que no uséis.

---

## 1. Servicio `kafka`

1. En el proyecto: **+ Create → Docker Image** → `apache/kafka:3.9.1` → Enter.
2. Cambia el nombre del servicio a `kafka` (*Settings → Service name*). El
   nombre importa: el servicio de CENTRAL lo usa en sus variables.
3. **TCP proxy:** *Settings → Networking → TCP Proxy* → puerto **9092**.
   Railway te da una dirección como `shuttle.proxy.rlwy.net:15140`. Apúntala:
   es la **dirección pública de Kafka** (Engines y FO del laboratorio).
4. **Variables:** *Variables → Raw Editor*, pega esto y guarda:

   ```
   KAFKA_NODE_ID=1
   KAFKA_PROCESS_ROLES=broker,controller
   KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093
   KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://${{RAILWAY_TCP_PROXY_DOMAIN}}:${{RAILWAY_TCP_PROXY_PORT}}
   KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER
   KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
   KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093
   KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1
   KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1
   KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1
   KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0
   KAFKA_NUM_PARTITIONS=3
   KAFKA_HEAP_OPTS=-Xmx256m -Xms256m
   ```

   Son las mismas que en nuestro `docker-compose.yml`, con dos cambios:
   - **`KAFKA_ADVERTISED_LISTENERS`**: es la dirección que Kafka le da a cada
     cliente para que vuelva a conectarse. En local es `localhost:9092`; aquí
     tiene que ser la **dirección pública del TCP proxy**, porque los clientes
     están fuera de Railway. `${{RAILWAY_TCP_PROXY_DOMAIN}}` y
     `${{RAILWAY_TCP_PROXY_PORT}}` las rellena Railway con los datos del paso 3.
     Si al desplegar salen vacías, escribe la dirección a mano:
     `PLAINTEXT://shuttle.proxy.rlwy.net:15140`.
   - **`KAFKA_HEAP_OPTS`**: limita la memoria de Java a 256 MB. Para nuestro
     uso sobra, y así no se pasa del límite del plan.

5. Despliega (*Deploy*). En *Deployments → View logs* tiene que salir
   `Kafka Server started`.
6. **Comprobar desde tu PC** (con el `.venv` activado, en la raíz del repo):
   ```bash
   python -m scripts.prueba_kafka shuttle.proxy.rlwy.net:15140
   ```
   Tiene que acabar en `OK: el mensaje ha ido y vuelto por Kafka.`

> Kafka no lleva volumen: si se redespliega, empieza vacío. No importa, porque
> sus mensajes son de paso (lo que hay que guardar está en la BD de CENTRAL) y
> CENTRAL crea los 5 topics cada vez que arranca.

---

## 2. Servicio `wm-central`

1. **+ Create → GitHub Repo** → `ral44-ua/watermanagement-sd`. Nombre del
   servicio: `wm-central`.
2. **Variables** (*Raw Editor*):

   ```
   RAILWAY_DOCKERFILE_PATH=WM_Central/Dockerfile
   BROKER=${{kafka.RAILWAY_TCP_PROXY_DOMAIN}}:${{kafka.RAILWAY_TCP_PROXY_PORT}}
   PUERTO_SOCKETS=9000
   PORT=8080
   WM_PANEL_CLAVE=pon-aqui-una-clave
   ```

   - `RAILWAY_DOCKERFILE_PATH`: el Dockerfile no está en la raíz del repo.
   - `BROKER`: CENTRAL se conecta a Kafka por la misma dirección pública que
     los PCs del laboratorio. `${{kafka.X}}` lee la variable X del servicio
     `kafka`. Si sale vacía, escribe la dirección a mano.
   - `WM_PANEL_CLAVE`: el panel web es público en Internet; con esto pide
     clave (el usuario da igual). Sin la variable, no pide nada.
3. **Volumen para la BD:** clic derecho sobre el servicio (o `Ctrl+K`) →
   *Attach Volume* → mount path **`/data`**. La BD está en
   `/data/wm_central.db`: sin volumen se borraría en cada despliegue.
4. **Red** (*Settings → Networking*):
   - **Generate Domain** → puerto **8080**. Te da `https://algo.up.railway.app`:
     es el **panel de CENTRAL**.
   - **TCP Proxy** → puerto **9000**. Te da `algo.proxy.rlwy.net:NNNNN`: es la
     **dirección de CENTRAL para los Monitores**.
5. Despliega. En los logs tiene que salir:
   ```
   CENTRAL arrancada: 0 estaciones en BD ...
   Kafka: topics creados [...]          (solo la primera vez)
   Kafka: conectado a ...proxy.rlwy.net:...
   Servidor de sockets escuchando en el puerto 9000
   Panel web en el puerto 8080 (con clave)
   ```
6. Abre el dominio web: tiene que pedir la clave y enseñar el panel vacío.

### CI/CD (VS Code → GitHub → Railway)

Con el servicio conectado al repo, **cada `push` a `main` redespliega
CENTRAL** solo. Para que no se redespliegue cuando cambia algo que no le
afecta (por ejemplo, el FO): *Settings → Source → Watch Paths*:
```
WM_Central/**
common/**
requirements.txt
```

---

## 3. Laboratorio: conectar las estaciones y los operarios

Con las dos direcciones de Railway (ejemplo):

| | Dirección |
|---|---|
| CENTRAL (TCP proxy 9000) | `maglev.proxy.rlwy.net:41234` |
| Kafka (TCP proxy 9092) | `shuttle.proxy.rlwy.net:15140` |

**PC LAB 2 · estaciones** (Monitor primero, después su Engine):
```bash
python -m WM_WS.WS_M.WM_WS_M 9100 maglev.proxy.rlwy.net:41234 WS-04 --ubicacion "River Park"
python -m WM_WS.WS_E.WM_WS_E shuttle.proxy.rlwy.net:15140 127.0.0.1:9100
python -m WM_WS.WS_M.WM_WS_M 9101 maglev.proxy.rlwy.net:41234 WS-11 --ubicacion "Riverside Gardens"
python -m WM_WS.WS_E.WM_WS_E shuttle.proxy.rlwy.net:15140 127.0.0.1:9101
```

**PC LAB 1 · operarios:**
```bash
python -m WM_FO.WM_FO shuttle.proxy.rlwy.net:15140 FO-12
python -m WM_FO.WM_FO shuttle.proxy.rlwy.net:15140 FO-07 --fichero WM_FO/peticiones.txt
```

La primera vez que arranca una WS, CENTRAL contesta `WS no registrada` y el
Monitor se da de alta solo (`REGISTER`) antes de autenticarse.

Con Docker (obligatorio en la corrección), los mismos argumentos en
`docker run`: ver el apartado 2 del `Readme.md` y los Dockerfiles de
`WM_WS/WS_M/`, `WM_WS/WS_E/` y `WM_FO/`.

---

## 4. Problemas frecuentes

| Síntoma | Causa y solución |
|---|---|
| El cliente conecta a Kafka y luego falla con `localhost:9092` o con una dirección vacía | `KAFKA_ADVERTISED_LISTENERS` está mal: Kafka anuncia una dirección que el cliente no alcanza. Pon a mano la del TCP proxy y redespliega `kafka`. |
| En los logs de CENTRAL: `AVISO Kafka: no se han podido comprobar los topics` | Kafka aún no estaba listo. Redespliega `wm-central` cuando Kafka diga `Kafka Server started`. |
| Las estaciones salen DESCONECTADA tras redesplegar CENTRAL | Normal durante unos segundos: los Monitores reconectan solos cada 3 s. |
| La BD está vacía tras cada despliegue | Falta el volumen en `/data` (paso 2.3). |
| El panel no carga | El dominio web tiene que apuntar al puerto **8080** (paso 2.4). |
| `Address already in use` en el PC del laboratorio | Hay otro Monitor usando ese puerto del Engine: usa 9100, 9101, 9102... |
