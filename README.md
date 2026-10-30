# WaterManagement · SD 26/27 (Release 1)

Simulación de una red de riego de parques: una central (**WM_Central**) que
registra, vigila y controla estaciones de riego (**WM_WS**, formadas por un
Monitor y un Engine), y una aplicación para los operarios (**WM_FO**).

- Lenguaje: Python 3.9+ · Kafka con `confluent-kafka` · BD SQLite.
- Sockets TCP con tramas `<STX>DATA<ETX><LRC>` para autenticación y salud.
- Kafka para todo lo asíncrono (peticiones, órdenes, telemetría, avisos).
- Despliegue: CENTRAL, su BD y Kafka en **Railway**; WM_WS y WM_FO en los
  PCs del laboratorio, todo con **Docker**.

Autores: ver `Nombres.txt`.

## Estructura

```
wm_central/      CENTRAL: WM_Central.py, sockets, Kafka, BD, panel web, Dockerfile
wm_ws/WS_M/      Monitor de una WS (WM_WS_M.py) + Dockerfile
wm_ws/WS_E/      Engine de una WS (WM_WS_E.py) + Dockerfile
WM_FO/           Aplicación del operario (WM_FO.py, peticiones.txt) + Dockerfile
common/          Código compartido: protocolo de tramas, mensajes Kafka, utilidades
tests/           Tests (pytest), sin necesidad de Kafka
docs/            Enunciado, contrato entre módulos y guía de Railway
prompts/         Prompts de IA usados (obligatorio)
scripts/         Utilidades de prueba (crear topics, probar Kafka...)
```

Todos los módulos se ejecutan **desde la raíz** del repositorio
(`python -m carpeta.Modulo ...`) para que funcione `from common import ...`.
Las imágenes de Docker se construyen también desde la raíz.

## 1. Parte cloud (Railway)

CENTRAL (con su BD SQLite en un volumen) y Kafka se despliegan en Railway.
Paso a paso en [`docs/railway.md`](docs/railway.md). Al acabar tendréis:

| | Ejemplo | Lo usan |
|---|---|---|
| Kafka (TCP proxy) | `shuttle.proxy.rlwy.net:15140` | Engines y FO |
| CENTRAL sockets (TCP proxy) | `maglev.proxy.rlwy.net:41234` | Monitores |
| Panel web de CENTRAL | `https://algo.up.railway.app` | profesor / vosotros |

Cada `push` a `main` redespliega CENTRAL (CI/CD VS Code → GitHub → Railway).

## 2. Laboratorio con Docker

En cada PC, una vez, desde la raíz del repo:

```bash
docker build -f wm_ws/WS_M/Dockerfile -t wm-ws-m .
docker build -f wm_ws/WS_E/Dockerfile -t wm-ws-e .
docker build -f WM_FO/Dockerfile     -t wm-fo .
docker network create wm          # red para que el Engine encuentre a su Monitor
```

Sustituid `CENTRAL` y `KAFKA` por las direcciones de Railway.

**PC 1 · estaciones** (cada una en su terminal; primero el Monitor, después su Engine):

```bash
docker run -it --rm --name ws04-m --network wm wm-ws-m 9100 CENTRAL WS-04 --ubicacion "River Park"
docker run -it --rm --name ws04-e --network wm -v wm-ws04:/datos wm-ws-e KAFKA ws04-m:9100

docker run -it --rm --name ws11-m --network wm wm-ws-m 9100 CENTRAL WS-11 --ubicacion "Riverside Gardens"
docker run -it --rm --name ws11-e --network wm -v wm-ws11:/datos wm-ws-e KAFKA ws11-m:9100
```

**PC 2 · operarios:**

```bash
docker run -it --rm wm-fo KAFKA FO-12
docker run -it --rm wm-fo KAFKA FO-07 --fichero WM_FO/peticiones.txt
```

Se pueden lanzar tantas WS y FO como se quiera: cada WS con su ID, su
nombre de contenedor y su volumen; cada FO con su ID (`FO-<número>`; un ID
nuevo se da de alta solo en su primera petición).

### Sin Docker (para desarrollo)

```bash
python -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt

docker compose up -d                                  # Kafka local
python -m wm_central.WM_Central 9000 127.0.0.1:9092   # panel web en http://127.0.0.1:8080
python -m wm_ws.WS_M.WM_WS_M 9100 127.0.0.1:9000 WS-04 --ubicacion "River Park"
python -m wm_ws.WS_E.WM_WS_E 127.0.0.1:9092 127.0.0.1:9100
python -m WM_FO.WM_FO 127.0.0.1:9092 FO-12
```

## 3. Uso

| Módulo | Qué se ve / qué se puede hacer |
|---|---|
| CENTRAL (panel web y consola) | ID, ubicación, estado con color, caudal, volumen, **agua total suministrada** y operario de cada WS. Botones: iniciar riego, bloquear y activar, en una WS o en todas. |
| Monitor | Autenticación, Engine conectado, OK o FUGA, errores de conexión con CENTRAL. |
| Engine | Menú: `k` avería on/off · `p` parar riego · `r` pedir riego · `q` salir. Telemetría de cada segundo. |
| FO | Menú: `1` ver estaciones · `2` pedir riego (`WS-04#30`). Con `--fichero`, lanza las peticiones del fichero una a una. |

Estados de una WS: Disponible (verde), Regando (verde parpadeante), Fuga
(rojo), Fuera de Servicio (naranja), Desconectada (gris).

## 4. Resiliencia

| Qué se cae | Qué pasa |
|---|---|
| **Monitor** | CENTRAL marca la WS como Desconectada (socket cerrado o 5 s sin ALIVE) y corta el riego. El Engine muestra **AVERIADA**, corta su riego y rechaza riegos (también los manuales) hasta que el Monitor vuelve. |
| **Engine** | El Monitor avisa de FUGA y CENTRAL cierra el riego con la última telemetría y avisa al operario. El Engine guarda el riego en curso en disco (`riego_WS-04.json`, en el volumen `/datos`); al volver a lanzarlo, **envía el estado final** y CENTRAL corrige los totales. |
| **FO** | El riego sigue. Al relanzar el FO **con el mismo ID**, recibe los avisos que se perdió (el RESUMEN), marcados como `[PENDIENTE]`. |
| **CENTRAL** | Las WS terminan su riego. El Monitor muestra que no puede conectar y reintenta cada 3 s. El FO y el Engine muestran **"Imposible conectar con WM_Central"** (no llega el estado de la red en 15 s) y no envían peticiones. Al volver, CENTRAL recupera el riego que siguiera en marcha (lo "adopta" con la telemetría) o lo guarda si terminó mientras estaba caída, y manda el RESUMEN: **no se pierde ningún riego**. |

## 5. Cambios respecto al contrato entre módulos

Añadidos durante la revisión con la guía de corrección (todos compatibles con
los mensajes anteriores):

1. **Registro separado de la autenticación** (sockets Monitor → CENTRAL):
   - `AUTH` de una WS que no está en la BD → `KO#AUTH#WS no registrada`.
   - Nueva trama `REGISTER#WS-04#River Park` → `OK#REGISTER` o
     `KO#REGISTER#motivo` (ya registrada, ID no válido).
   - El Monitor, al recibir "no registrada", se registra y repite el `AUTH`.
     Con `--sin-registro` no lo hace (para enseñar la denegación).
2. **`START`, `TELEMETRIA` y `FINISHED` llevan `fo_id` y `origen`**. El Engine
   los copia del `START`. Sirven para que una CENTRAL reiniciada sepa de quién
   es un riego que ya no tiene en memoria.
3. **`DENEGADA` en `wm.ws.commands`** (clave `ws_id`): cuando CENTRAL deniega
   un riego pedido desde la propia WS (`origen: "WS"`), se lo dice a su Engine.
4. **El Engine también lee `wm.ws.status`**, solo para saber si CENTRAL
   sigue viva.
5. **El FO lee `wm.fo.notifications` desde el último offset de su grupo**
   (antes saltaba al final), para recibir los avisos atrasados.
6. **Operarios**: un `FO-<número>` desconocido se da de alta en su primera
   petición; un ID con otro formato se deniega.
7. Motivo `DESCONEXION` en `FINISHED`: el Engine corta el riego al perder el Monitor.

## 6. Tests

```bash
pip install pytest
python -m pytest tests
```

No necesitan Kafka: usan un productor falso y sockets en `localhost`.
Los de la guía de corrección están en `test_resiliencia_central.py`,
`test_engine.py`, `test_fo_resiliencia.py` y `test_monitor_registro.py`.
