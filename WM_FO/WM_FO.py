"""
WM_FO · Aplicación del operario de campo (Field Operator).

Uso (siempre desde la raíz del repositorio):
    python -m WM_FO.WM_FO <ip_broker:puerto> <fo_id> [--fichero RUTA]
    python -m WM_FO.WM_FO 127.0.0.1:9092 FO-12
    python -m WM_FO.WM_FO 127.0.0.1:9092 FO-12 --fichero WM_FO/peticiones.txt

Qué hace:
  - Pide riegos a CENTRAL publicando SOLICITUD_RIEGO en wm.requests.
  - Muestra lo que CENTRAL le contesta en wm.fo.notifications (solo lo suyo):
    PASO, ACEPTADA, DENEGADA, TELEMETRIA y RESUMEN.
  - Guarda el último ESTADO_RED de wm.ws.status para enseñar las estaciones.

Dos modos:
  - Manual: menú para ver las estaciones y pedir riegos.
  - Fichero (--fichero): lanza las peticiones del fichero una a una. Espera a que
    cada una termine (RESUMEN o DENEGADA), deja 4 s y lanza la siguiente.
    Al acabar el fichero pasa al menú manual.

El FO NO habla con ningún socket: todo va por Kafka. Por eso no necesita saber
la IP de CENTRAL, solo la del broker.

Resiliencia (guía de corrección):
  - Si el FO se cierra mientras le riegan, al volver a arrancar con el MISMO
    ID recibe los avisos que se perdió (por ejemplo, el RESUMEN): Kafka guarda
    por dónde iba su grupo (wm-fo-<ID>) y sigue desde ahí. Se marcan como
    [PENDIENTE] para que se vea que llegaron mientras estaba cerrado.
  - Si CENTRAL deja de publicar el estado de la red (cada 5 s), muestra
    "Imposible conectar con WM_Central" y no envía peticiones.

Hilos:
  - Principal: menú de teclado o recorrido del fichero.
  - hilo_kafka: un único consumidor suscrito a los DOS topics que lee el FO.
"""
import argparse
import re
import threading
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import OFFSET_END

from common import kafka_utils as ku
from common import mensajes as m
from common.utilidades import VigiaCentral, ip_puerto, log   # mismas utilidades que el Monitor y el Engine

PAUSA_ENTRE_PETICIONES = 4    # s entre una petición del fichero y la siguiente (enunciado)
MARGEN_ESPERA = 60            # s de margen, además de la duración, antes de dar una petición por perdida
ESPERA_KAFKA = 30             # s máximos esperando a que Kafka nos asigne particiones
ESPERA_ESTADO = 7             # s esperando el primer ESTADO_RED al arrancar (llega cada 5 s)


# ---------------------------------------------------------------------------
# Validación de argumentos y del fichero
# ---------------------------------------------------------------------------

def id_fo(texto: str) -> str:
    """Los IDs llevan guion (contrato): FO-12. Es la clave con la que CENTRAL
    nos manda las notificaciones, así que tiene que ser exacto."""
    if not re.fullmatch(r"FO-\d+", texto):
        raise argparse.ArgumentTypeError(f"'{texto}' no es un ID válido (ejemplo: FO-12)")
    return texto


def leer_peticion(linea: str) -> tuple:
    """'WS-04#30' -> ('WS-04', 30). Lanza ValueError si la línea no es válida.

    El formato está pendiente de confirmar con el profesor: si cambia, solo hay
    que tocar esta función.
    """
    partes = linea.strip().split("#")
    if len(partes) != 2:
        raise ValueError("el formato es WS-04#30 (ID#segundos)")
    ws_id, segundos = partes[0].strip(), partes[1].strip()
    if not re.fullmatch(r"WS-\d+", ws_id):
        raise ValueError(f"'{ws_id}' no es un ID de estación (ejemplo: WS-04)")
    if not segundos.isdigit() or int(segundos) <= 0:
        raise ValueError(f"'{segundos}' no es un número de segundos mayor que 0")
    return ws_id, int(segundos)


def leer_fichero(ruta: str) -> list:
    """Devuelve la lista de peticiones válidas del fichero.

    Se saltan las líneas vacías y las que empiezan por '#' (comentarios). Una
    línea mal escrita se avisa y se salta: no tiene sentido abortar todas las
    peticiones por una errata.
    """
    peticiones = []
    with open(ruta, encoding="utf-8") as f:
        for n, linea in enumerate(f, start=1):
            if not linea.strip() or linea.lstrip().startswith("#"):
                continue
            try:
                peticiones.append(leer_peticion(linea))
            except ValueError as e:
                log(f"[AVISO] {ruta}, línea {n}: {e}. Se salta")
    return peticiones


def colocar_offsets(consumer, particiones) -> None:
    """Kafka llama a esta función al darnos las particiones. Decide desde dónde
    se lee cada topic:

      - wm.ws.status (estado de la red): desde el FINAL. Solo interesa el
        estado de ahora, no los cientos que se publicaron mientras estábamos
        cerrados.
      - wm.fo.notifications (nuestros avisos): desde donde lo dejó nuestro
        grupo (wm-fo-<ID>). No se toca el offset: Kafka usa el guardado. Así,
        si el FO se cerró en mitad de un riego, al volver recibe los avisos
        que se perdió, entre ellos el RESUMEN (lo exige la guía de corrección).
        La primera vez que arranca un ID no hay nada guardado y se aplica
        auto.offset.reset = "latest": no se ven avisos de otras sesiones.

    Antes se saltaba al final en los dos topics, y el RESUMEN se perdía.
    """
    for p in particiones:
        if p.topic == m.T_STATUS:
            p.offset = OFFSET_END
    consumer.assign(particiones)


# ---------------------------------------------------------------------------
# FO
# ---------------------------------------------------------------------------

class FO:
    def __init__(self, fo_id: str, broker: str, producer=None):
        self.fo_id = fo_id
        self.broker = broker
        # Un Producer se puede compartir entre hilos. Los tests pasan uno falso.
        self.producer = producer or ku.crear_producer(broker)
        # Hora de arranque (UTC, como el "ts" de los mensajes). Un aviso más
        # antiguo es uno que llegó mientras el FO estaba cerrado.
        self.arranque = datetime.now(timezone.utc)
        self.vigia = VigiaCentral()     # ¿CENTRAL sigue publicando el estado?

        # 'listo' se activa cuando Kafka nos asigna particiones. Hasta entonces NO se
        # puede pedir nada: con 'latest', lo que CENTRAL conteste antes de ese
        # momento no nos llegaría nunca, y el modo fichero se quedaría esperando.
        self.listo = threading.Event()
        self.saliendo = threading.Event()

        # Compartido entre hilo_kafka (escribe) y el menú (lee): va con lock.
        self.lock = threading.Lock()
        self.estaciones = []          # último ESTADO_RED: [{"ws_id", "ubicacion", "estado"}]
        self.esperando = None         # req_id de la petición del fichero en curso
        self.fin_peticion = threading.Event()   # se activa con su RESUMEN o DENEGADA

    # ----------------------------- Pedir riego -----------------------------

    def pedir_riego(self, ws_id: str, segundos: int, req_id: str) -> bool:
        """Publica la SOLICITUD_RIEGO.

        req_id es un UUID (uuid4): único aunque dos FO pidan a la vez, y es lo que
        CENTRAL repite en todas sus respuestas para saber de qué petición hablan.
        La clave del mensaje es ws_id (contrato): las peticiones de una misma
        estación van a la misma partición y CENTRAL las recibe en orden.

        Devuelve False (y no envía nada) si CENTRAL no da señales: la guía
        dice que con CENTRAL caída no se admiten peticiones nuevas, y así el
        operario lo sabe al momento en vez de esperar una respuesta que no llega.
        """
        if not self.vigia.disponible():
            log(f"[ERROR] Imposible conectar con WM_Central: no se envía la petición para {ws_id}")
            return False
        ku.enviar(self.producer, m.T_REQUESTS, ws_id,
                  m.solicitud_riego(req_id, "FO", self.fo_id, ws_id, segundos))
        log(f"[PETICIÓN] Riego de {segundos} s en {ws_id} enviado (req {req_id[:8]})")
        return True

    # ----------------------------- Kafka -----------------------------

    def hilo_kafka(self) -> None:
        """Un solo consumidor para los dos topics que lee el FO.

        Un Consumer no se puede compartir entre hilos, pero sí puede leer varios
        topics. Con uno solo hay un hilo menos y un único group.id (wm-fo-FO-12):
        cada FO recibe TODO lo de esos topics (publicación/suscripción) y se
        queda con lo suyo.
        """
        topics = [m.T_NOTIFICATIONS, m.T_STATUS]
        consumer = ku.crear_consumer(self.broker, f"wm-fo-{self.fo_id}", topics)
        consumer.subscribe(topics, on_assign=colocar_offsets)
        try:
            while not self.saliendo.is_set():
                msg = consumer.poll(1.0)
                if not self.listo.is_set() and consumer.assignment():
                    self.listo.set()
                if msg is None:
                    continue
                if msg.error():
                    log(f"[AVISO] Kafka: {msg.error()}")
                    continue
                try:
                    if msg.topic() == m.T_STATUS:
                        if ku.es_para_mi(msg, m.KEY_RED):
                            self.guardar_estado(ku.leer(msg))
                            self.vigia.latido()          # CENTRAL está viva
                    elif ku.es_para_mi(msg, self.fo_id):
                        self.mostrar_notificacion(ku.leer(msg))
                except (ValueError, KeyError, TypeError) as e:
                    # Un mensaje mal formado no debe tirar el hilo: se ignora.
                    log(f"[AVISO] Mensaje mal formado, se ignora ({e!r})")
        finally:
            # close() avisa a Kafka de que dejamos el grupo; sin él, al reiniciar
            # el FO tardaría ~45 s en volver a recibir particiones.
            consumer.close()

    def guardar_estado(self, msg: dict) -> None:
        with self.lock:
            self.estaciones = msg["estaciones"]

    def es_pendiente(self, n: dict) -> bool:
        """True si el aviso se envió antes de arrancar este FO, es decir,
        mientras estaba cerrado (le llega ahora porque Kafka lo guardó)."""
        try:
            ts = datetime.fromisoformat(n["ts"])
        except (KeyError, TypeError, ValueError):
            return False
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        # 1 s de margen: "ts" va en segundos enteros (timespec="seconds").
        return (self.arranque - ts).total_seconds() > 1

    def mostrar_notificacion(self, n: dict) -> None:
        tipo, ws_id, req = n["tipo"], n["ws_id"], n["req_id"]
        corto = req[:8]                              # el UUID entero no cabe en pantalla
        marca = "[PENDIENTE] " if self.es_pendiente(n) else ""
        if tipo == "PASO":
            log(f"{marca}[{ws_id}] {n['texto']}  (req {corto})")
        elif tipo == "ACEPTADA":
            log(f"{marca}[{ws_id}] ACEPTADA: la estación va a empezar a regar  (req {corto})")
        elif tipo == "DENEGADA":
            log(f"{marca}[{ws_id}] DENEGADA: {n['motivo']}  (req {corto})")
        elif tipo == "TELEMETRIA":
            log(f"{marca}[{ws_id}] Regando · caudal {n['caudal_lpm']:4.1f} L/min · volumen {n['volumen_l']:6.2f} L")
        elif tipo == "RESUMEN":
            log(f"{marca}[{ws_id}] RESUMEN · motivo {n['motivo']} · {n['volumen_total_l']:.2f} L"
                f" en {n['duracion_s']} s  (req {corto})")
        else:
            log(f"[AVISO] Notificación desconocida: {tipo}")
            return

        # ¿Es el final de la petición que está esperando el modo fichero?
        if tipo in ("RESUMEN", "DENEGADA"):
            with self.lock:
                if req == self.esperando:
                    self.fin_peticion.set()

    # ----------------------------- Modo fichero -----------------------------

    def modo_fichero(self, ruta: str) -> None:
        try:
            peticiones = leer_fichero(ruta)
        except OSError as e:
            log(f"[ERROR] No se puede leer el fichero {ruta} ({e})")
            return
        log(f"[FICHERO] {len(peticiones)} peticiones en {ruta}")

        for i, (ws_id, segundos) in enumerate(peticiones, start=1):
            log(f"[FICHERO] Petición {i}/{len(peticiones)}: {ws_id} durante {segundos} s")
            # Se prepara la espera ANTES de enviar: si CENTRAL contestara muy rápido,
            # la respuesta podría llegar antes de que hubiéramos apuntado el req_id.
            with self.lock:
                self.fin_peticion.clear()
                self.esperando = req_id = str(uuid.uuid4())
            if not self.pedir_riego(ws_id, segundos, req_id):
                # CENTRAL caída: no tiene sentido esperar una respuesta.
                log(f"[FICHERO] Petición {i} no enviada")
            # El riego dura 'segundos', así que la espera máxima depende de él.
            elif not self.fin_peticion.wait(segundos + MARGEN_ESPERA):
                log(f"[AVISO] CENTRAL no ha respondido a la petición {req_id[:8]}: se pasa a la siguiente")
            with self.lock:
                self.esperando = None

            if i < len(peticiones):
                log(f"[FICHERO] Siguiente petición en {PAUSA_ENTRE_PETICIONES} s...")
                time.sleep(PAUSA_ENTRE_PETICIONES)
        log("[FICHERO] Fichero terminado. Paso al menú manual")

    # ----------------------------- Modo manual -----------------------------

    def ver_estaciones(self) -> None:
        with self.lock:
            estaciones = list(self.estaciones)
        if not estaciones:
            log("Aún no hay estado de la red: CENTRAL lo publica cada 5 s")
            return
        log(f"{'ESTACIÓN':<9} {'ESTADO':<16} UBICACIÓN")
        for e in estaciones:
            marca = "  <- se puede pedir riego" if e["estado"] == "DISPONIBLE" else ""
            log(f"{e['ws_id']:<9} {e['estado']:<16} {e['ubicacion']}{marca}")

    def pedir_riego_teclado(self) -> None:
        texto = input("Estación y segundos (ejemplo WS-04#30): ")
        try:
            ws_id, segundos = leer_peticion(texto)   # mismo formato que el fichero
        except ValueError as e:
            log(f"Petición no válida: {e}")
            return
        self.pedir_riego(ws_id, segundos, str(uuid.uuid4()))

    def bucle_teclado(self) -> None:
        while True:
            log(f"--- WM_FO · {self.fo_id} ---")
            log("    1 = ver estaciones   2 = pedir riego   q = salir   (y Enter)")
            opcion = input().strip().lower()
            if opcion == "1":
                self.ver_estaciones()
            elif opcion == "2":
                self.pedir_riego_teclado()
            elif opcion == "q":
                return
            elif opcion:
                log(f"Opción desconocida: {opcion}")


def main() -> None:
    parser = argparse.ArgumentParser(description="WM_FO: aplicación del operario de campo")
    parser.add_argument("broker", type=ip_puerto, help="IP y puerto del broker Kafka (ip:puerto)")
    parser.add_argument("fo_id", type=id_fo, help="ID del operario, por ejemplo FO-12")
    parser.add_argument("--fichero", help="fichero de peticiones, una por línea: WS-04#30")
    args = parser.parse_args()

    broker = f"{args.broker[0]}:{args.broker[1]}"
    fo = FO(args.fo_id, broker)
    log(f"=== WM_FO · {args.fo_id} · broker Kafka {broker} ===")

    hilo = threading.Thread(target=fo.hilo_kafka, daemon=True)
    hilo.start()
    try:
        log("Conectando con Kafka...")
        if not fo.listo.wait(ESPERA_KAFKA):
            log(f"[ERROR] Kafka no responde en {broker}. ¿Está arrancado?")
            return
        log("[OK] Escuchando notificaciones y estado de la red")
        fo.vigia.arrancar(fo.saliendo)
        # CENTRAL publica el estado cada 5 s: si no llega ninguno, se avisa ya.
        limite = time.monotonic() + ESPERA_ESTADO
        while fo.vigia.ultimo is None and time.monotonic() < limite:
            time.sleep(0.2)
        if fo.vigia.ultimo is None:
            log("[AVISO] Aún no llega el estado de la red: ¿está WM_Central arrancada? "
                "Si sigue sin llegar, se avisará y no se enviarán peticiones")
        if args.fichero:
            fo.modo_fichero(args.fichero)
        fo.bucle_teclado()
    except (KeyboardInterrupt, EOFError):
        pass
    finally:
        log("Cerrando el FO...")
        fo.saliendo.set()
        hilo.join(5)           # poll() vuelve en ≤1 s y luego se cierra el consumidor


if __name__ == "__main__":
    main()
