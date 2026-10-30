"""
WM_WS_E · Engine de una Watering Station (WS).

Uso (siempre desde la raíz del repositorio):
    python -m wm_ws.WS_E.WM_WS_E <ip_broker:puerto> <ip_monitor:puerto> [--estado CARPETA]
    python -m wm_ws.WS_E.WM_WS_E 127.0.0.1:9092 127.0.0.1:9100

Orden de arranque: primero el Monitor (WM_WS_M) y después el Engine.

Qué hace:
  Sockets con el Monitor (R3):
    1. Se conecta:  ENQ -> ACK,  HELLO -> ID#WS-04. El Engine recibe su ID del Monitor.
    2. Contesta a cada HEALTH con OK#HEALTH o KO#HEALTH.
  Kafka (R4):
    3. Escucha las órdenes de CENTRAL en wm.ws.commands (solo las de su ID):
       START, STOP, BLOCK y ACTIVATE.
    4. Con START abre la válvula: publica STARTED, una TELEMETRIA por segundo y,
       al acabar, FINISHED. Si no puede regar, publica START_REJECTED.
  Resiliencia (guía de corrección):
    5. Sin Monitor la estación queda AVERIADA: corta el riego en curso y no
       acepta riegos (ni de CENTRAL ni manuales) hasta que el Monitor vuelva.
    6. Mientras riega, guarda el riego en disco cada segundo (riego_WS-04.json
       en --estado). Si el Engine se cae, al volver a arrancar envía el estado
       final de ese riego a CENTRAL: lo regado no se pierde.
    7. Lee también wm.ws.status: si CENTRAL deja de publicar el estado de la
       red, avisa "Imposible conectar con WM_Central".
    8. Si CENTRAL deniega un riego pedido desde la propia WS, lo muestra.
  Menú de teclado:
    k = simular/reparar avería (si está regando, cierra la válvula: FINISHED con FUGA)
    p = parar el riego (FINISHED con MANUAL)
    r = pedir un riego desde la propia estación (SOLICITUD_RIEGO con origen "WS")
    q = salir

Hilos:
  - Principal: menú de teclado con input().
  - hilo_salud: socket con el Monitor.
  - hilo_ordenes: consumidor Kafka de wm.ws.commands.
  - hilo_riego: uno por riego, solo mientras dura.
El estado compartido (estado_ok, bloqueada, riego actual) lo tocan varios hilos:
todo acceso va dentro de self.lock.
Un Producer de Kafka se puede compartir entre hilos; un Consumer no (por eso solo
lo usa hilo_ordenes, que es quien lo crea).
"""
import argparse
import json
import os
import random
import socket
import threading
import time
import uuid

from confluent_kafka import OFFSET_END

from common import kafka_utils as ku
from common import mensajes as m
from common.protocol import ACK, ENQ, enviar_trama, leer_byte, peticion, recibir_trama
from common.utilidades import VigiaCentral, ip_puerto, log   # mismas utilidades que el Monitor

TIMEOUT_MONITOR = 5          # s sin recibir HEALTH (llega cada 1 s) = Monitor perdido
ESPERA_RECONEXION = 3        # s entre intentos de conexión con el Monitor
CAUDAL_MIN, CAUDAL_MAX = 9.0, 12.0   # L/min que "mide" el caudalímetro simulado


def saltar_al_final(consumer, particiones) -> None:
    """Kafka llama a esta función cuando asigna particiones al consumidor.
    Se pide leer desde el final (OFFSET_END): solo las órdenes que lleguen a partir
    de ahora, ignorando las antiguas aunque el grupo tuviera una posición guardada."""
    for p in particiones:
        p.offset = OFFSET_END
    consumer.assign(particiones)


class Riego:
    """Datos del riego en curso. Solo puede haber uno a la vez."""

    def __init__(self, req_id: str, duracion_s: int, fo_id=None, origen=None):
        self.req_id = req_id
        self.duracion_s = duracion_s
        # Vienen en el START y se repiten en TELEMETRIA y FINISHED: así una
        # CENTRAL recién reiniciada sabe de quién es el riego (ver mensajes.py).
        self.fo_id = fo_id
        self.origen = origen
        # Se activa para cortar el riego antes de tiempo. Es un Event y no un bool
        # porque el hilo de riego puede ESPERARLO: parar.wait(1) duerme 1 s, pero
        # se despierta en el acto si alguien lo activa. Así la válvula se cierra
        # al momento y no al acabar el segundo.
        self.parar = threading.Event()
        self.motivo = None                  # por qué se cortó: MANUAL, FUGA, BLOQUEO...
        self.terminado = threading.Event()  # se activa cuando ya se ha publicado FINISHED


class Engine:
    def __init__(self, dir_monitor: tuple, broker: str, dir_estado: str = ".",
                 producer=None):
        self.dir_monitor = dir_monitor
        self.broker = broker                # "ip:puerto", formato que pide Kafka
        self.ws_id = None                   # lo asigna el Monitor en el HELLO
        # producer se puede pasar desde fuera para los tests (uno falso).
        self.producer = producer or ku.crear_producer(broker)
        self.dir_estado = dir_estado        # carpeta del fichero del riego en curso
        self.vigia = VigiaCentral()         # ¿CENTRAL sigue publicando el estado?

        self.lock = threading.Lock()
        self.estado_ok = True               # False = avería simulada con 'k'
        self.bloqueada = False              # True tras un BLOCK de CENTRAL
        self.riego = None                   # Riego en curso, o None si la válvula está cerrada
        # False mientras no hay Monitor: la estación está AVERIADA (guía de
        # corrección, caso "cae el Monitor"). Sin Monitor nadie vigila la salud
        # del Engine ni avisa a CENTRAL, así que no es seguro regar.
        self.monitor_ok = True
        self.saliendo = threading.Event()   # avisa a hilo_ordenes de que hay que terminar

    # ----------------------------- Monitor (sockets) -----------------------------

    def conectar_monitor(self) -> socket.socket:
        """Conecta con el Monitor y obtiene el ID. Reintenta hasta conseguirlo."""
        while True:
            sock = None
            try:
                # El timeout vale para todo el socket: si el Monitor deja de mandar
                # HEALTH durante 5 s, recv() lanza socket.timeout y reconectamos.
                sock = socket.create_connection(self.dir_monitor, timeout=TIMEOUT_MONITOR)
                sock.sendall(ENQ)
                if leer_byte(sock) != ACK:
                    raise ConnectionError("el Monitor no respondió ACK al ENQ")

                campos = peticion(sock, "HELLO")            # -> ["ID", "WS-04"]
                if campos[0] != "ID" or len(campos) < 2:
                    raise ConnectionError(f"respuesta inesperada al HELLO: {'#'.join(campos)}")
                if self.ws_id is not None and campos[1] != self.ws_id:
                    # Un Engine pertenece a una sola WS: no puede cambiar de ID a mitad.
                    raise ConnectionError(f"el Monitor es de {campos[1]}, no de {self.ws_id}")

                self.ws_id = campos[1]
                log(f"[OK] Conectado al Monitor. Soy {self.ws_id}")
                return sock
            except OSError as e:
                if sock:
                    sock.close()
                ip, puerto = self.dir_monitor
                log(f"[AVISO] No se puede conectar con el Monitor en {ip}:{puerto} ({e})")
            log(f"Reintento en {ESPERA_RECONEXION} s...")
            time.sleep(ESPERA_RECONEXION)

    def hilo_salud(self, sock: socket.socket) -> None:
        """Contesta a los HEALTH del Monitor. Si lo pierde, reconecta."""
        while True:
            try:
                while True:
                    texto = recibir_trama(sock)
                    if texto != "HEALTH":
                        log(f"[AVISO] Mensaje desconocido del Monitor: {texto}")
                        continue
                    # Se lee el estado con el lock y se suelta ANTES de enviar:
                    # enviar puede tardar (red lenta) y no queremos bloquear a nadie.
                    with self.lock:
                        ok = self.estado_ok
                    enviar_trama(sock, "OK#HEALTH" if ok else "KO#HEALTH")
            except OSError as e:
                log(f"[AVISO] Monitor perdido ({e})")
                sock.close()
                self.monitor_perdido()
            sock = self.conectar_monitor()
            self.monitor_recuperado()

    def monitor_perdido(self) -> None:
        """Sin Monitor: AVERIADA. Se corta el riego (lo pide la guía: "si
        hubiera un riego en marcha este finalizará") y se rechaza todo riego
        nuevo hasta que vuelva."""
        with self.lock:
            self.monitor_ok = False
            regaba = self._parar_riego("DESCONEXION")
        log("[AVERIADA] Sin conexión con el Monitor: la estación no admite riegos")
        if regaba:
            log("[AVERIADA] Cerrando la válvula del riego en curso")

    def monitor_recuperado(self) -> None:
        with self.lock:
            self.monitor_ok = True
        log("[OK] Monitor recuperado: la estación vuelve a admitir riegos")

    # ----------------------------- Kafka: publicar -----------------------------

    def publicar(self, topic: str, mensaje: dict) -> None:
        """Todo lo que publica el Engine lleva como clave su ID.
        Misma clave = misma partición = Kafka respeta el orden: CENTRAL nunca
        recibirá un FINISHED antes que el STARTED del mismo riego."""
        ku.enviar(self.producer, topic, self.ws_id, mensaje)

    # ----------------------------- Riego -----------------------------

    def empezar_riego(self, req_id: str, duracion_s: int, fo_id=None, origen=None) -> str:
        """Abre la válvula si se puede. Devuelve "" si empieza, o el motivo de rechazo.

        Comprobar y reservar se hace en el MISMO bloque con lock: si no, entre
        "no está regando" y "ahora riego" podría colarse otro hilo (por ejemplo,
        alguien pulsa 'k' justo en medio) y regaríamos estando en KO.
        """
        with self.lock:
            if not self.monitor_ok:
                return "AVERIADA"
            if not self.estado_ok:
                return "ENGINE_KO"
            if self.bloqueada:
                return "BLOQUEADA"
            if self.riego is not None:
                return "YA_REGANDO"
            riego = self.riego = Riego(req_id, duracion_s, fo_id, origen)
        threading.Thread(target=self.hilo_riego, args=(riego,), daemon=True).start()
        return ""

    def _parar_riego(self, motivo: str) -> bool:
        """Pide al hilo de riego que cierre la válvula. Hay que llamarla con self.lock cogido.

        Devuelve False si no se estaba regando. No publica FINISHED: lo hace el
        propio hilo de riego al salir. Así hay siempre UN único FINISHED por riego
        y sale después de la última TELEMETRIA.
        """
        riego = self.riego
        if riego is None or riego.parar.is_set():
            return False
        riego.motivo = motivo
        riego.parar.set()
        return True

    def parar_riego(self, motivo: str) -> bool:
        with self.lock:
            return self._parar_riego(motivo)

    def hilo_riego(self, riego: Riego) -> None:
        """Simula el riego: válvula abierta, caudalímetro y telemetría cada segundo."""
        self.guardar_riego(riego, 0.0, 0)  # en disco desde el primer momento
        self.publicar(m.T_TELEMETRY, m.started(riego.req_id, self.ws_id))
        log(f"[RIEGO] Válvula ABIERTA durante {riego.duracion_s} s (petición {riego.req_id})")

        volumen = 0.0
        segundos = 0
        # parar.wait(1) espera 1 s y devuelve True si nos han mandado parar.
        while segundos < riego.duracion_s and not riego.parar.wait(1):
            segundos += 1
            caudal = round(random.uniform(CAUDAL_MIN, CAUDAL_MAX), 1)
            volumen += caudal / 60          # L/min durante 1 s = caudal/60 litros
            # Primero a disco y después a Kafka: si el Engine muere justo
            # aquí, lo que está en disco es lo que ya se ha regado.
            self.guardar_riego(riego, volumen, segundos)
            self.publicar(m.T_TELEMETRY, m.telemetria(
                riego.req_id, self.ws_id, caudal, round(volumen, 2), segundos,
                riego.fo_id, riego.origen))
            log(f"[RIEGO] {segundos:>3}/{riego.duracion_s} s · caudal {caudal:4.1f} L/min"
                f" · volumen {volumen:6.2f} L")

        with self.lock:
            riego.parar.set()               # desde aquí, _parar_riego() ya no cambia el motivo
            motivo = riego.motivo or "TIEMPO"
        self.publicar(m.T_TELEMETRY, m.finished(
            riego.req_id, self.ws_id, motivo, round(volumen, 2), segundos,
            riego.fo_id, riego.origen))
        # FINISHED ya ha salido (enviar() espera la confirmación de Kafka):
        # el fichero ya no hace falta.
        self.borrar_riego_guardado()
        # Se libera la válvula DESPUÉS de publicar FINISHED: así el STARTED de un
        # riego nuevo nunca puede salir antes que el FINISHED de este.
        with self.lock:
            self.riego = None
        riego.terminado.set()
        log(f"[RIEGO] Válvula CERRADA · motivo {motivo} · {volumen:.2f} L en {segundos} s")

    # ------------------------- Riego guardado en disco -------------------------
    # Guía de corrección, caso "cae el Engine": "La WS no puede perder lo que
    # ya se haya regado y, cuando se recupere, debe ser capaz de enviar el
    # estado final del riego realizado". Lo que está en memoria se pierde al
    # caerse el programa; lo que está en disco, no.

    def ruta_riego(self) -> str:
        return os.path.join(self.dir_estado, f"riego_{self.ws_id}.json")

    def guardar_riego(self, riego: Riego, volumen: float, segundos: int) -> None:
        datos = {"req_id": riego.req_id, "ws_id": self.ws_id, "fo_id": riego.fo_id,
                 "origen": riego.origen, "duracion_s": riego.duracion_s,
                 "volumen_l": round(volumen, 2), "segundos": segundos}
        ruta = self.ruta_riego()
        temporal = ruta + ".tmp"
        try:
            # Se escribe en un fichero aparte y luego se renombra: os.replace
            # cambia uno por otro de golpe. Si el Engine muriera a mitad de
            # escribir, el fichero bueno anterior seguiría intacto.
            with open(temporal, "w", encoding="utf-8") as f:
                json.dump(datos, f)
            os.replace(temporal, ruta)
        except OSError as e:
            log(f"[AVISO] No se puede guardar el riego en {ruta} ({e})")

    def borrar_riego_guardado(self) -> None:
        try:
            os.remove(self.ruta_riego())
        except FileNotFoundError:
            pass
        except OSError as e:
            log(f"[AVISO] No se puede borrar {self.ruta_riego()} ({e})")

    def recuperar_riego_pendiente(self) -> "dict | None":
        """Al arrancar: si quedó un riego a medias (el Engine se cayó regando),
        envía su FINISHED con lo que se llegó a regar. Motivo FUGA: un Engine
        caído es una avería (tabla Monitor_OK y Engine_KO de la guía).
        Devuelve los datos recuperados (o None si no había nada)."""
        ruta = self.ruta_riego()
        try:
            with open(ruta, encoding="utf-8") as f:
                datos = json.load(f)
        except FileNotFoundError:
            return None
        except (OSError, ValueError) as e:
            log(f"[AVISO] Fichero de riego {ruta} ilegible, se descarta ({e})")
            self.borrar_riego_guardado()
            return None
        self.publicar(m.T_TELEMETRY, m.finished(
            datos["req_id"], self.ws_id, "FUGA", datos["volumen_l"], datos["segundos"],
            datos.get("fo_id"), datos.get("origen")))
        self.borrar_riego_guardado()
        log(f"[RECUPERADO] Riego {datos['req_id'][:8]} interrumpido por la caída del Engine: "
            f"{datos['volumen_l']:.2f} L en {datos['segundos']} s. Estado final enviado a CENTRAL")
        return datos

    # ----------------------------- Kafka: órdenes -----------------------------

    def hilo_ordenes(self) -> None:
        """Consume wm.ws.commands. group.id propio (wm-engine-WS-04): cada Engine
        recibe TODOS los mensajes del topic y se queda solo con los de su clave."""
        # Dos topics con un solo consumidor: las órdenes (wm.ws.commands) y el
        # estado de la red (wm.ws.status), que solo se usa para saber si
        # CENTRAL sigue viva (VigiaCentral).
        topics = [m.T_COMMANDS, m.T_STATUS]
        consumer = ku.crear_consumer(self.broker, f"wm-engine-{self.ws_id}", topics)
        # "latest" solo se aplica si el grupo NO tiene una posición guardada, pero
        # Kafka guarda la posición sola (auto commit). Al reiniciar el Engine,
        # seguiría donde lo dejó y ejecutaría los START que llegaron mientras
        # estaba apagado (comprobado en las pruebas). Por eso se vuelve a suscribir
        # con on_assign: cuando Kafka le da sus particiones, salta al final.
        consumer.subscribe(topics, on_assign=saltar_al_final)
        try:
            self._bucle_ordenes(consumer)
        finally:
            # close() avisa a Kafka de que dejamos el grupo. Sin él, Kafka nos
            # sigue contando como miembro unos 45 s, y si el Engine se reinicia en
            # ese tiempo no recibe particiones (ni órdenes) hasta que caducamos.
            consumer.close()

    def _bucle_ordenes(self, consumer) -> None:
        avisado = False
        while not self.saliendo.is_set():
            msg = consumer.poll(1.0)        # espera hasta 1 s a que llegue algo
            if not avisado and consumer.assignment():
                # Con "latest", lo enviado antes de este momento no nos llega.
                log("[OK] Escuchando órdenes de CENTRAL en wm.ws.commands")
                avisado = True
            if msg is None:
                continue
            if msg.error():
                log(f"[AVISO] Kafka: {msg.error()}")
                continue
            if msg.topic() == m.T_STATUS:
                if ku.es_para_mi(msg, m.KEY_RED):
                    self.vigia.latido()         # CENTRAL está viva
                continue
            if not ku.es_para_mi(msg, self.ws_id):
                continue
            try:
                self.atender_orden(ku.leer(msg))
            except (ValueError, KeyError, TypeError) as e:
                # JSON roto o sin un campo (p. ej. escrito a mano en la consola):
                # se ignora ese mensaje en vez de dejar que muera el hilo.
                log(f"[AVISO] Orden mal formada, se ignora ({e!r})")

    def atender_orden(self, orden: dict) -> None:
        tipo = orden["tipo"]
        if tipo == "START":
            req_id, duracion = orden["req_id"], orden["duracion_s"]
            if not isinstance(duracion, int) or duracion <= 0:
                motivo = "DURACION_INVALIDA"
            else:
                motivo = self.empezar_riego(req_id, duracion, orden.get("fo_id"),
                                            orden.get("origen"))
            if motivo:
                self.publicar(m.T_TELEMETRY, m.start_rejected(req_id, self.ws_id, motivo))
                log(f"[RECHAZADO] START {req_id}: {motivo}")

        elif tipo == "STOP":
            motivo = orden.get("motivo", "MANUAL")
            if self.parar_riego(motivo):
                log(f"[ORDEN] STOP de CENTRAL ({motivo}): cerrando la válvula")
            else:
                # Normal tras una fuga: el Engine ya cerró solo y CENTRAL manda STOP igualmente.
                log(f"[ORDEN] STOP de CENTRAL ({motivo}): no se estaba regando, se ignora")

        elif tipo == "BLOCK":
            with self.lock:
                self.bloqueada = True
                regaba = self._parar_riego("BLOQUEO")
            log("[ORDEN] BLOCK: estación FUERA DE SERVICIO" + (", riego cortado" if regaba else ""))

        elif tipo == "ACTIVATE":
            with self.lock:
                self.bloqueada = False
            log("[ORDEN] ACTIVATE: estación activada de nuevo")

        elif tipo == "DENEGADA":
            # Respuesta de CENTRAL a un riego pedido desde esta WS (opción 'r').
            log(f"[DENEGADA] CENTRAL rechaza el riego pedido desde la estación "
                f"(req {orden['req_id'][:8]}): {orden.get('motivo')}")

        else:
            log(f"[AVISO] Orden desconocida: {tipo}")

    # ----------------------------- Teclado -----------------------------

    def alternar_estado(self) -> None:
        with self.lock:
            self.estado_ok = not self.estado_ok
            ok = self.estado_ok
            # Seguridad: con una avería, la válvula se cierra en el acto (flujo C).
            # Dentro del mismo lock: nadie puede empezar un riego entre las dos cosas.
            regaba = not ok and self._parar_riego("FUGA")
        if ok:
            log("[OK] Avería reparada: el Monitor recibirá OK#HEALTH")
        else:
            log("[KO] Avería simulada: el Monitor recibirá KO#HEALTH y avisará de FUGA")
            if regaba:
                log("[KO] Cerrando la válvula por seguridad (FINISHED con motivo FUGA)")

    def pedir_riego_manual(self) -> None:
        """Opción 'r': la estación pide su propio riego. Pasa por CENTRAL igual que
        el de un operario (origen "WS", fo_id null); si lo acepta, llegará un START."""
        with self.lock:
            if not self.monitor_ok:
                problema = "la estación está AVERIADA (sin Monitor)"
            elif not self.estado_ok:
                problema = "la estación está en KO"
            elif self.bloqueada:
                problema = "la estación está fuera de servicio"
            elif self.riego is not None:
                problema = "ya está regando"
            else:
                problema = ""
        if not problema and not self.vigia.disponible():
            problema = "Imposible conectar con WM_Central"
        if problema:
            # Se avisa aquí porque, si CENTRAL lo denegara, no hay operario al que
            # notificar y no nos enteraríamos de nada.
            log(f"No se puede pedir riego: {problema}")
            return

        texto = input("Duración del riego en segundos: ").strip()
        if not texto.isdigit() or int(texto) <= 0:
            log("Duración no válida: escribe un número entero mayor que 0")
            return
        req_id = str(uuid.uuid4())
        self.publicar(m.T_REQUESTS, m.solicitud_riego(req_id, "WS", None, self.ws_id, int(texto)))
        log(f"[PETICIÓN] Riego de {texto} s enviado a CENTRAL (req {req_id}). Si lo acepta, llegará un START")

    def mostrar_menu(self) -> None:
        with self.lock:
            if not self.monitor_ok:
                salud = "AVERIADA (sin Monitor)"
            else:
                salud = "OK" if self.estado_ok else "KO (avería)"
            servicio = "FUERA DE SERVICIO" if self.bloqueada else "activa"
            valvula = "REGANDO" if self.riego else "válvula cerrada"
        central = "" if self.vigia.disponible() else " · SIN CENTRAL"
        log(f"--- WM_WS_E · {self.ws_id} · Salud: {salud} · {servicio} · {valvula}{central} ---")
        log("    k = avería on/off   p = parar riego   r = pedir riego   q = salir   (y Enter)")

    def bucle_teclado(self) -> None:
        """input() y no msvcrt: msvcrt solo existe en Windows; input() vale en
        Windows, Linux y Docker."""
        while True:
            self.mostrar_menu()
            opcion = input().strip().lower()
            if opcion == "k":
                self.alternar_estado()
            elif opcion == "p":
                if not self.parar_riego("MANUAL"):
                    log("No hay ningún riego en marcha")
            elif opcion == "r":
                self.pedir_riego_manual()
            elif opcion == "q":
                return
            elif opcion:
                log(f"Opción desconocida: {opcion}")

    # ----------------------------- Cierre -----------------------------

    def cerrar(self, hilo_ordenes) -> None:
        """Si se sale regando, primero se cierra la válvula y se espera a que salga
        FINISHED (si no, CENTRAL creería que la WS sigue regando). Después se
        cierra el consumidor de Kafka."""
        with self.lock:
            riego = self.riego
            self._parar_riego("MANUAL")
        if riego:
            log("Cerrando la válvula antes de salir...")
            riego.terminado.wait(10)
        self.saliendo.set()
        if hilo_ordenes is not None:
            hilo_ordenes.join(5)            # poll() vuelve en ≤1 s y luego close()


def main() -> None:
    parser = argparse.ArgumentParser(description="WM_WS_E: Engine de una Watering Station")
    parser.add_argument("broker", type=ip_puerto, help="IP y puerto del broker Kafka (ip:puerto)")
    parser.add_argument("monitor", type=ip_puerto, help="IP y puerto del Monitor de la WS (ip:puerto)")
    # En Docker: WM_ESTADO_DIR=/datos y un volumen montado ahí, para que el
    # fichero sobreviva aunque se borre el contenedor.
    parser.add_argument("--estado", default=os.environ.get("WM_ESTADO_DIR", "."),
                        help="carpeta donde guardar el riego en curso (por defecto la actual)")
    args = parser.parse_args()

    broker = f"{args.broker[0]}:{args.broker[1]}"
    engine = Engine(args.monitor, broker, args.estado)
    log(f"=== WM_WS_E · broker Kafka {broker} ===")
    hilo_ordenes = None
    try:
        # Primero el ID: sin él no sabemos qué órdenes de Kafka son nuestras.
        sock = engine.conectar_monitor()
        # Con el ID ya sabido: ¿quedó un riego a medias de una caída anterior?
        engine.recuperar_riego_pendiente()
        engine.vigia.arrancar(engine.saliendo)
        # daemon=True: estos hilos mueren cuando termina el principal.
        threading.Thread(target=engine.hilo_salud, args=(sock,), daemon=True).start()
        hilo_ordenes = threading.Thread(target=engine.hilo_ordenes, daemon=True)
        hilo_ordenes.start()
        try:
            engine.bucle_teclado()
        except EOFError:
            # Sin teclado (stdin cerrado, p. ej. Docker sin -it): el menú no sirve,
            # pero el Engine debe seguir contestando al Monitor y a CENTRAL.
            log("[AVISO] Sin teclado: el Engine sigue funcionando sin menú")
            while True:
                time.sleep(1)
    except KeyboardInterrupt:
        pass
    engine.cerrar(hilo_ordenes)
    # Al salir se cierra el socket y el Monitor lo detecta como 'Engine sin
    # respuesta' -> LEAK. Es lo correcto: sin Engine no se puede regar.
    log("Cerrando el Engine...")


if __name__ == "__main__":
    main()
