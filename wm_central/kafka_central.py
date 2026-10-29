"""
Parte Kafka de CENTRAL: peticiones de riego, órdenes a las WS, telemetría,
avisos a los operarios y estado de la red (contrato, apartado 2).

Hilos:
  - consumidor de wm.requests y wm.ws.telemetry (group "wm-central")
  - publicador de ESTADO_RED cada 5 s
  - temporizadores de 5 s de las reservas (threading.Timer)

Un único Producer compartido (es seguro usarlo desde varios hilos) y un
único Consumer para los dos topics, que solo usa su hilo (un Consumer NO se
puede compartir entre hilos).

Flujo de una petición (flujo B del contrato):
  SOLICITUD_RIEGO -> PASO -> ACEPTADA + START -> STARTED -> PASO "Riego
  iniciado" -> TELEMETRIA cada segundo (se reenvía al FO) -> FINISHED ->
  guardar en BD + RESUMEN al FO.
"""

from __future__ import annotations      # "X | None" en Python 3.9

import threading
import uuid
from datetime import datetime, timezone

from wm_central import estados as e
from common import kafka_utils as ku
from common import mensajes as m

GROUP_CENTRAL = "wm-central"
SEG_MAX_PETICION = 30      # peticiones más antiguas se descartan
SEG_RESERVA = 5            # tiempo máximo esperando STARTED
SEG_ESTADO_RED = 5         # ESTADO_RED periódico
SEG_SESION = 10            # session.timeout.ms del consumidor (ver _bucle_consumidor)


def antiguedad_s(ts: str) -> float:
    """Segundos que han pasado desde ts (ISO 8601).

    Si ts no trae zona horaria (por ejemplo, un mensaje escrito a mano en
    la consola), se supone UTC, que es lo que pone common.mensajes.ahora().
    """
    fecha = datetime.fromisoformat(ts)
    if fecha.tzinfo is None:
        fecha = fecha.replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - fecha).total_seconds()


class KafkaCentral:
    def __init__(self, central, broker: str, producer=None,
                 seg_reserva: float = SEG_RESERVA):
        self.central = central
        self.broker = broker
        # producer se puede pasar desde fuera para los tests (uno falso).
        self.producer = producer or ku.crear_producer(broker)
        self.seg_reserva = seg_reserva
        self.timers = {}                     # req_id -> threading.Timer
        self.lock_timers = threading.Lock()
        # req_id ya procesados. Kafka entrega "al menos una vez": si nos
        # llegara la misma petición dos veces, la segunda se ignora.
        self.procesadas = set()
        self.lock_procesadas = threading.Lock()

    # ------------------------------------------------------------ arranque
    def arrancar(self) -> None:
        # Antes de suscribirse: que existan los 5 topics (ver crear_topics).
        try:
            creados = ku.crear_topics(self.broker, m.TOPICS)
            if creados:
                self.central.log(f"Kafka: topics creados {creados}")
        except Exception as ex:
            # Sin topics CENTRAL funciona a medias, pero no se cae: se avisa
            # y se pueden crear a mano (scripts/crear_topics.sh).
            self.central.log(f"AVISO Kafka: no se han podido comprobar los topics ({ex})")
        self.parar = threading.Event()       # se activa en detener()
        self.hilo_consumidor = threading.Thread(target=self._bucle_consumidor,
                                                daemon=True)
        self.hilo_consumidor.start()
        threading.Thread(target=self._bucle_estado_red, daemon=True).start()
        self.central.log(f"Kafka: conectado a {self.broker}")

    def detener(self) -> None:
        """Cierre ordenado (Ctrl+C, o docker stop / Railway al redesplegar).

        consumer.close() avisa al bróker de que CENTRAL sale del grupo
        "wm-central". Sin él, Kafka la sigue dando por viva unos 45 s
        (session.timeout.ms) y, si CENTRAL se reinicia antes, el consumidor
        nuevo se queda esperando el reparto de particiones y las peticiones
        de ese rato no se atienden.
        """
        self.parar.set()
        self.hilo_consumidor.join(timeout=10)
        self.producer.flush(5)               # que salgan los avisos pendientes

    def _bucle_consumidor(self):
        # UN solo consumidor para los dos topics que lee CENTRAL. Antes había
        # dos consumidores (uno por topic) en el mismo grupo: cada uno que
        # entraba o salía obligaba a Kafka a repartir de nuevo las particiones
        # de los dos, y mientras tanto nadie leía. Rubén lo vio una vez: CENTRAL
        # tardó ~100 s en atender peticiones. Con uno solo, el reparto es uno.
        # group.id fijo: si CENTRAL se reinicia, sigue desde el último offset
        # confirmado (commit automático). Las peticiones viejas se descartan
        # por su ts (más de 30 s).
        #
        # session.timeout.ms: cuánto tarda Kafka en dar por muerto a un miembro
        # del grupo que deja de dar señales (45 s por defecto). Si CENTRAL se
        # cae sin cerrar (se cuelga, se corta la red...) y vuelve a arrancar,
        # el consumidor nuevo no recibe particiones hasta que Kafka echa al
        # viejo. Con 10 s, CENTRAL vuelve a atender peticiones mucho antes.
        procesar = {m.T_REQUESTS: self.procesar_solicitud,
                    m.T_TELEMETRY: self.procesar_telemetria}
        consumer = ku.crear_consumer(self.broker, GROUP_CENTRAL, list(procesar),
                                     extra={"session.timeout.ms": SEG_SESION * 1000})
        try:
            while not self.parar.is_set():
                msg = consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    self.central.log(f"Kafka ({msg.topic()}): {msg.error()}")
                    continue
                try:
                    procesar[msg.topic()](ku.leer(msg))
                except Exception as ex:
                    # Un mensaje mal formado (por ejemplo, escrito a mano en la
                    # consola) no debe tirar el hilo: se apunta y se sigue.
                    self.central.log(f"Mensaje ignorado en {msg.topic()}: {ex!r}")
        finally:
            consumer.close()

    def _bucle_estado_red(self):
        # Cada 5 s, para que un FO recién arrancado (con "latest") vea la
        # red enseguida aunque no cambie nada.
        while not self.parar.wait(SEG_ESTADO_RED):
            try:
                self.publicar_estado()
            except Exception as ex:
                self.central.log(f"ERROR Kafka: {ex!r}")

    # -------------------------------------------------------- envíos
    def _enviar(self, topic, key, mensaje):
        # esperar=False: no bloquear los hilos de los sockets si Kafka tarda.
        ku.enviar(self.producer, topic, key, mensaje, esperar=False)

    def _notificar(self, fo_id, mensaje):
        """Aviso al operario. Si la petición vino de la WS (fo_id None), nada."""
        if fo_id:
            self._enviar(m.T_NOTIFICATIONS, fo_id, mensaje)

    def publicar_estado(self):
        lista = self.central.red.lista_estado()
        self._enviar(m.T_STATUS, m.KEY_RED, m.estado_red(lista))

    # ------------------------------------------------- wm.requests
    def procesar_solicitud(self, d: dict):
        if d.get("tipo") != "SOLICITUD_RIEGO":
            return
        req_id = d["req_id"]
        with self.lock_procesadas:
            if req_id in self.procesadas:
                return                       # duplicado
            self.procesadas.add(req_id)
        edad = antiguedad_s(d["ts"])
        if edad > SEG_MAX_PETICION:
            self.central.log(f"Petición {req_id[:8]} descartada: tiene {edad:.0f} s")
            return
        self.solicitar(req_id, d["ws_id"], d["duracion_s"],
                       d.get("fo_id"), d.get("origen", "FO"))

    def solicitar(self, req_id, ws_id, duracion_s, fo_id=None, origen="CENTRAL"):
        """Lógica de una petición de riego. La usan los mensajes de
        wm.requests y, más adelante, el menú de CENTRAL ("iniciar riego")."""
        quien = fo_id or origen
        self.central.log(f"Petición de {quien}: {ws_id} durante {duracion_s} s")
        self._notificar(fo_id, m.paso(req_id, fo_id, ws_id,
                                      f"Comprobando estado de {ws_id}…"))

        def denegar(motivo):
            self.central.log(f"Petición de {quien} para {ws_id} DENEGADA: {motivo}")
            self._notificar(fo_id, m.denegada(req_id, fo_id, ws_id, motivo))

        est = self.central.red.get(ws_id)
        if est is None:
            return denegar(f"La estación {ws_id} no existe")
        if origen == "FO" and not self.central.bd.existe_operario(fo_id):
            return denegar(f"Operario {fo_id} no registrado")
        if not isinstance(duracion_s, int) or duracion_s <= 0:
            return denegar("Duración no válida")

        # Comprobar y reservar con el lock cogido: el motivo que leemos es
        # exactamente el que ha hecho que reservar() deniegue.
        with est.lock:
            _, motivo = est.puede_regar()
            acciones = est.reservar(req_id, fo_id, origen, duracion_s)
        if e.ENVIAR_START in acciones:
            self.central.log(f"{ws_id}: RESERVADA para {quien}, START enviado")
        datos = {"req_id": req_id, "ws_id": ws_id, "fo_id": fo_id,
                 "origen": origen, "duracion_s": duracion_s}
        self.central.tras_evento(est, acciones, datos, motivo)

    # ----------------------------------------------- wm.ws.telemetry
    def procesar_telemetria(self, d: dict):
        tipo = d.get("tipo")
        est = self.central.red.get(d.get("ws_id"))
        if est is None:
            return
        req_id = d.get("req_id")

        if tipo == "STARTED":
            acciones = self.central.aplicar(
                est, est.valvula_abierta, req_id,
                motivo="Riego no autorizado por CENTRAL")
            if e.CANCELAR_TIMER in acciones:
                fo_id = est.fo_actual
                self.central.log(f"{est.ws_id}: válvula abierta, REGANDO")
                self._notificar(fo_id, m.paso(req_id, fo_id, est.ws_id,
                                              f"Riego iniciado en {est.ws_id}"))

        elif tipo == "TELEMETRIA":
            if est.telemetria(req_id, d["caudal_lpm"], d["volumen_l"], d["segundos"]):
                fo_id = est.fo_actual
                self._notificar(fo_id, m.telemetria_fo(
                    req_id, fo_id, est.ws_id, d["caudal_lpm"], d["volumen_l"]))

        elif tipo == "FINISHED":
            self.central.aplicar(est, est.fin_riego, req_id, motivo=d["motivo"],
                                 volumen_total_l=d["volumen_total_l"],
                                 duracion_s=d["duracion_s"])

        elif tipo == "START_REJECTED":
            self.central.aplicar(
                est, est.start_rechazado, req_id,
                motivo=f"La estación ha rechazado el riego ({d.get('motivo')})")

    # ----------------------------------- acciones de la máquina de estados
    def ejecutar(self, est, acciones, datos, motivo,
                 volumen_total_l=None, duracion_s=None):
        """Convierte las acciones que devuelve estados.py en mensajes Kafka.

        datos: copia del riego afectado (req_id, fo_id...), tomada antes
        del evento. motivo: texto para DENEGADA, STOP o RESUMEN.
        """
        ws_id = est.ws_id
        req_id = datos["req_id"] if datos else None
        fo_id = datos.get("fo_id") if datos else None

        for accion in acciones:
            if accion == e.ENVIAR_ACEPTADA:
                self._notificar(fo_id, m.aceptada(req_id, fo_id, ws_id))
            elif accion == e.ENVIAR_START:
                self._enviar(m.T_COMMANDS, ws_id,
                             m.start(req_id, ws_id, fo_id, datos["duracion_s"]))
            elif accion == e.ARRANCAR_TIMER:
                self._arrancar_timer(est, req_id)
            elif accion == e.CANCELAR_TIMER:
                self._cancelar_timer(req_id)
            elif accion == e.ENVIAR_DENEGADA:
                self.central.log(f"{ws_id}: petición DENEGADA ({motivo})")
                self._notificar(fo_id, m.denegada(req_id, fo_id, ws_id, motivo))
            elif accion == e.ENVIAR_STOP:
                self._enviar(m.T_COMMANDS, ws_id, m.stop(ws_id, motivo))
            elif accion == e.ENVIAR_BLOCK:
                self._enviar(m.T_COMMANDS, ws_id, m.block(ws_id))
            elif accion == e.ENVIAR_ACTIVATE:
                self._enviar(m.T_COMMANDS, ws_id, m.activate(ws_id))
            elif accion == e.CERRAR_RIEGO:
                self._cerrar_riego(datos, motivo, volumen_total_l, duracion_s)

    def _cerrar_riego(self, datos, motivo, volumen_total_l, duracion_s):
        """Guarda el riego en BD y manda el RESUMEN al operario.

        Si lo cierra un FINISHED, los totales vienen del Engine. Si lo
        cierra CENTRAL (fuga, bloqueo, desconexión), usamos la última
        telemetría recibida.
        """
        if volumen_total_l is None:
            volumen_total_l = datos["volumen"]
        if duracion_s is None:
            duracion_s = datos["segundos"]
        volumen_total_l = round(volumen_total_l, 2)
        self.central.bd.guardar_riego(
            datos["req_id"], datos["ws_id"], datos["fo_id"], datos["origen"],
            datos["inicio"], m.ahora(), duracion_s, volumen_total_l, motivo)
        self.central.log(f"{datos['ws_id']}: riego terminado ({motivo}), "
                         f"{volumen_total_l} L en {duracion_s} s")
        self._notificar(datos["fo_id"], m.resumen(
            datos["req_id"], datos["fo_id"], datos["ws_id"], motivo,
            volumen_total_l, duracion_s))

    # --------------------------------------------------- temporizadores
    def _arrancar_timer(self, est, req_id):
        t = threading.Timer(self.seg_reserva, self._fin_timer, args=(est, req_id))
        t.daemon = True
        with self.lock_timers:
            self.timers[req_id] = t
        t.start()

    def _cancelar_timer(self, req_id):
        with self.lock_timers:
            t = self.timers.pop(req_id, None)
        if t is not None:
            t.cancel()

    def _fin_timer(self, est, req_id):
        """Han pasado 5 s y no ha llegado STARTED.

        Si mientras tanto llegó el STARTED, timeout_reserva() no hace nada
        (comprueba el req_id y que siga reservada).
        """
        with self.lock_timers:
            self.timers.pop(req_id, None)
        self.central.aplicar(est, est.timeout_reserva, req_id,
                             motivo=f"{est.ws_id} no ha confirmado el inicio en "
                                    f"{self.seg_reserva:g} s")

    # ------------------------------------- órdenes del operador de CENTRAL
    # Punto 11 del enunciado: iniciar un riego, bloquear o activar, para una
    # WS o para todas. Las llama el panel web (wm_central/panel_web.py).
    # "Todas" = un mensaje por estación (contrato).
    def iniciar_riego(self, ws_id, duracion_s) -> None:
        """Riego pedido desde CENTRAL: mismo camino que el de un operario,
        pero con origen "CENTRAL" y sin fo_id (no hay operario a quien avisar).
        """
        self.solicitar(str(uuid.uuid4()), ws_id, duracion_s, origen="CENTRAL")

    def iniciar_todas(self, duracion_s) -> None:
        # Las que no estén DISPONIBLE se deniegan y queda en el log por qué.
        for est in self.central.red.todas():
            self.iniciar_riego(est.ws_id, duracion_s)

    def bloquear_ws(self, ws_id) -> bool:
        est = self.central.red.get(ws_id)
        if est is None:
            return False
        self.central.log(f"{ws_id}: bloqueada por CENTRAL (FUERA DE SERVICIO)")
        self.central.aplicar(est, est.bloquear, motivo="BLOQUEO")
        return True

    def activar_ws(self, ws_id) -> bool:
        est = self.central.red.get(ws_id)
        if est is None:
            return False
        self.central.log(f"{ws_id}: activada por CENTRAL")
        self.central.aplicar(est, est.activar)
        return True

    def bloquear_todas(self):
        for est in self.central.red.todas():     # un mensaje por estación
            self.bloquear_ws(est.ws_id)

    def activar_todas(self):
        for est in self.central.red.todas():
            self.activar_ws(est.ws_id)
