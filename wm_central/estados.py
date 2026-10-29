"""
Máquina de estados de las WS (contrato, apartado 1). Lógica pura: no usa
red, ni Kafka, ni BD. Por eso se puede probar entera con tests.

Idea clave: NO guardamos "el estado" en una variable. Guardamos 4 banderas
independientes (las mismas que en la BD) y el estado que se pinta se
CALCULA con una prioridad fija:

    DESCONECTADA > FUGA > FUERA_SERVICIO > REGANDO > RESERVADA > DISPONIBLE

Así las combinaciones salen solas: una WS bloqueada que tiene una fuga y se
repara vuelve a naranja (sigue bloqueada), no a verde.

Cada función de evento (fuga(), bloquear()...) cambia las banderas y
DEVUELVE una lista de acciones que CENTRAL tiene que hacer (enviar un STOP,
cerrar el riego...). Esta clase no envía nada: solo decide.

Concurrencia: a una misma estación le llegan eventos desde varios hilos
(su socket, Kafka, el watchdog). Cada Estacion tiene su propio lock. Es un
RLock (reentrante): el mismo hilo puede cogerlo varias veces. Lo necesitamos
porque CENTRAL hace "comprobar y reservar" con el lock cogido y, dentro,
reservar() vuelve a cogerlo. Con un Lock normal, el hilo se bloquearía a sí mismo.
"""

# Permite escribir anotaciones como "str | None" en Python 3.9: con esta línea
# Python no evalúa las anotaciones al cargar el módulo (las guarda como texto).
# Sin ella, en 3.9 da "TypeError: unsupported operand type(s) for |".
# Tiene que ir antes que cualquier otro import.
from __future__ import annotations

import threading
import time
from datetime import datetime, timezone

# Estados visibles (lo que se pinta en el panel y se publica en ESTADO_RED).
DESCONECTADA = "DESCONECTADA"
DISPONIBLE = "DISPONIBLE"
RESERVADA = "RESERVADA"
REGANDO = "REGANDO"
FUGA = "FUGA"
FUERA_SERVICIO = "FUERA_SERVICIO"

ORDEN_ESTADOS = [DISPONIBLE, REGANDO, RESERVADA, FUGA, FUERA_SERVICIO, DESCONECTADA]

# Acciones que pueden devolver los eventos. CENTRAL las ejecuta (D5).
ENVIAR_ACEPTADA = "ENVIAR_ACEPTADA"   # ACEPTADA al FO
ENVIAR_START = "ENVIAR_START"         # START al Engine
ARRANCAR_TIMER = "ARRANCAR_TIMER"     # 5 s para que llegue STARTED
CANCELAR_TIMER = "CANCELAR_TIMER"     # ha llegado STARTED (o ya no hace falta)
ENVIAR_DENEGADA = "ENVIAR_DENEGADA"   # DENEGADA al FO
ENVIAR_STOP = "ENVIAR_STOP"           # STOP al Engine
ENVIAR_BLOCK = "ENVIAR_BLOCK"         # BLOCK al Engine
ENVIAR_ACTIVATE = "ENVIAR_ACTIVATE"   # ACTIVATE al Engine
CERRAR_RIEGO = "CERRAR_RIEGO"         # guardar el riego en BD y RESUMEN al FO


def _ahora_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class Estacion:
    def __init__(self, ws_id, ubicacion, conectada=False, fuga=False,
                 bloqueada=False, regando=False):
        self.ws_id = ws_id
        self.ubicacion = ubicacion
        # --- banderas persistentes (también en la BD) ---
        self.conectada = bool(conectada)
        self.fuga = bool(fuga)
        self.bloqueada = bool(bloqueada)
        self.regando = bool(regando)
        # --- solo en memoria ---
        self.reservada = False
        self.req_actual = None       # req_id del riego reservado o en curso
        self.fo_actual = None        # operario que lo pidió (None si origen WS)
        self.origen = None           # "FO", "WS" o "CENTRAL"
        self.duracion_s = 0          # duración pedida
        self.inicio = None           # ISO 8601 de cuando se abrió la válvula
        self.caudal = 0.0            # L/min (última telemetría)
        self.volumen = 0.0           # L acumulados (última telemetría)
        self.segundos = 0            # segundos regando (última telemetría)
        self.ultimo_alive = 0.0      # time.monotonic() del último ALIVE
        self.lock = threading.RLock()

    # ------------------------------------------------------------ consultas
    def estado_visible(self) -> str:
        with self.lock:
            if not self.conectada:
                return DESCONECTADA
            if self.fuga:
                return FUGA
            if self.bloqueada:
                return FUERA_SERVICIO
            if self.regando:
                return REGANDO
            if self.reservada:
                return RESERVADA
            return DISPONIBLE

    def puede_regar(self) -> tuple[bool, str]:
        """Solo una WS DISPONIBLE acepta un riego. Si no, dice por qué."""
        with self.lock:
            if not self.conectada:
                return False, f"{self.ws_id} DESCONECTADA"
            if self.fuga:
                return False, f"{self.ws_id} en FUGA"
            if self.bloqueada:
                return False, f"{self.ws_id} FUERA DE SERVICIO"
            if self.regando:
                return False, f"{self.ws_id} ya está regando"
            if self.reservada:
                return False, f"{self.ws_id} reservada por otra petición"
            return True, ""

    def flags(self) -> dict:
        """Las 4 banderas, tal como se guardan en la BD."""
        with self.lock:
            return {"conectada": self.conectada, "fuga": self.fuga,
                    "bloqueada": self.bloqueada, "regando": self.regando}

    def datos_riego(self) -> dict | None:
        """Copia de los datos del riego actual (o None si no hay).

        CENTRAL la toma ANTES de un evento que cierra el riego (fuga,
        bloqueo...), porque el evento borra estos datos y luego hacen falta
        para guardar el riego y mandar el RESUMEN.
        """
        with self.lock:
            if self.req_actual is None:
                return None
            return {"req_id": self.req_actual, "ws_id": self.ws_id,
                    "fo_id": self.fo_actual, "origen": self.origen,
                    "duracion_s": self.duracion_s, "inicio": self.inicio,
                    "caudal": self.caudal, "volumen": self.volumen,
                    "segundos": self.segundos}

    def _limpiar_riego(self):
        """Olvida el riego actual (reserva o riego en curso)."""
        self.reservada = False
        self.regando = False
        self.req_actual = None
        self.fo_actual = None
        self.origen = None
        self.duracion_s = 0
        self.inicio = None
        self.caudal = 0.0
        self.volumen = 0.0
        self.segundos = 0

    def _cortar(self) -> list:
        """Acciones para cortar lo que hubiera en marcha, y lo olvida.

        - Si regaba: parar el Engine y cerrar el riego (BD + RESUMEN).
        - Si estaba reservada (START enviado, sin STARTED): parar el Engine
          por si llega a arrancar, cancelar el temporizador y DENEGADA al FO.
        """
        if self.regando:
            acciones = [ENVIAR_STOP, CERRAR_RIEGO]
        elif self.reservada:
            acciones = [CANCELAR_TIMER, ENVIAR_STOP, ENVIAR_DENEGADA]
        else:
            acciones = []
        self._limpiar_riego()
        return acciones

    # --------------------------------------------- eventos (desde el socket)
    def autenticada(self) -> list:
        """El Monitor se ha autenticado (AUTH): DESCONECTADA -> DISPONIBLE.

        Si estaba bloqueada en BD, la bandera sigue puesta y se verá
        FUERA_SERVICIO. La fuga se pone a 0: es una sesión nueva del Monitor
        y, si el Engine sigue mal, el Monitor volverá a mandar LEAK.
        """
        with self.lock:
            self.conectada = True
            self.fuga = False
            self._limpiar_riego()
            self.ultimo_alive = time.monotonic()
            return []

    def alive(self) -> list:
        with self.lock:
            self.ultimo_alive = time.monotonic()
            return []

    def fuga_detectada(self) -> list:
        """LEAK del Monitor. Si regaba, se corta el riego.

        (En el plan se llama fuga(), pero ese nombre ya lo usa la bandera
        self.fuga: un método y un atributo no pueden llamarse igual.)
        """
        with self.lock:
            self.fuga = True
            return self._cortar()

    def reparada(self) -> list:
        """RECOVERED del Monitor: FUGA -> DISPONIBLE (o naranja si bloqueada)."""
        with self.lock:
            self.fuga = False
            return []

    def desconectar(self) -> list:
        """Socket cerrado o 5 s sin ALIVE. Si regaba, se cierra el riego."""
        with self.lock:
            if not self.conectada:
                return []                 # ya estaba desconectada
            self.conectada = False
            return self._cortar()

    # --------------------------------------- eventos (peticiones y Kafka)
    def reservar(self, req_id, fo_id, origen="FO", duracion_s=0) -> list:
        """Petición válida: DISPONIBLE -> RESERVADA.

        Comprueba puede_regar() dentro del lock: así, si dos operarios piden
        la misma WS a la vez, solo uno la reserva. Al otro le devuelve
        ENVIAR_DENEGADA (el motivo lo da puede_regar()).
        """
        with self.lock:
            ok, _ = self.puede_regar()
            if not ok:
                return [ENVIAR_DENEGADA]
            self.reservada = True
            self.req_actual = req_id
            self.fo_actual = fo_id
            self.origen = origen
            self.duracion_s = duracion_s
            return [ENVIAR_ACEPTADA, ENVIAR_START, ARRANCAR_TIMER]

    def valvula_abierta(self, req_id) -> list:
        """STARTED del Engine: RESERVADA -> REGANDO."""
        with self.lock:
            if self.reservada and req_id == self.req_actual:
                self.reservada = False
                self.regando = True
                self.inicio = _ahora_iso()
                return [CANCELAR_TIMER]
            if self.regando and req_id == self.req_actual:
                return []                 # STARTED repetido: ya lo sabíamos
            # STARTED de un riego que CENTRAL ya no espera (la reserva caducó,
            # o se cortó por fuga/bloqueo): el Engine no debería estar regando.
            return [ENVIAR_STOP]

    def telemetria(self, req_id, caudal, volumen, segundos) -> bool:
        """Guarda la telemetría. Devuelve False si no es del riego actual."""
        with self.lock:
            if not self.regando or req_id != self.req_actual:
                return False
            self.caudal = caudal
            self.volumen = volumen
            self.segundos = segundos
            return True

    def fin_riego(self, req_id) -> list:
        """FINISHED del Engine: REGANDO -> DISPONIBLE.

        Si ese riego ya no es el actual (CENTRAL ya lo cerró por fuga,
        bloqueo o desconexión), no se hace nada: así el RESUMEN no se manda
        dos veces.
        """
        with self.lock:
            if not self.regando or req_id != self.req_actual:
                return []
            self._limpiar_riego()
            return [CERRAR_RIEGO]

    def timeout_reserva(self, req_id) -> list:
        """Pasan 5 s sin STARTED: RESERVADA -> DISPONIBLE y DENEGADA al FO."""
        with self.lock:
            if not self.reservada or req_id != self.req_actual:
                return []                 # ya llegó STARTED (o se canceló)
            self._limpiar_riego()
            return [ENVIAR_DENEGADA]

    def start_rechazado(self, req_id) -> list:
        """START_REJECTED del Engine: RESERVADA -> DISPONIBLE y DENEGADA."""
        with self.lock:
            if not self.reservada or req_id != self.req_actual:
                return []
            self._limpiar_riego()
            return [CANCELAR_TIMER, ENVIAR_DENEGADA]

    # ------------------------------------------- órdenes del operador de CENTRAL
    def bloquear(self) -> list:
        """-> FUERA_SERVICIO. Se avisa al Engine y se corta el riego si lo hay."""
        with self.lock:
            self.bloqueada = True
            return [ENVIAR_BLOCK] + self._cortar()

    def activar(self) -> list:
        """FUERA_SERVICIO -> DISPONIBLE."""
        with self.lock:
            if not self.bloqueada:
                return []
            self.bloqueada = False
            return [ENVIAR_ACTIVATE]


class Red:
    """Todas las estaciones: diccionario ws_id -> Estacion.

    Se carga con la lista de dicts que devuelve BD.listar_estaciones(), así
    este módulo no necesita importar la BD. El diccionario tiene su propio
    lock porque los hilos de los sockets pueden añadir estaciones nuevas
    mientras otro hilo lo recorre.
    """

    def __init__(self, filas_bd: list[dict] = ()):
        self.lock = threading.Lock()
        self.estaciones: dict[str, Estacion] = {}
        for f in filas_bd:
            self.estaciones[f["ws_id"]] = Estacion(
                f["ws_id"], f["ubicacion"], f["conectada"], f["fuga"],
                f["bloqueada"], f["regando"])

    def get(self, ws_id) -> Estacion | None:
        with self.lock:
            return self.estaciones.get(ws_id)

    def agregar(self, ws_id, ubicacion) -> Estacion:
        """Devuelve la estación, creándola si no existía."""
        with self.lock:
            if ws_id not in self.estaciones:
                self.estaciones[ws_id] = Estacion(ws_id, ubicacion)
            return self.estaciones[ws_id]

    def todas(self) -> list[Estacion]:
        """Copia de la lista, ordenada por ID, para recorrerla sin el lock."""
        with self.lock:
            return [self.estaciones[k] for k in sorted(self.estaciones)]

    def resumen(self) -> dict:
        """Cuántas estaciones hay en cada estado."""
        cuenta = {e: 0 for e in ORDEN_ESTADOS}
        for est in self.todas():
            cuenta[est.estado_visible()] += 1
        return cuenta

    def lista_estado(self) -> list[dict]:
        """Lista para el mensaje ESTADO_RED: [{ws_id, ubicacion, estado}]."""
        return [{"ws_id": e.ws_id, "ubicacion": e.ubicacion,
                 "estado": e.estado_visible()} for e in self.todas()]
