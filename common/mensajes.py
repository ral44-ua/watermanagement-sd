"""
Mensajes Kafka de WaterManagement (contrato entre módulos, apartado 2).

Este módulo SOLO construye diccionarios. No envía nada: para eso está
common/kafka_utils.py. Separarlo así permite probar los mensajes sin Kafka.

Reglas del contrato que se cumplen aquí:
  - Todos los mensajes llevan siempre "tipo" (qué es) y "ts" (fecha ISO 8601).
  - Los IDs llevan guion: estaciones "WS-04", operarios "FO-12".
    Este módulo no los valida ni los transforma: se copian tal cual llegan,
    porque se usan como clave (key) de Kafka y cada Engine/FO filtra
    comparando la clave con su propio ID. "WS04" y "WS-04" serían distintos.
  - req_id es un UUID que genera quien pide; todas las respuestas lo repiten.
"""

from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# Topics (5, todos con 3 particiones). Constantes para no escribirlos a mano:
# un error de tecleo en un string no da error, simplemente no llega nada.
# ---------------------------------------------------------------------------
T_REQUESTS = "wm.requests"                # FO / Engine -> CENTRAL   key: ws_id
T_COMMANDS = "wm.ws.commands"             # CENTRAL -> Engines       key: ws_id
T_TELEMETRY = "wm.ws.telemetry"           # Engine -> CENTRAL        key: ws_id
T_NOTIFICATIONS = "wm.fo.notifications"   # CENTRAL -> FOs           key: fo_id
T_STATUS = "wm.ws.status"                 # CENTRAL -> FOs           key: "red"

TOPICS = [T_REQUESTS, T_COMMANDS, T_TELEMETRY, T_NOTIFICATIONS, T_STATUS]

# Clave fija de wm.ws.status: es un mensaje para todos los FO, no para uno.
KEY_RED = "red"


def ahora() -> str:
    """Fecha y hora actual en ISO 8601, en UTC y con la zona indicada.

    Ejemplo: "2026-10-22T14:00:00+00:00".
    Usamos UTC porque CENTRAL (en Railway) y las WS/FO (en el laboratorio)
    pueden tener relojes en zonas horarias distintas, y CENTRAL compara este
    campo para descartar las peticiones con más de 30 s.
    """
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _base(tipo: str) -> dict:
    """Campos comunes a todos los mensajes: tipo y ts."""
    return {"tipo": tipo, "ts": ahora()}


# ---------------------------------------------------------------------------
# wm.requests  (FO o Engine -> CENTRAL)
# ---------------------------------------------------------------------------
def solicitud_riego(req_id, origen, fo_id, ws_id, duracion_s) -> dict:
    """Petición de riego.

    origen: "FO" si la pide un operario, "WS" si la pide la propia estación
    desde su menú. En ese último caso fo_id es None (en JSON: null).
    """
    m = _base("SOLICITUD_RIEGO")
    m.update(req_id=req_id, origen=origen, fo_id=fo_id,
             ws_id=ws_id, duracion_s=duracion_s)
    return m


# ---------------------------------------------------------------------------
# wm.ws.commands  (CENTRAL -> Engine)
# Para órdenes a todas las WS se envía un mensaje por estación.
# ---------------------------------------------------------------------------
def start(req_id, ws_id, fo_id, duracion_s, origen=None) -> dict:
    """Orden de abrir la válvula.

    origen ("FO", "WS" o "CENTRAL") y fo_id se añadieron para la resiliencia:
    el Engine los repite en su TELEMETRIA y su FINISHED. Así, si CENTRAL se
    cae y se reinicia en mitad de un riego, sabe de quién era ese riego y a
    qué operario mandarle el RESUMEN, aunque ya no lo tenga en memoria.
    """
    m = _base("START")
    m.update(req_id=req_id, ws_id=ws_id, fo_id=fo_id, duracion_s=duracion_s,
             origen=origen)
    return m


def stop(ws_id, motivo) -> dict:
    m = _base("STOP")
    m.update(ws_id=ws_id, motivo=motivo)
    return m


def block(ws_id) -> dict:
    m = _base("BLOCK")
    m.update(ws_id=ws_id)
    return m


def activate(ws_id) -> dict:
    m = _base("ACTIVATE")
    m.update(ws_id=ws_id)
    return m


# ---------------------------------------------------------------------------
# wm.ws.telemetry  (Engine -> CENTRAL)
# ---------------------------------------------------------------------------
def started(req_id, ws_id) -> dict:
    """El Engine ha abierto la válvula."""
    m = _base("STARTED")
    m.update(req_id=req_id, ws_id=ws_id)
    return m


def telemetria(req_id, ws_id, caudal_lpm, volumen_l, segundos,
               fo_id=None, origen=None) -> dict:
    """Se envía cada segundo mientras se riega.

    fo_id y origen: los del START (ver start()). Son opcionales para que los
    mensajes antiguos y los tests sigan valiendo.
    """
    m = _base("TELEMETRIA")
    m.update(req_id=req_id, ws_id=ws_id, caudal_lpm=caudal_lpm,
             volumen_l=volumen_l, segundos=segundos, fo_id=fo_id, origen=origen)
    return m


def finished(req_id, ws_id, motivo, volumen_total_l, duracion_s,
             fo_id=None, origen=None) -> dict:
    """Fin del riego. motivo: TIEMPO, MANUAL, FUGA, BLOQUEO o DESCONEXION.

    fo_id y origen: los del START. CENTRAL los usa si el riego ya no está en
    su memoria (se reinició mientras se regaba) para guardarlo y avisar al FO.
    """
    m = _base("FINISHED")
    m.update(req_id=req_id, ws_id=ws_id, motivo=motivo,
             volumen_total_l=volumen_total_l, duracion_s=duracion_s,
             fo_id=fo_id, origen=origen)
    return m


def start_rejected(req_id, ws_id, motivo) -> dict:
    """El Engine no puede empezar (por ejemplo, está en KO o ya regando)."""
    m = _base("START_REJECTED")
    m.update(req_id=req_id, ws_id=ws_id, motivo=motivo)
    return m


# ---------------------------------------------------------------------------
# wm.fo.notifications  (CENTRAL -> FO)
# Todos llevan req_id, fo_id y ws_id, para que el FO sepa a qué petición
# y a qué estación corresponde cada aviso.
# ---------------------------------------------------------------------------
def paso(req_id, fo_id, ws_id, texto) -> dict:
    """Paso intermedio, por ejemplo "Comprobando estado de WS-04…"."""
    m = _base("PASO")
    m.update(req_id=req_id, fo_id=fo_id, ws_id=ws_id, texto=texto)
    return m


def aceptada(req_id, fo_id, ws_id) -> dict:
    m = _base("ACEPTADA")
    m.update(req_id=req_id, fo_id=fo_id, ws_id=ws_id)
    return m


def denegada(req_id, fo_id, ws_id, motivo) -> dict:
    m = _base("DENEGADA")
    m.update(req_id=req_id, fo_id=fo_id, ws_id=ws_id, motivo=motivo)
    return m


def telemetria_fo(req_id, fo_id, ws_id, caudal_lpm, volumen_l) -> dict:
    """Telemetría que CENTRAL reenvía al FO que pidió el riego.

    Tiene tipo "TELEMETRIA", igual que la del Engine, pero va por otro topic
    y lleva fo_id. Por eso la función tiene otro nombre.
    """
    m = _base("TELEMETRIA")
    m.update(req_id=req_id, fo_id=fo_id, ws_id=ws_id,
             caudal_lpm=caudal_lpm, volumen_l=volumen_l)
    return m


def resumen(req_id, fo_id, ws_id, motivo, volumen_total_l, duracion_s) -> dict:
    """Resumen final del riego que recibe el FO."""
    m = _base("RESUMEN")
    m.update(req_id=req_id, fo_id=fo_id, ws_id=ws_id, motivo=motivo,
             volumen_total_l=volumen_total_l, duracion_s=duracion_s)
    return m


# ---------------------------------------------------------------------------
# wm.ws.status  (CENTRAL -> todos los FO), clave KEY_RED
# ---------------------------------------------------------------------------
def estado_red(estaciones) -> dict:
    """Estado de toda la red.

    estaciones: lista de dicts {"ws_id", "ubicacion", "estado"}.
    Se publica en cada cambio y cada 5 s.
    """
    m = _base("ESTADO_RED")
    m.update(estaciones=estaciones)
    return m
