"""
Tests de common/mensajes.py.

Comprueban, para cada tipo de mensaje del contrato:
  1. que devuelve un dict con "tipo" y "ts";
  2. que lleva exactamente los campos del contrato (ni más ni menos);
  3. que se puede serializar a JSON.

Ejecutar desde la raíz del repo:  python -m pytest tests/test_mensajes.py
"""

import json
from datetime import datetime

import pytest

from common import mensajes as m

# (función, argumentos, tipo esperado, campos del contrato además de tipo y ts)
CASOS = [
    (m.solicitud_riego, ("r1", "FO", "FO-12", "WS-04", 30), "SOLICITUD_RIEGO",
     {"req_id", "origen", "fo_id", "ws_id", "duracion_s"}),
    (m.start, ("r1", "WS-04", "FO-12", 30), "START",
     {"req_id", "ws_id", "fo_id", "duracion_s", "origen"}),
    (m.stop, ("WS-04", "FUGA"), "STOP", {"ws_id", "motivo"}),
    (m.block, ("WS-04",), "BLOCK", {"ws_id"}),
    (m.activate, ("WS-04",), "ACTIVATE", {"ws_id"}),
    (m.started, ("r1", "WS-04"), "STARTED", {"req_id", "ws_id", "fo_id", "origen"}),
    (m.telemetria, ("r1", "WS-04", 11.4, 186.2, 17), "TELEMETRIA",
     {"req_id", "ws_id", "caudal_lpm", "volumen_l", "segundos", "fo_id", "origen"}),
    (m.finished, ("r1", "WS-04", "TIEMPO", 342.0, 30), "FINISHED",
     {"req_id", "ws_id", "motivo", "volumen_total_l", "duracion_s", "fo_id", "origen"}),
    (m.start_rejected, ("r1", "WS-04", "ENGINE_KO"), "START_REJECTED",
     {"req_id", "ws_id", "motivo"}),
    (m.paso, ("r1", "FO-12", "WS-04", "Comprobando estado de WS-04…"), "PASO",
     {"req_id", "fo_id", "ws_id", "texto"}),
    (m.aceptada, ("r1", "FO-12", "WS-04"), "ACEPTADA",
     {"req_id", "fo_id", "ws_id"}),
    (m.denegada, ("r1", "FO-12", "WS-04", "WS en FUGA"), "DENEGADA",
     {"req_id", "fo_id", "ws_id", "motivo"}),
    (m.telemetria_fo, ("r1", "FO-12", "WS-04", 11.4, 186.2), "TELEMETRIA",
     {"req_id", "fo_id", "ws_id", "caudal_lpm", "volumen_l"}),
    (m.resumen, ("r1", "FO-12", "WS-04", "TIEMPO", 342.0, 30), "RESUMEN",
     {"req_id", "fo_id", "ws_id", "motivo", "volumen_total_l", "duracion_s"}),
    (m.estado_red,
     ([{"ws_id": "WS-04", "ubicacion": "River Park", "estado": "DISPONIBLE"}],),
     "ESTADO_RED", {"estaciones"}),
]


@pytest.mark.parametrize("funcion, args, tipo, campos", CASOS,
                         ids=[c[2] + "_" + c[0].__name__ for c in CASOS])
def test_tipo_ts_y_campos(funcion, args, tipo, campos):
    msg = funcion(*args)
    assert isinstance(msg, dict)
    assert msg["tipo"] == tipo
    assert "ts" in msg
    assert set(msg) == campos | {"tipo", "ts"}


@pytest.mark.parametrize("funcion, args, tipo, campos", CASOS,
                         ids=[c[2] + "_" + c[0].__name__ for c in CASOS])
def test_serializable_a_json(funcion, args, tipo, campos):
    msg = funcion(*args)
    texto = json.dumps(msg, ensure_ascii=False)
    assert json.loads(texto) == msg       # ida y vuelta sin perder nada


def test_ts_es_iso8601_en_utc():
    ts = m.ahora()
    fecha = datetime.fromisoformat(ts)    # falla si no es ISO 8601
    assert fecha.utcoffset().total_seconds() == 0


def test_solicitud_desde_la_ws_lleva_fo_id_null():
    msg = m.solicitud_riego("r1", "WS", None, "WS-04", 30)
    assert '"fo_id": null' in json.dumps(msg)


def test_cinco_topics():
    assert m.TOPICS == ["wm.requests", "wm.ws.commands", "wm.ws.telemetry",
                        "wm.fo.notifications", "wm.ws.status"]
