"""
Tests del panel de CENTRAL: el de consola (colores) y el web (estado y órdenes).

Para el web se arranca un PanelWeb de verdad en un puerto libre y se le hacen
peticiones HTTP con urllib (biblioteca estándar). Kafka se sustituye por un
productor falso, como en test_kafka_central.py.

Ejecutar desde la raíz:  python -m pytest tests/test_panel.py
"""

import base64
import json
import urllib.error
import urllib.request

import pytest

from common import mensajes as m
from wm_central.WM_Central import Central, REGANDO_A, FONDO, texto_panel
from wm_central.db import BD
from wm_central.estados import Red
from wm_central.kafka_central import KafkaCentral
from wm_central.panel_web import PanelWeb


class ProducerFalso:
    def __init__(self):
        self.enviados = []           # (topic, key, dict)

    def produce(self, topic, key=None, value=None):
        self.enviados.append((topic, key.decode(), json.loads(value)))

    def poll(self, *args):
        pass

    def flush(self, *args):
        pass

    def tipos(self, topic, key=None):
        return [d["tipo"] for t, k, d in self.enviados
                if t == topic and (key is None or k == key)]


@pytest.fixture
def central(tmp_path):
    bd = BD(str(tmp_path / "p.db"))
    bd.inicializar()
    c = Central(bd, Red())
    c.prod = ProducerFalso()
    c.kafka = KafkaCentral(c, "falso:9092", producer=c.prod, seg_reserva=60)
    for ws, sitio in (("WS-04", "River Park"), ("WS-11", "Riverside Gardens")):
        bd.registrar_ws(ws, sitio)
        est = c.red.agregar(ws, sitio)
        c.aplicar(est, est.autenticada)
    c.prod.enviados.clear()
    return c


def pedir(puerto, ruta, orden=None, clave=None):
    """GET (sin orden) o POST con JSON. Devuelve (código, cuerpo)."""
    datos = None if orden is None else json.dumps(orden).encode()
    req = urllib.request.Request(f"http://127.0.0.1:{puerto}{ruta}", data=datos)
    if clave is not None:
        req.add_header("Authorization",
                       "Basic " + base64.b64encode(f"admin:{clave}".encode()).decode())
    try:
        with urllib.request.urlopen(req, timeout=5) as r:
            return r.status, r.read()
    except urllib.error.HTTPError as ex:
        return ex.code, ex.read()


def estado(central, ws):
    return central.red.get(ws).estado_visible()


# ---------------------------------------------------------- panel de consola
def test_consola_colores_y_parpadeo(central):
    est = central.red.get("WS-04")
    central.kafka.solicitar("r1", "WS-04", 30, "FO-12", "FO")
    est.valvula_abierta("r1")
    est.telemetria("r1", 11.4, 3.25, 17)
    central.kafka.bloquear_ws("WS-11")
    foto = central.foto()
    a = texto_panel(foto, "prueba", parpadeo=True)
    b = texto_panel(foto, "prueba", parpadeo=False)
    assert REGANDO_A in a and REGANDO_A not in b       # verde parpadeando
    assert "11.4 L/m" in a and "3.25 L" in a and "FO-12" in a
    assert FONDO["FUERA_SERVICIO"] in a and "Fuera de Servicio" in a


def test_foto_solo_da_caudal_si_riega(central):
    [ws04, ws11] = central.foto()["estaciones"]
    assert ws04["estado"] == "DISPONIBLE" and ws04["caudal"] is None
    assert central.foto()["resumen"]["DISPONIBLE"] == 2


# --------------------------------------------------------------- panel web
def test_web_pagina_y_estado(central):
    puerto = PanelWeb(central, 0).arrancar()
    codigo, cuerpo = pedir(puerto, "/")
    assert codigo == 200 and b"WM_Central" in cuerpo
    codigo, cuerpo = pedir(puerto, "/api/estado")
    foto = json.loads(cuerpo)
    assert [e["ws_id"] for e in foto["estaciones"]] == ["WS-04", "WS-11"]
    assert foto["estaciones"][0]["etiqueta"] == "Disponible"


def test_web_bloquear_y_activar_una(central):
    puerto = PanelWeb(central, 0).arrancar()
    codigo, _ = pedir(puerto, "/api/orden", {"orden": "bloquear", "ws_id": "WS-04"})
    assert codigo == 200
    assert estado(central, "WS-04") == "FUERA_SERVICIO"
    assert central.prod.tipos(m.T_COMMANDS, "WS-04") == ["BLOCK"]
    assert estado(central, "WS-11") == "DISPONIBLE"
    pedir(puerto, "/api/orden", {"orden": "activar", "ws_id": "WS-04"})
    assert estado(central, "WS-04") == "DISPONIBLE"
    assert central.prod.tipos(m.T_COMMANDS, "WS-04") == ["BLOCK", "ACTIVATE"]


def test_web_ordenes_a_todas(central):
    puerto = PanelWeb(central, 0).arrancar()
    pedir(puerto, "/api/orden", {"orden": "bloquear", "ws_id": "TODAS"})
    assert central.prod.tipos(m.T_COMMANDS) == ["BLOCK", "BLOCK"]   # uno por WS
    pedir(puerto, "/api/orden", {"orden": "activar", "ws_id": "TODAS"})
    codigo, _ = pedir(puerto, "/api/orden",
                      {"orden": "iniciar", "ws_id": "TODAS", "duracion_s": 20})
    assert codigo == 200
    starts = [d for t, k, d in central.prod.enviados if d["tipo"] == "START"]
    assert sorted(d["ws_id"] for d in starts) == ["WS-04", "WS-11"]
    assert all(d["fo_id"] is None for d in starts)       # lo pide CENTRAL
    assert central.prod.tipos(m.T_NOTIFICATIONS) == []   # no hay operario
    assert estado(central, "WS-04") == "RESERVADA"


def test_web_iniciar_riego_en_ws_con_fuga_se_deniega(central):
    est = central.red.get("WS-11")
    central.aplicar(est, est.fuga_detectada, motivo="FUGA")
    puerto = PanelWeb(central, 0).arrancar()
    pedir(puerto, "/api/orden", {"orden": "iniciar", "ws_id": "WS-11", "duracion_s": 20})
    assert central.prod.tipos(m.T_COMMANDS) == []        # no se manda START
    assert any("DENEGADA" in ev for ev in central.foto()["eventos"])


@pytest.mark.parametrize("orden, codigo", [
    ({"orden": "borrar", "ws_id": "WS-04"}, 400),
    ({"orden": "bloquear"}, 400),
    ({"orden": "bloquear", "ws_id": "WS-99"}, 404),
    ({"orden": "iniciar", "ws_id": "WS-04"}, 400),
    ({"orden": "iniciar", "ws_id": "WS-04", "duracion_s": 0}, 400),
    ({"orden": "iniciar", "ws_id": "WS-04", "duracion_s": "30"}, 400),
    ({"orden": "iniciar", "ws_id": "WS-04", "duracion_s": True}, 400),
    ([1, 2], 400),
])
def test_web_rechaza_ordenes_invalidas(central, orden, codigo):
    puerto = PanelWeb(central, 0).arrancar()
    assert pedir(puerto, "/api/orden", orden)[0] == codigo
    assert central.prod.enviados == []


def test_web_json_roto(central):
    puerto = PanelWeb(central, 0).arrancar()
    req = urllib.request.Request(f"http://127.0.0.1:{puerto}/api/orden", data=b"{roto")
    with pytest.raises(urllib.error.HTTPError) as ex:
        urllib.request.urlopen(req, timeout=5)
    assert ex.value.code == 400


def test_web_con_clave(central):
    puerto = PanelWeb(central, 0, clave="secreta").arrancar()
    assert pedir(puerto, "/api/estado")[0] == 401                  # sin clave
    assert pedir(puerto, "/api/estado", clave="mala")[0] == 401
    assert pedir(puerto, "/api/estado", clave="secreta")[0] == 200
    codigo, _ = pedir(puerto, "/api/orden", {"orden": "bloquear", "ws_id": "WS-04"})
    assert codigo == 401 and central.prod.enviados == []
