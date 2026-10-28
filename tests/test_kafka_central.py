"""
Tests de la lógica Kafka de CENTRAL SIN bróker.

En lugar de un Producer de verdad usamos uno falso que guarda lo que se
"publica". Los mensajes de entrada se pasan directamente a
procesar_solicitud() / procesar_telemetria(), como si llegaran de Kafka.
Así se comprueban los flujos B, C y D del contrato en milisegundos.

Ejecutar desde la raíz:  python -m pytest tests/test_kafka_central.py
"""

import json
import time

import pytest

from central.WM_Central import Central
from central.db import BD
from central.estados import Red
from central.kafka_central import KafkaCentral
from common import mensajes as m


class ProducerFalso:
    def __init__(self):
        self.enviados = []           # (topic, key, dict)

    def produce(self, topic, key=None, value=None):
        self.enviados.append((topic, key.decode(), json.loads(value)))

    def flush(self, *args):
        pass

    def poll(self, *args):
        pass

    def tipos(self, topic, key=None):
        return [d["tipo"] for t, k, d in self.enviados
                if t == topic and (key is None or k == key)]

    def ultimo(self, topic, tipo):
        for t, k, d in reversed(self.enviados):
            if t == topic and d["tipo"] == tipo:
                return d
        return None


@pytest.fixture
def entorno(tmp_path):
    bd = BD(str(tmp_path / "k.db"))
    bd.inicializar()
    central = Central(bd, Red())
    prod = ProducerFalso()
    central.kafka = KafkaCentral(central, "falso:9092", producer=prod,
                                 seg_reserva=0.3)
    # Dos WS autenticadas, como si sus Monitores ya se hubieran conectado.
    for ws, sitio in (("WS-04", "River Park"), ("WS-11", "Riverside Gardens")):
        bd.registrar_ws(ws, sitio)
        est = central.red.agregar(ws, sitio)
        central.aplicar(est, est.autenticada)
    prod.enviados.clear()
    return central, prod


def pedir(central, req, ws="WS-04", fo="FO-12", dur=30, origen="FO", ts=None):
    d = m.solicitud_riego(req, origen, fo, ws, dur)
    if ts:
        d["ts"] = ts
    central.kafka.procesar_solicitud(d)


def estado(central, ws="WS-04"):
    return central.red.get(ws).estado_visible()


NOTIF = m.T_NOTIFICATIONS
CMD = m.T_COMMANDS


def tel(central, msg):
    central.kafka.procesar_telemetria(msg)


# ------------------------------------------------------- flujo B: riego normal
def test_flujo_b_riego_normal(entorno):
    central, prod = entorno
    pedir(central, "r1")
    assert prod.tipos(NOTIF, "FO-12") == ["PASO", "ACEPTADA"]
    assert prod.tipos(CMD, "WS-04") == ["START"]
    assert estado(central) == "RESERVADA"

    tel(central, m.started("r1", "WS-04"))
    assert estado(central) == "REGANDO"
    assert prod.ultimo(NOTIF, "PASO")["texto"] == "Riego iniciado en WS-04"

    tel(central, m.telemetria("r1", "WS-04", 11.4, 0.19, 1))
    assert prod.ultimo(NOTIF, "TELEMETRIA")["caudal_lpm"] == 11.4

    tel(central, m.finished("r1", "WS-04", "TIEMPO", 5.5, 30))
    assert estado(central) == "DISPONIBLE"
    resumen = prod.ultimo(NOTIF, "RESUMEN")
    assert resumen["motivo"] == "TIEMPO" and resumen["volumen_total_l"] == 5.5
    [riego] = central.bd.listar_riegos("WS-04")
    assert riego["req_id"] == "r1" and riego["fo_id"] == "FO-12"
    # Se ha publicado el estado de la red tras los cambios.
    assert "ESTADO_RED" in prod.tipos(m.T_STATUS, "red")


def test_no_hay_denegada_por_timeout_si_llega_started(entorno):
    central, prod = entorno
    pedir(central, "r1")
    tel(central, m.started("r1", "WS-04"))
    time.sleep(0.5)                                  # más que seg_reserva
    assert "DENEGADA" not in prod.tipos(NOTIF)
    assert estado(central) == "REGANDO"


# --------------------------------------------- flujo C: fuga durante el riego
def test_flujo_c_fuga_en_mitad_del_riego(entorno):
    central, prod = entorno
    pedir(central, "r1")
    tel(central, m.started("r1", "WS-04"))
    tel(central, m.telemetria("r1", "WS-04", 10.0, 2.0, 12))
    # El Engine corta y publica FINISHED (FUGA); el Monitor manda LEAK.
    tel(central, m.finished("r1", "WS-04", "FUGA", 2.1, 12))
    est = central.red.get("WS-04")
    central.aplicar(est, est.fuga_detectada, motivo="FUGA")   # LEAK por socket
    assert estado(central) == "FUGA"
    assert prod.tipos(NOTIF, "FO-12").count("RESUMEN") == 1   # solo un resumen
    assert prod.ultimo(NOTIF, "RESUMEN")["motivo"] == "FUGA"


def test_fuga_llega_antes_que_finished(entorno):
    central, prod = entorno
    pedir(central, "r1")
    tel(central, m.started("r1", "WS-04"))
    tel(central, m.telemetria("r1", "WS-04", 10.0, 2.0, 12))
    est = central.red.get("WS-04")
    central.aplicar(est, est.fuga_detectada, motivo="FUGA")   # LEAK primero
    assert prod.ultimo(CMD, "STOP")["motivo"] == "FUGA"
    tel(central, m.finished("r1", "WS-04", "FUGA", 2.1, 12))  # se ignora
    assert prod.tipos(NOTIF, "FO-12").count("RESUMEN") == 1
    assert central.bd.listar_riegos("WS-04")[0]["volumen_total_l"] == 2.0


# ------------------------------------------------------ flujo D: denegada
def test_flujo_d_denegada_en_fuga(entorno):
    central, prod = entorno
    est = central.red.get("WS-11")
    central.aplicar(est, est.fuga_detectada, motivo="FUGA")
    prod.enviados.clear()
    pedir(central, "r1", ws="WS-11", fo="FO-07")
    assert prod.tipos(NOTIF, "FO-07") == ["PASO", "DENEGADA"]
    assert "FUGA" in prod.ultimo(NOTIF, "DENEGADA")["motivo"]
    assert prod.tipos(CMD) == []                     # la WS no recibe nada


# ------------------------------------------------------------ otros casos
def test_dos_operarios_misma_ws(entorno):
    central, prod = entorno
    pedir(central, "r1", fo="FO-12")
    pedir(central, "r2", fo="FO-07")
    assert "ACEPTADA" in prod.tipos(NOTIF, "FO-12")
    assert prod.tipos(NOTIF, "FO-07") == ["PASO", "DENEGADA"]
    assert prod.tipos(CMD) == ["START"]


def test_ws_y_operario_que_no_existen(entorno):
    central, prod = entorno
    pedir(central, "r1", ws="WS-99")
    assert "no existe" in prod.ultimo(NOTIF, "DENEGADA")["motivo"]
    pedir(central, "r2", fo="FO-99")
    assert "no registrado" in prod.ultimo(NOTIF, "DENEGADA")["motivo"]


def test_peticion_antigua_se_descarta(entorno):
    central, prod = entorno
    pedir(central, "r1", ts="2026-01-01T00:00:00+00:00")
    assert prod.enviados == []
    assert estado(central) == "DISPONIBLE"


def test_peticion_duplicada_se_ignora(entorno):
    central, prod = entorno
    pedir(central, "r1")
    pedir(central, "r1")
    assert prod.tipos(NOTIF, "FO-12") == ["PASO", "ACEPTADA"]


def test_timeout_de_reserva(entorno):
    central, prod = entorno
    pedir(central, "r1")
    time.sleep(0.6)                                  # seg_reserva = 0.3
    assert estado(central) == "DISPONIBLE"
    assert prod.ultimo(NOTIF, "DENEGADA")["req_id"] == "r1"


def test_start_rejected(entorno):
    central, prod = entorno
    pedir(central, "r1")
    tel(central, m.start_rejected("r1", "WS-04", "ENGINE_KO"))
    assert estado(central) == "DISPONIBLE"
    assert "ENGINE_KO" in prod.ultimo(NOTIF, "DENEGADA")["motivo"]


def test_riego_manual_desde_la_ws_no_notifica(entorno):
    central, prod = entorno
    pedir(central, "r1", fo=None, origen="WS", dur=10)
    assert prod.tipos(NOTIF) == []
    assert prod.tipos(CMD) == ["START"]
    tel(central, m.started("r1", "WS-04"))
    tel(central, m.finished("r1", "WS-04", "MANUAL", 1.2, 6))
    assert prod.tipos(NOTIF) == []
    assert central.bd.listar_riegos()[0]["origen"] == "WS"


def test_desconexion_durante_riego(entorno):
    central, prod = entorno
    pedir(central, "r1")
    tel(central, m.started("r1", "WS-04"))
    est = central.red.get("WS-04")
    central.aplicar(est, est.desconectar, motivo="DESCONEXION")
    assert estado(central) == "DESCONECTADA"
    assert prod.ultimo(CMD, "STOP")["motivo"] == "DESCONEXION"
    assert prod.ultimo(NOTIF, "RESUMEN")["motivo"] == "DESCONEXION"


def test_bloquear_durante_riego_y_activar(entorno):
    central, prod = entorno
    pedir(central, "r1")
    tel(central, m.started("r1", "WS-04"))
    central.kafka.bloquear_ws("WS-04")
    assert estado(central) == "FUERA_SERVICIO"
    assert "BLOCK" in prod.tipos(CMD, "WS-04")
    assert prod.ultimo(NOTIF, "RESUMEN")["motivo"] == "BLOQUEO"
    central.kafka.activar_ws("WS-04")
    assert estado(central) == "DISPONIBLE"
    assert prod.tipos(CMD, "WS-04")[-1] == "ACTIVATE"


def test_bloquear_todas_un_mensaje_por_estacion(entorno):
    central, prod = entorno
    central.kafka.bloquear_todas()
    assert prod.tipos(CMD, "WS-04") == ["BLOCK"]
    assert prod.tipos(CMD, "WS-11") == ["BLOCK"]


def test_estado_red(entorno):
    central, prod = entorno
    central.kafka.publicar_estado()
    d = prod.ultimo(m.T_STATUS, "ESTADO_RED")
    assert d["estaciones"][0] == {"ws_id": "WS-04", "ubicacion": "River Park",
                                  "estado": "DISPONIBLE"}
