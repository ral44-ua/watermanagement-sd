"""
Tests de los arreglos de CENTRAL pedidos por la guía de corrección.

Igual que test_kafka_central.py: sin bróker, con un Producer falso, y los
mensajes de Kafka se pasan a mano a procesar_solicitud()/procesar_telemetria().

Para simular que CENTRAL "se cae y se reinicia" se crea una Central nueva
sobre el MISMO fichero de BD: es exactamente lo que pasa al volver a lanzar
python -m WM_Central.WM_Central (la memoria se pierde, la BD se queda).

Ejecutar desde la raíz:  python -m pytest tests/test_resiliencia_central.py
"""

import time

import pytest

from common import mensajes as m
from tests.test_kafka_central import ProducerFalso
from WM_Central.WM_Central import Central, texto_panel
from WM_Central.db import BD
from WM_Central.estados import Red
from WM_Central.kafka_central import KafkaCentral

NOTIF = m.T_NOTIFICATIONS
CMD = m.T_COMMANDS


def arrancar_central(ruta_bd, seg_reserva=0.3):
    """Hace lo mismo que WM_Central.main() (sin sockets ni panel) y deja WS-04
    y WS-11 autenticadas, como si sus Monitores ya se hubieran conectado."""
    bd = BD(str(ruta_bd))
    bd.inicializar()
    for ws, sitio in (("WS-04", "River Park"), ("WS-11", "Riverside Gardens")):
        bd.registrar_ws(ws, sitio)
    central = Central(bd, Red(bd.listar_estaciones()))
    prod = ProducerFalso()
    central.kafka = KafkaCentral(central, "falso:9092", producer=prod,
                                 seg_reserva=seg_reserva)
    for ws in ("WS-04", "WS-11"):
        est = central.red.get(ws)
        central.aplicar(est, est.autenticada)
    prod.enviados.clear()
    return central, prod


@pytest.fixture
def ruta(tmp_path):
    return tmp_path / "central.db"


@pytest.fixture
def entorno(ruta):
    return arrancar_central(ruta)


def pedir(central, req, ws="WS-04", fo="FO-12", dur=30, origen="FO"):
    central.kafka.procesar_solicitud(m.solicitud_riego(req, origen, fo, ws, dur))


def tel(central, msg):
    central.kafka.procesar_telemetria(msg)


def estado(central, ws="WS-04"):
    return central.red.get(ws).estado_visible()


# ------------------------------------------- operarios nuevos (escalabilidad)
def test_operario_nuevo_se_da_de_alta_y_puede_regar(entorno):
    central, prod = entorno
    assert not central.bd.existe_operario("FO-03")
    pedir(central, "r1", fo="FO-03")
    assert prod.tipos(NOTIF, "FO-03") == ["PASO", "ACEPTADA"]
    assert central.bd.existe_operario("FO-03")
    assert prod.tipos(CMD, "WS-04") == ["START"]


def test_operario_con_id_invalido_se_deniega(entorno):
    central, prod = entorno
    pedir(central, "r1", fo="Pepe")
    assert prod.tipos(NOTIF, "Pepe") == ["PASO", "DENEGADA"]
    assert prod.tipos(CMD) == []


# -------------------------------- riego pedido desde la WS: aviso de denegada
def test_riego_de_la_ws_denegado_avisa_a_la_ws(entorno):
    central, prod = entorno
    est = central.red.get("WS-04")
    central.aplicar(est, est.fuga_detectada, motivo="FUGA")
    prod.enviados.clear()
    pedir(central, "r1", fo=None, origen="WS")
    d = prod.ultimo(CMD, "DENEGADA")
    assert d is not None and d["ws_id"] == "WS-04" and d["req_id"] == "r1"
    assert "FUGA" in d["motivo"]
    assert prod.tipos(NOTIF) == []                 # no hay operario


def test_riego_de_la_ws_que_caduca_avisa_a_la_ws(entorno):
    central, prod = entorno
    pedir(central, "r1", fo=None, origen="WS")
    time.sleep(0.6)                                 # seg_reserva = 0.3
    assert prod.ultimo(CMD, "DENEGADA")["req_id"] == "r1"


def test_start_lleva_origen_y_operario(entorno):
    central, prod = entorno
    pedir(central, "r1")
    d = prod.ultimo(CMD, "START")
    assert d["fo_id"] == "FO-12" and d["origen"] == "FO"


# -------------------------------------- CENTRAL se cae en mitad de un riego
def test_reinicio_de_central_en_mitad_de_un_riego(ruta):
    central, prod = arrancar_central(ruta)
    pedir(central, "r1")
    tel(central, m.started("r1", "WS-04"))
    tel(central, m.telemetria("r1", "WS-04", 10.0, 1.0, 6, "FO-12", "FO"))

    # --- CENTRAL se cae y vuelve a arrancar con la misma BD ---
    central, prod = arrancar_central(ruta)
    assert estado(central) == "DISPONIBLE"         # aún no sabe nada del riego

    # El Engine sigue regando: su telemetría hace que CENTRAL adopte el riego.
    tel(central, m.telemetria("r1", "WS-04", 10.0, 1.2, 7, "FO-12", "FO"))
    assert estado(central) == "REGANDO"
    assert prod.ultimo(NOTIF, "TELEMETRIA")["fo_id"] == "FO-12"   # el FO sigue viéndolo
    # Mientras riega, nadie más puede pedir esa WS.
    pedir(central, "r2", fo="FO-07")
    assert prod.tipos(NOTIF, "FO-07") == ["PASO", "DENEGADA"]

    tel(central, m.finished("r1", "WS-04", "TIEMPO", 5.0, 30, "FO-12", "FO"))
    assert estado(central) == "DISPONIBLE"
    resumen = prod.ultimo(NOTIF, "RESUMEN")
    assert resumen["fo_id"] == "FO-12" and resumen["volumen_total_l"] == 5.0
    [riego] = central.bd.listar_riegos("WS-04")
    assert riego["req_id"] == "r1" and riego["fo_id"] == "FO-12"   # no se ha perdido


def test_riego_que_termina_con_central_caida_no_se_pierde(ruta):
    central, prod = arrancar_central(ruta)
    pedir(central, "r1")
    tel(central, m.started("r1", "WS-04"))
    # CENTRAL cae; mientras está caída el riego termina. Al volver, Kafka le
    # entrega el FINISHED que estaba esperando en el topic.
    central, prod = arrancar_central(ruta)
    tel(central, m.finished("r1", "WS-04", "TIEMPO", 5.0, 30, "FO-12", "FO"))
    assert prod.ultimo(NOTIF, "RESUMEN")["req_id"] == "r1"
    assert central.bd.listar_riegos("WS-04")[0]["volumen_total_l"] == 5.0
    assert estado(central) == "DISPONIBLE"


def test_central_vuelve_y_lo_primero_que_relee_es_el_started(ruta):
    """CENTRAL se cae de golpe antes de guardar su offset (lo hace cada 5 s):
    al volver relee el STARTED del riego. Tiene que adoptarlo CON su operario
    para que, al terminar, el FO reciba el RESUMEN (fallaba: el STARTED no
    llevaba fo_id y el riego quedaba sin operario)."""
    central, prod = arrancar_central(ruta)
    pedir(central, "r1")
    central, prod = arrancar_central(ruta)                       # caída
    tel(central, m.started("r1", "WS-04", "FO-12", "FO"))         # relee el STARTED
    assert estado(central) == "REGANDO"
    tel(central, m.telemetria("r1", "WS-04", 10.0, 1.2, 7, "FO-12", "FO"))
    assert prod.ultimo(NOTIF, "TELEMETRIA")["fo_id"] == "FO-12"
    tel(central, m.finished("r1", "WS-04", "TIEMPO", 5.0, 30, "FO-12", "FO"))
    assert prod.ultimo(NOTIF, "RESUMEN")["fo_id"] == "FO-12"
    [riego] = central.bd.listar_riegos("WS-04")
    assert riego["fo_id"] == "FO-12" and riego["origen"] == "FO"


def test_riego_adoptado_sigue_tras_autenticarse_el_monitor(ruta):
    central, prod = arrancar_central(ruta)
    pedir(central, "r1")
    tel(central, m.started("r1", "WS-04"))
    # Reinicio: la telemetría llega ANTES de que el Monitor se reconecte.
    bd = BD(str(ruta))
    bd.inicializar()
    central = Central(bd, Red(bd.listar_estaciones()))
    prod = ProducerFalso()
    central.kafka = KafkaCentral(central, "falso:9092", producer=prod)
    tel(central, m.telemetria("r1", "WS-04", 10.0, 1.2, 7, "FO-12", "FO"))
    assert estado(central) == "DESCONECTADA"
    est = central.red.get("WS-04")
    central.aplicar(est, est.autenticada)            # el Monitor vuelve
    assert estado(central) == "REGANDO"              # el riego no se olvida


def test_telemetria_atrasada_no_resucita_un_riego_cerrado(entorno):
    central, prod = entorno
    pedir(central, "r1")
    tel(central, m.started("r1", "WS-04"))
    tel(central, m.finished("r1", "WS-04", "TIEMPO", 5.0, 30, "FO-12", "FO"))
    tel(central, m.telemetria("r1", "WS-04", 10.0, 4.9, 29, "FO-12", "FO"))
    assert estado(central) == "DISPONIBLE"


def test_telemetria_atrasada_tras_reinicio_no_resucita(ruta):
    central, prod = arrancar_central(ruta)
    pedir(central, "r1")
    tel(central, m.started("r1", "WS-04"))
    tel(central, m.finished("r1", "WS-04", "TIEMPO", 5.0, 30, "FO-12", "FO"))
    central, prod = arrancar_central(ruta)           # riego ya guardado en BD
    tel(central, m.telemetria("r1", "WS-04", 10.0, 4.9, 29, "FO-12", "FO"))
    assert estado(central) == "DISPONIBLE"
    tel(central, m.finished("r1", "WS-04", "TIEMPO", 5.0, 30, "FO-12", "FO"))
    assert prod.tipos(NOTIF) == []                   # no se repite el RESUMEN


def test_started_tardio_de_una_reserva_caducada_sigue_recibiendo_stop(entorno):
    central, prod = entorno
    pedir(central, "r1")
    time.sleep(0.6)                                  # caduca la reserva
    tel(central, m.started("r1", "WS-04"))
    assert prod.ultimo(CMD, "STOP") is not None      # no se adopta
    assert estado(central) == "DISPONIBLE"


def test_riego_pedido_desde_el_panel_no_se_confunde_con_uno_desconocido(entorno):
    central, prod = entorno
    central.kafka.iniciar_riego("WS-04", 20)
    req = prod.ultimo(CMD, "START")["req_id"]
    tel(central, m.started(req, "WS-04"))
    assert estado(central) == "REGANDO"
    assert prod.ultimo(CMD, "STOP") is None


# --------------------- Engine caído: el estado final llega cuando se recupera
def test_estado_final_del_engine_corrige_la_bd(entorno):
    central, prod = entorno
    pedir(central, "r1")
    tel(central, m.started("r1", "WS-04"))
    tel(central, m.telemetria("r1", "WS-04", 10.0, 2.0, 12, "FO-12", "FO"))
    # El Engine se cae: el Monitor manda LEAK y CENTRAL cierra con 2.0 L.
    est = central.red.get("WS-04")
    central.aplicar(est, est.fuga_detectada, motivo="FUGA")
    assert central.bd.listar_riegos()[0]["volumen_total_l"] == 2.0
    # El Engine vuelve y manda el estado final que guardó en disco.
    tel(central, m.finished("r1", "WS-04", "FUGA", 2.17, 13, "FO-12", "FO"))
    riego = central.bd.listar_riegos()[0]
    assert riego["volumen_total_l"] == 2.17 and riego["duracion_s"] == 13
    assert central.foto()["estaciones"][0]["agua_total"] == 2.17
    assert prod.tipos(NOTIF, "FO-12").count("RESUMEN") == 1


# ------------------------------------------------ panel: agua suministrada
def test_agua_total_en_el_panel_y_tras_reiniciar(ruta):
    central, prod = arrancar_central(ruta)
    for req, litros in (("r1", 5.0), ("r2", 2.5)):
        pedir(central, req)
        tel(central, m.started(req, "WS-04"))
        tel(central, m.finished(req, "WS-04", "TIEMPO", litros, 30, "FO-12", "FO"))
    foto = central.foto()
    assert foto["estaciones"][0]["agua_total"] == 7.5
    assert "7.50 L" in texto_panel(foto, "prueba", parpadeo=False)

    # Mientras riega, el total incluye lo que lleva el riego en curso.
    pedir(central, "r3")
    tel(central, m.started("r3", "WS-04"))
    tel(central, m.telemetria("r3", "WS-04", 10.0, 1.0, 6, "FO-12", "FO"))
    assert central.foto()["estaciones"][0]["agua_total"] == 8.5

    central, prod = arrancar_central(ruta)           # se carga de la BD
    assert central.foto()["estaciones"][0]["agua_total"] == 7.5
    assert central.foto()["estaciones"][1]["agua_total"] == 0
