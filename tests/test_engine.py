"""
Tests del Engine (WM_WS_E) sin Kafka: con un Producer falso que guarda lo
que se publica, y con un Monitor falso hecho con sockets de verdad.

Comprueban los arreglos de la guía de corrección:
  - sin Monitor la estación queda AVERIADA (corta el riego y rechaza riegos);
  - el riego en curso se guarda en disco y se recupera tras una caída;
  - TELEMETRIA y FINISHED llevan fo_id y origen;
  - el aviso de DENEGADA para los riegos pedidos desde la WS.

Ejecutar desde la raíz:  python -m pytest tests/test_engine.py
"""

import json
import os
import socket
import threading
import time

import pytest

from common import mensajes as m
from common.protocol import ACK, ENQ, enviar_trama, leer_byte, recibir_trama
from tests.test_kafka_central import ProducerFalso
from wm_ws.WS_E.WM_WS_E import Engine

TEL = m.T_TELEMETRY


@pytest.fixture
def engine(tmp_path):
    e = Engine(("127.0.0.1", 1), "falso:9092", dir_estado=str(tmp_path),
               producer=ProducerFalso())
    e.ws_id = "WS-04"                      # normalmente lo da el Monitor (HELLO)
    return e


def start(engine, req="r1", dur=30, fo="FO-12", origen="FO"):
    engine.atender_orden(m.start(req, "WS-04", fo, dur, origen))


def esperar(condicion, segundos=4):
    limite = time.time() + segundos
    while time.time() < limite:
        if condicion():
            return True
        time.sleep(0.05)
    return False


# ----------------------------------------------------- sin Monitor = AVERIADA
def test_sin_monitor_rechaza_el_start(engine):
    engine.monitor_perdido()
    start(engine)
    rechazo = engine.producer.ultimo(TEL, "START_REJECTED")
    assert rechazo["motivo"] == "AVERIADA"
    assert engine.riego is None


def test_sin_monitor_corta_el_riego_en_curso(engine):
    start(engine, dur=30)
    assert esperar(lambda: engine.producer.ultimo(TEL, "TELEMETRIA") is not None)
    riego = engine.riego
    engine.monitor_perdido()
    assert riego.terminado.wait(3)
    fin = engine.producer.ultimo(TEL, "FINISHED")
    assert fin["motivo"] == "DESCONEXION" and fin["fo_id"] == "FO-12"


def test_sin_monitor_no_deja_pedir_riego_manual(engine, monkeypatch, capsys):
    engine.monitor_perdido()
    # Si llegara a pedir la duración por teclado, el test fallaría.
    monkeypatch.setattr("builtins.input", lambda *a: pytest.fail("no debe preguntar"))
    engine.pedir_riego_manual()
    assert "AVERIADA" in capsys.readouterr().out
    assert engine.producer.tipos(m.T_REQUESTS) == []


def test_al_volver_el_monitor_admite_riegos(engine):
    engine.monitor_perdido()
    engine.monitor_recuperado()
    start(engine, dur=1)
    assert engine.producer.ultimo(TEL, "START_REJECTED") is None
    assert esperar(lambda: engine.producer.ultimo(TEL, "FINISHED") is not None)


def test_monitor_real_que_se_cae_deja_la_ws_averiada(engine):
    """Monitor falso con sockets de verdad: hace la presentación, manda
    HEALTH y después se "cae" (cierra el socket). El hilo de salud del Engine
    tiene que pasar a AVERIADA y, cuando el Monitor vuelve, recuperarse."""
    srv = socket.socket()
    srv.bind(("127.0.0.1", 0))
    srv.listen()
    engine.dir_monitor = srv.getsockname()
    conexiones = []

    def monitor_falso():
        for _ in range(2):                         # se cae y vuelve una vez
            conn, _ = srv.accept()
            conexiones.append(conn)
            assert leer_byte(conn) == ENQ
            conn.sendall(ACK)
            assert recibir_trama(conn) == "HELLO"
            enviar_trama(conn, "ID#WS-04")
            enviar_trama(conn, "HEALTH")
            recibir_trama(conn)                    # OK#HEALTH
            if len(conexiones) == 1:
                conn.close()                       # ¡Ctrl+C en el Monitor!

    # Se apunta cada cambio en vez de mirar engine.monitor_ok cada 50 ms: la
    # reconexión puede ser tan rápida (en Windows lo es) que el estado AVERIADA
    # dura menos de lo que tarda el test en volver a mirar.
    cambios = []
    perdido, recuperado = engine.monitor_perdido, engine.monitor_recuperado

    def apuntar_perdido():
        perdido()
        cambios.append(("perdido", engine.monitor_ok))

    def apuntar_recuperado():
        recuperado()
        cambios.append(("recuperado", engine.monitor_ok))

    engine.monitor_perdido = apuntar_perdido
    engine.monitor_recuperado = apuntar_recuperado

    threading.Thread(target=monitor_falso, daemon=True).start()
    sock = engine.conectar_monitor()
    threading.Thread(target=engine.hilo_salud, args=(sock,), daemon=True).start()
    # Tras la caída: AVERIADA (monitor_ok False). Al reconectar: otra vez bien.
    assert esperar(lambda: len(cambios) >= 2, 6)
    assert cambios[:2] == [("perdido", False), ("recuperado", True)]
    srv.close()


# ------------------------------------- riego en disco y recuperación tras caída
def test_el_riego_se_guarda_en_disco_y_se_borra_al_acabar(engine, tmp_path):
    ruta = tmp_path / "riego_WS-04.json"
    start(engine, dur=2)
    assert esperar(lambda: engine.producer.ultimo(TEL, "TELEMETRIA") is not None)
    datos = json.loads(ruta.read_text(encoding="utf-8"))
    assert datos["req_id"] == "r1" and datos["fo_id"] == "FO-12"
    assert datos["segundos"] >= 1 and datos["volumen_l"] > 0
    assert esperar(lambda: engine.producer.ultimo(TEL, "FINISHED") is not None)
    assert esperar(lambda: not ruta.exists(), 2)


def test_telemetria_y_finished_llevan_operario_y_origen(engine):
    start(engine, dur=1, fo=None, origen="CENTRAL")
    assert esperar(lambda: engine.producer.ultimo(TEL, "FINISHED") is not None)
    for tipo in ("TELEMETRIA", "FINISHED"):
        d = engine.producer.ultimo(TEL, tipo)
        assert d["origen"] == "CENTRAL" and d["fo_id"] is None


def test_recuperar_riego_tras_una_caida(engine, tmp_path):
    # Lo que dejaría en disco un Engine que muere a mitad de riego.
    (tmp_path / "riego_WS-04.json").write_text(json.dumps({
        "req_id": "r9", "ws_id": "WS-04", "fo_id": "FO-07", "origen": "FO",
        "duracion_s": 30, "volumen_l": 2.17, "segundos": 13}), encoding="utf-8")
    datos = engine.recuperar_riego_pendiente()
    assert datos["req_id"] == "r9"
    fin = engine.producer.ultimo(TEL, "FINISHED")
    assert fin == {**fin, "req_id": "r9", "motivo": "FUGA", "volumen_total_l": 2.17,
                   "duracion_s": 13, "fo_id": "FO-07", "origen": "FO"}
    assert not (tmp_path / "riego_WS-04.json").exists()
    assert engine.recuperar_riego_pendiente() is None   # ya no queda nada


def test_fichero_de_riego_roto_se_descarta(engine, tmp_path):
    (tmp_path / "riego_WS-04.json").write_text("{esto no es json", encoding="utf-8")
    assert engine.recuperar_riego_pendiente() is None
    assert engine.producer.enviados == []
    assert not os.path.exists(tmp_path / "riego_WS-04.json")


# ---------------------------------------------------- avisos de CENTRAL a la WS
def test_denegada_de_un_riego_pedido_desde_la_ws(engine, capsys):
    engine.atender_orden(m.denegada("r1", None, "WS-04", "WS-04 en FUGA"))
    salida = capsys.readouterr().out
    assert "[DENEGADA]" in salida and "WS-04 en FUGA" in salida


def test_sin_central_no_deja_pedir_riego_manual(engine, monkeypatch, capsys):
    engine.vigia.seg = 0                    # CENTRAL "callada" desde el principio
    monkeypatch.setattr("builtins.input", lambda *a: pytest.fail("no debe preguntar"))
    engine.pedir_riego_manual()
    assert "Imposible conectar con WM_Central" in capsys.readouterr().out
    assert engine.producer.tipos(m.T_REQUESTS) == []
