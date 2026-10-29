"""
Tests del servidor de sockets de CENTRAL con un Monitor falso.

Se abre CENTRAL de verdad en un puerto libre de localhost y un "Monitor"
hecho con common/protocol.py (R1 de Rubén) le manda tramas reales.

Ejecutar desde la raíz:  python -m pytest tests/test_socket_server.py
"""

import socket
import time

import pytest

from wm_central.WM_Central import Central
from wm_central.db import BD
from wm_central.estados import Red
from wm_central.socket_server import ServidorSockets
from common.protocol import ACK, ENQ, peticion


def crear_central(tmp_path, seg_sin_alive):
    bd = BD(str(tmp_path / "c.db"))
    bd.inicializar()
    c = Central(bd, Red(bd.listar_estaciones()))
    # Puerto 0 = el sistema elige uno libre.
    c.puerto = ServidorSockets(c, 0, seg_sin_alive=seg_sin_alive).arrancar()
    return c


@pytest.fixture
def central(tmp_path):
    # 5 s sin ALIVE, como en el contrato. Estos tests no mandan ALIVE, así que
    # con menos tiempo el watchdog podría desconectar la WS en mitad del test.
    return crear_central(tmp_path, seg_sin_alive=5)


@pytest.fixture
def central_rapida(tmp_path):
    # Solo para probar el watchdog: 1 s sin ALIVE, para no esperar 5 s.
    return crear_central(tmp_path, seg_sin_alive=1)


def monitor(central):
    """Conecta como un Monitor: ENQ -> ACK."""
    # 127.0.0.1 y no "localhost": en Windows, "localhost" prueba primero IPv6
    # (::1), donde CENTRAL no escucha, y tarda ~2 s en pasar a IPv4. Con el
    # watchdog a 1 s de los tests, la WS se desconectaría durante esa espera.
    s = socket.create_connection(("127.0.0.1", central.puerto), timeout=5)
    s.sendall(ENQ)
    assert s.recv(1) == ACK
    return s


def esperar_estado(central, ws_id, estado, segundos=4):
    limite = time.time() + segundos
    while time.time() < limite:
        est = central.red.get(ws_id)
        if est and est.estado_visible() == estado:
            return True
        time.sleep(0.05)
    return False


def test_flujo_a_autenticacion(central):
    s = monitor(central)
    assert peticion(s, "AUTH#WS-04#River Park") == ["OK", "AUTH"]
    assert central.red.get("WS-04").estado_visible() == "DISPONIBLE"
    assert central.bd.existe_ws("WS-04")                 # registrada en BD
    assert central.bd.listar_estaciones()[0]["conectada"] == 1
    assert peticion(s, "ALIVE#WS-04") == ["OK", "ALIVE"]
    s.close()


def test_id_ya_conectado(central):
    s1 = monitor(central)
    peticion(s1, "AUTH#WS-04#River Park")
    s2 = monitor(central)
    assert peticion(s2, "AUTH#WS-04#River Park") == ["KO", "AUTH", "ID ya conectado"]
    s1.close()
    s2.close()


def test_sin_auth_se_rechaza(central):
    s = monitor(central)
    assert peticion(s, "ALIVE#WS-04")[0] == "KO"
    s.close()


def test_fuga_y_reparacion(central):
    s = monitor(central)
    peticion(s, "AUTH#WS-04#River Park")
    assert peticion(s, "LEAK#WS-04#Engine KO") == ["OK", "LEAK"]
    assert central.red.get("WS-04").estado_visible() == "FUGA"
    assert central.bd.listar_estaciones()[0]["fuga"] == 1
    assert peticion(s, "RECOVERED#WS-04") == ["OK", "RECOVERED"]
    assert central.red.get("WS-04").estado_visible() == "DISPONIBLE"
    s.close()


def test_cerrar_monitor_desconecta(central):
    s = monitor(central)
    peticion(s, "AUTH#WS-04#River Park")
    s.close()
    assert esperar_estado(central, "WS-04", "DESCONECTADA", 2)


def test_sin_alive_desconecta(central_rapida):
    s = monitor(central_rapida)
    peticion(s, "AUTH#WS-04#River Park")
    # No mandamos ALIVE: el watchdog (1 s en este test) la desconecta.
    assert esperar_estado(central_rapida, "WS-04", "DESCONECTADA", 4)
    s.close()


def test_puede_volver_a_conectarse(central):
    s = monitor(central)
    peticion(s, "AUTH#WS-04#River Park")
    s.close()
    assert esperar_estado(central, "WS-04", "DESCONECTADA", 2)
    s = monitor(central)
    assert peticion(s, "AUTH#WS-04#River Park") == ["OK", "AUTH"]
    s.close()
