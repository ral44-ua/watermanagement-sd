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


def entrar(central, ws="WS-04", sitio="River Park"):
    """Monitor que se da de alta (REGISTER) y se autentica (AUTH), como hace
    el WM_WS_M de verdad la primera vez. Devuelve el socket ya autenticado."""
    s = monitor(central)
    if not central.bd.existe_ws(ws):
        assert peticion(s, f"REGISTER#{ws}#{sitio}") == ["OK", "REGISTER"]
    assert peticion(s, f"AUTH#{ws}#{sitio}") == ["OK", "AUTH"]
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
    assert peticion(s, "REGISTER#WS-04#River Park") == ["OK", "REGISTER"]
    assert peticion(s, "AUTH#WS-04#River Park") == ["OK", "AUTH"]
    assert central.red.get("WS-04").estado_visible() == "DISPONIBLE"
    assert central.bd.existe_ws("WS-04")                 # registrada en BD
    assert central.bd.listar_estaciones()[0]["conectada"] == 1
    assert peticion(s, "ALIVE#WS-04") == ["OK", "ALIVE"]
    s.close()


def test_id_ya_conectado(central):
    s1 = entrar(central)
    s2 = monitor(central)
    assert peticion(s2, "AUTH#WS-04#River Park") == ["KO", "AUTH", "ID ya conectado"]
    s1.close()
    s2.close()


def test_sin_auth_se_rechaza(central):
    s = monitor(central)
    assert peticion(s, "ALIVE#WS-04")[0] == "KO"
    s.close()


def test_fuga_y_reparacion(central):
    s = entrar(central)
    assert peticion(s, "LEAK#WS-04#Engine KO") == ["OK", "LEAK"]
    assert central.red.get("WS-04").estado_visible() == "FUGA"
    assert central.bd.listar_estaciones()[0]["fuga"] == 1
    assert peticion(s, "RECOVERED#WS-04") == ["OK", "RECOVERED"]
    assert central.red.get("WS-04").estado_visible() == "DISPONIBLE"
    s.close()


def test_cerrar_monitor_desconecta(central):
    s = entrar(central)
    s.close()
    assert esperar_estado(central, "WS-04", "DESCONECTADA", 2)


def test_sin_alive_desconecta(central_rapida):
    s = entrar(central_rapida)
    # No mandamos ALIVE: el watchdog (1 s en este test) la desconecta.
    assert esperar_estado(central_rapida, "WS-04", "DESCONECTADA", 4)
    s.close()


def test_puede_volver_a_conectarse(central):
    s = entrar(central)
    s.close()
    assert esperar_estado(central, "WS-04", "DESCONECTADA", 2)
    s = entrar(central)                  # ya registrada: solo AUTH
    s.close()


# ------------------------------------------- registro (guía: "WS no registrada")
def test_auth_sin_registrar_se_deniega(central):
    s = monitor(central)
    assert peticion(s, "AUTH#WS-04#River Park") == ["KO", "AUTH", "WS no registrada"]
    assert not central.bd.existe_ws("WS-04")
    s.close()


def test_registrar_dos_veces_se_deniega(central):
    s = entrar(central)
    s2 = monitor(central)
    assert peticion(s2, "REGISTER#WS-04#Otro sitio") == ["KO", "REGISTER", "WS ya registrada"]
    s.close()
    s2.close()


def test_registro_con_id_invalido(central):
    s = monitor(central)
    assert peticion(s, "REGISTER#PEPE#River Park")[:2] == ["KO", "REGISTER"]
    s.close()


def test_registrada_sale_en_el_panel_tras_reiniciar(tmp_path):
    """Requisito: tras un reinicio, CENTRAL enseña las WS ya registradas como
    Desconectadas, con su ubicación."""
    c = crear_central(tmp_path, seg_sin_alive=5)
    entrar(c).close()
    c2 = crear_central(tmp_path, seg_sin_alive=5)       # misma BD: "reinicio"
    est = c2.red.get("WS-04")
    assert est.ubicacion == "River Park"
    assert est.estado_visible() == "DESCONECTADA"
