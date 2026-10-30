"""
Tests del registro de una WS con el Monitor (WM_WS_M) de verdad contra el
servidor de sockets de CENTRAL de verdad (en un puerto libre de localhost).

  - WS nueva: AUTH -> KO "WS no registrada" -> REGISTER -> AUTH -> OK.
  - WS ya registrada: AUTH directo.
  - Con --sin-registro: la denegación se ve y el Monitor termina.

Ejecutar desde la raíz:  python -m pytest tests/test_monitor_registro.py
"""

import pytest

from tests.test_socket_server import crear_central
from wm_ws.WS_M.WM_WS_M import Monitor


@pytest.fixture
def central(tmp_path):
    return crear_central(tmp_path, seg_sin_alive=5)


def test_ws_nueva_se_registra_y_se_autentica(central, capsys):
    mon = Monitor("WS-07", "Parque Lo Morant", ("127.0.0.1", central.puerto))
    mon.conectar_central(primera_vez=True)
    salida = capsys.readouterr().out
    assert "no está registrada" in salida and "registrada en CENTRAL" in salida
    assert central.bd.existe_ws("WS-07")
    assert central.red.get("WS-07").ubicacion == "Parque Lo Morant"
    assert central.red.get("WS-07").estado_visible() == "DISPONIBLE"
    mon.cerrar()


def test_ws_ya_registrada_entra_directamente(central, capsys):
    central.bd.registrar_ws("WS-07", "Parque Lo Morant")
    central.red.agregar("WS-07", "Parque Lo Morant")
    mon = Monitor("WS-07", "Parque Lo Morant", ("127.0.0.1", central.puerto))
    mon.conectar_central(primera_vez=True)
    assert "no está registrada" not in capsys.readouterr().out
    assert central.red.get("WS-07").estado_visible() == "DISPONIBLE"
    mon.cerrar()


def test_sin_registro_se_ve_la_denegacion(central, capsys):
    mon = Monitor("WS-07", "Parque Lo Morant", ("127.0.0.1", central.puerto),
                  registrar=False)
    with pytest.raises(SystemExit):
        mon.conectar_central(primera_vez=True)
    assert "WS no registrada" in capsys.readouterr().out
    assert not central.bd.existe_ws("WS-07")
