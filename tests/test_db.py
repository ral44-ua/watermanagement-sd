"""
Tests de WM_Central/db.py. Cada test usa un fichero .db temporal (tmp_path lo
crea pytest y lo borra al acabar), así no se toca la BD real.

Ejecutar desde la raíz:  python -m pytest tests/test_db.py
"""

import pytest

from WM_Central.db import BD


@pytest.fixture
def bd(tmp_path):
    base = BD(str(tmp_path / "prueba.db"))
    base.inicializar()
    yield base
    base.cerrar()


def test_operarios_de_prueba(bd):
    for fo in ("FO-01", "FO-07", "FO-12"):
        assert bd.existe_operario(fo)
    assert not bd.existe_operario("FO-99")


def test_inicializar_dos_veces_no_duplica(bd):
    bd.inicializar()                       # no debe fallar ni duplicar
    assert bd.existe_operario("FO-12")


def test_registrar_ws(bd):
    assert bd.registrar_ws("WS-04", "River Park") is True     # nueva
    assert bd.registrar_ws("WS-04", "Otro sitio") is False    # ya existía
    assert bd.existe_ws("WS-04")
    assert not bd.existe_ws("WS-99")
    [ws] = bd.listar_estaciones()
    assert ws["ubicacion"] == "River Park"                    # no se ha cambiado
    assert ws["conectada"] == 0 and ws["fuga"] == 0


def test_actualizar_flags(bd):
    bd.registrar_ws("WS-04", "River Park")
    bd.actualizar_flags("WS-04", conectada=True, fuga=1)
    [ws] = bd.listar_estaciones()
    assert ws["conectada"] == 1 and ws["fuga"] == 1 and ws["bloqueada"] == 0


def test_actualizar_flags_rechaza_campos_desconocidos(bd):
    bd.registrar_ws("WS-04", "River Park")
    with pytest.raises(ValueError):
        bd.actualizar_flags("WS-04", ubicacion="x; DROP TABLE estaciones")


def test_reinicio_deja_estaciones_desconectadas(tmp_path):
    ruta = str(tmp_path / "reinicio.db")
    bd = BD(ruta)
    bd.inicializar()
    bd.registrar_ws("WS-04", "River Park")
    bd.actualizar_flags("WS-04", conectada=1, regando=1, bloqueada=1)
    bd.cerrar()

    bd = BD(ruta)                          # "reiniciamos" CENTRAL
    bd.inicializar()
    [ws] = bd.listar_estaciones()
    assert ws["ws_id"] == "WS-04"          # la estación sigue en la BD
    assert ws["conectada"] == 0            # pero sale DESCONECTADA
    assert ws["regando"] == 0
    assert ws["bloqueada"] == 1            # el bloqueo de CENTRAL se conserva
    bd.cerrar()


def test_guardar_riego_una_sola_vez(bd):
    bd.registrar_ws("WS-04", "River Park")
    args = dict(req_id="r1", ws_id="WS-04", fo_id="FO-12", origen="FO",
                inicio="2026-10-22T14:00:00+00:00",
                fin="2026-10-22T14:00:30+00:00",
                duracion_s=30, volumen_total_l=5.4, motivo="TIEMPO")
    assert bd.guardar_riego(**args) is True
    assert bd.guardar_riego(**args) is False          # duplicado de Kafka
    assert len(bd.listar_riegos("WS-04")) == 1
    [ws] = bd.listar_estaciones()
    assert ws["ultimo_riego"] == "2026-10-22T14:00:30+00:00"
