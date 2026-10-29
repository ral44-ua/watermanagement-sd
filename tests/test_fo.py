"""Tests de la lectura de peticiones del FO (no necesitan Kafka)."""
import pytest

from fo.WM_FO import leer_fichero, leer_peticion


def test_peticion_valida():
    assert leer_peticion("WS-04#30") == ("WS-04", 30)


def test_peticion_con_espacios_y_salto_de_linea():
    assert leer_peticion("  WS-11 # 8 \n") == ("WS-11", 8)


@pytest.mark.parametrize("linea", [
    "WS04#30",        # sin guion: no coincidiría con la clave de Kafka
    "WS-04",          # falta la duración
    "WS-04#0",        # duración 0
    "WS-04#-5",       # negativa
    "WS-04#diez",     # no es un número
    "WS-04#30#x",     # campos de más
])
def test_peticiones_invalidas(linea):
    with pytest.raises(ValueError):
        leer_peticion(linea)


def test_fichero_salta_comentarios_vacias_y_erroneas(tmp_path):
    ruta = tmp_path / "peticiones.txt"
    ruta.write_text("# comentario\n\nWS-04#10\nmal\nWS-11#8\n", encoding="utf-8")
    assert leer_fichero(str(ruta)) == [("WS-04", 10), ("WS-11", 8)]
