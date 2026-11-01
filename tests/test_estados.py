"""
Tests de la máquina de estados (WM_Central/estados.py). Sin red ni BD.

Ejecutar desde la raíz:  python -m pytest tests/test_estados.py
"""

import threading

from WM_Central import estados as e
from WM_Central.estados import Estacion, Red


def ws_disponible():
    est = Estacion("WS-04", "River Park")
    est.autenticada()
    return est


def ws_regando(req="r1"):
    est = ws_disponible()
    est.reservar(req, "FO-12", "FO", 30)
    est.valvula_abierta(req)
    return est


# ---------------------------------------------- los 4 mínimos del plan (D3)
def test_bloqueada_con_fuga_al_repararse_vuelve_a_naranja():
    est = ws_disponible()
    est.bloquear()
    est.fuga_detectada()
    assert est.estado_visible() == e.FUGA            # FUGA tiene prioridad
    est.reparada()
    assert est.estado_visible() == e.FUERA_SERVICIO  # naranja, NO verde


def test_pedir_riego_en_fuga_se_deniega():
    est = ws_disponible()
    est.fuga_detectada()
    ok, motivo = est.puede_regar()
    assert not ok and "FUGA" in motivo
    assert est.reservar("r1", "FO-07") == [e.ENVIAR_DENEGADA]


def test_dos_reservas_seguidas_la_segunda_se_deniega():
    est = ws_disponible()
    assert est.reservar("r1", "FO-12") == [e.ENVIAR_ACEPTADA, e.ENVIAR_START,
                                           e.ARRANCAR_TIMER]
    assert est.reservar("r2", "FO-07") == [e.ENVIAR_DENEGADA]
    assert est.req_actual == "r1"


def test_fuga_durante_riego_envia_stop():
    est = ws_regando()
    acciones = est.fuga_detectada()
    assert e.ENVIAR_STOP in acciones and e.CERRAR_RIEGO in acciones
    assert est.estado_visible() == e.FUGA
    assert not est.regando


# ------------------------------------------------------------ ciclo normal
def test_ciclo_normal():
    est = Estacion("WS-04", "River Park")
    assert est.estado_visible() == e.DESCONECTADA
    est.autenticada()
    assert est.estado_visible() == e.DISPONIBLE
    est.reservar("r1", "FO-12", "FO", 30)
    assert est.estado_visible() == e.RESERVADA
    assert est.valvula_abierta("r1") == [e.CANCELAR_TIMER]
    assert est.estado_visible() == e.REGANDO
    assert est.telemetria("r1", 11.4, 3.2, 17) is True
    assert est.fin_riego("r1") == [e.CERRAR_RIEGO]
    assert est.estado_visible() == e.DISPONIBLE


def test_finished_repetido_no_cierra_dos_veces():
    est = ws_regando()
    assert est.fin_riego("r1") == [e.CERRAR_RIEGO]
    assert est.fin_riego("r1") == []


def test_finished_despues_de_cortar_por_fuga_se_ignora():
    est = ws_regando()
    est.fuga_detectada()
    assert est.fin_riego("r1") == []          # el RESUMEN ya se mandó


def test_telemetria_de_otro_riego_se_ignora():
    est = ws_regando("r1")
    assert est.telemetria("otro", 10, 1, 1) is False


# ------------------------------------------------------- reserva y Engine
def test_timeout_de_reserva():
    est = ws_disponible()
    est.reservar("r1", "FO-12")
    assert est.timeout_reserva("r1") == [e.ENVIAR_DENEGADA]
    assert est.estado_visible() == e.DISPONIBLE


def test_timeout_despues_de_started_no_hace_nada():
    est = ws_regando("r1")
    assert est.timeout_reserva("r1") == []
    assert est.estado_visible() == e.REGANDO


def test_started_tarde_se_para():
    est = ws_disponible()
    est.reservar("r1", "FO-12")
    est.timeout_reserva("r1")
    assert est.valvula_abierta("r1") == [e.ENVIAR_STOP]


def test_start_rechazado():
    est = ws_disponible()
    est.reservar("r1", "FO-12")
    assert est.start_rechazado("r1") == [e.CANCELAR_TIMER, e.ENVIAR_DENEGADA]
    assert est.estado_visible() == e.DISPONIBLE


# ------------------------------------------------- bloqueo y desconexión
def test_bloquear_y_activar():
    est = ws_disponible()
    assert est.bloquear() == [e.ENVIAR_BLOCK]
    assert est.estado_visible() == e.FUERA_SERVICIO
    assert est.puede_regar()[0] is False
    assert est.activar() == [e.ENVIAR_ACTIVATE]
    assert est.estado_visible() == e.DISPONIBLE


def test_bloquear_durante_riego_lo_corta():
    est = ws_regando()
    assert est.bloquear() == [e.ENVIAR_BLOCK, e.ENVIAR_STOP, e.CERRAR_RIEGO]


def test_fuga_durante_reserva():
    est = ws_disponible()
    est.reservar("r1", "FO-12")
    assert est.fuga_detectada() == [e.CANCELAR_TIMER, e.ENVIAR_STOP,
                                    e.ENVIAR_DENEGADA]


def test_desconectar_durante_riego():
    est = ws_regando()
    acciones = est.desconectar()
    assert e.CERRAR_RIEGO in acciones
    assert est.estado_visible() == e.DESCONECTADA
    assert est.desconectar() == []            # segunda vez: nada


def test_desconectada_tiene_prioridad_sobre_todo():
    est = ws_disponible()
    est.bloquear()
    est.fuga_detectada()
    est.desconectar()
    assert est.estado_visible() == e.DESCONECTADA


def test_bloqueada_en_bd_al_conectar_sale_naranja():
    est = Estacion("WS-04", "River Park", bloqueada=1)
    est.autenticada()
    assert est.estado_visible() == e.FUERA_SERVICIO


def test_datos_riego_antes_de_cortar():
    est = ws_regando()
    est.telemetria("r1", 10.0, 2.5, 15)
    datos = est.datos_riego()
    est.fuga_detectada()
    assert datos["req_id"] == "r1" and datos["volumen"] == 2.5
    assert est.datos_riego() is None


# -------------------------------------------------------- concurrencia
def test_muchos_hilos_reservando_solo_gana_uno():
    est = ws_disponible()
    ganadores = []

    def pedir(i):
        if e.ENVIAR_START in est.reservar(f"r{i}", "FO-12"):
            ganadores.append(i)

    hilos = [threading.Thread(target=pedir, args=(i,)) for i in range(50)]
    for h in hilos:
        h.start()
    for h in hilos:
        h.join()
    assert len(ganadores) == 1


# ------------------------------------------------------------------- Red
def test_red_desde_bd_y_resumen():
    filas = [
        {"ws_id": "WS-04", "ubicacion": "River Park", "conectada": 0,
         "fuga": 0, "bloqueada": 0, "regando": 0},
        {"ws_id": "WS-11", "ubicacion": "Riverside Gardens", "conectada": 0,
         "fuga": 0, "bloqueada": 1, "regando": 0},
    ]
    red = Red(filas)
    assert red.resumen()[e.DESCONECTADA] == 2
    red.get("WS-04").autenticada()
    red.get("WS-11").autenticada()
    r = red.resumen()
    assert r[e.DISPONIBLE] == 1 and r[e.FUERA_SERVICIO] == 1
    nueva = red.agregar("WS-20", "Central Park")
    assert red.get("WS-20") is nueva
    assert [x["ws_id"] for x in red.lista_estado()] == ["WS-04", "WS-11", "WS-20"]
