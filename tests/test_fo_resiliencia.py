"""
Tests de los arreglos del FO y del vigilante de CENTRAL (sin Kafka).

  - colocar_offsets: los avisos del operario siguen donde los dejó su grupo
    (así ve el RESUMEN que llegó mientras estaba cerrado); el estado de la
    red se lee desde el final.
  - Los avisos atrasados se marcan como [PENDIENTE].
  - Con CENTRAL caída no se envían peticiones y se avisa.

Ejecutar desde la raíz:  python -m pytest tests/test_fo_resiliencia.py
"""

from datetime import datetime, timedelta, timezone

from confluent_kafka import OFFSET_END, OFFSET_INVALID, TopicPartition

import WM_FO.WM_FO as wm_fo
from common import mensajes as m
from common.utilidades import VigiaCentral
from tests.test_kafka_central import ProducerFalso


class ConsumerFalso:
    def __init__(self):
        self.asignadas = None

    def assign(self, particiones):
        self.asignadas = particiones


class Reloj:
    """Reloj que avanza solo cuando el test quiere."""
    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t


def fo_de_prueba():
    return wm_fo.FO("FO-12", "falso:9092", producer=ProducerFalso())


# --------------------------------------------------------------- offsets
def test_avisos_desde_lo_guardado_y_estado_desde_el_final():
    consumer = ConsumerFalso()
    parts = [TopicPartition(m.T_NOTIFICATIONS, 0), TopicPartition(m.T_NOTIFICATIONS, 1),
             TopicPartition(m.T_STATUS, 0)]
    wm_fo.colocar_offsets(consumer, parts)
    por_topic = {(p.topic, p.partition): p.offset for p in consumer.asignadas}
    # Sin tocar = Kafka usa el offset que tenga guardado el grupo wm-fo-FO-12.
    assert por_topic[(m.T_NOTIFICATIONS, 0)] == OFFSET_INVALID
    assert por_topic[(m.T_NOTIFICATIONS, 1)] == OFFSET_INVALID
    assert por_topic[(m.T_STATUS, 0)] == OFFSET_END


# ------------------------------------------------- avisos atrasados (PENDIENTE)
def test_resumen_atrasado_se_marca_pendiente(capsys):
    fo = fo_de_prueba()
    viejo = m.resumen("r1", "FO-12", "WS-04", "TIEMPO", 5.0, 30)
    viejo["ts"] = (datetime.now(timezone.utc) - timedelta(minutes=2)).isoformat()
    fo.mostrar_notificacion(viejo)
    assert "[PENDIENTE] [WS-04] RESUMEN" in capsys.readouterr().out


def test_resumen_actual_no_se_marca(capsys):
    fo = fo_de_prueba()
    fo.mostrar_notificacion(m.resumen("r1", "FO-12", "WS-04", "TIEMPO", 5.0, 30))
    salida = capsys.readouterr().out
    assert "RESUMEN" in salida and "PENDIENTE" not in salida


# ---------------------------------------------------------- CENTRAL caída
def test_vigia_detecta_caida_y_vuelta(capsys):
    reloj = Reloj()
    v = VigiaCentral(seg_sin_central=15, reloj=reloj)
    assert v.disponible()                     # recién arrancado: aún no se sabe
    reloj.t += 16                             # 16 s sin ESTADO_RED
    assert not v.disponible()
    v.comprobar()
    v.comprobar()                             # el aviso sale una sola vez
    salida = capsys.readouterr().out
    assert salida.count("Imposible conectar con WM_Central") == 1
    v.latido()                                # vuelve a llegar ESTADO_RED
    assert v.disponible()
    v.comprobar()
    assert "vuelve a estar disponible" in capsys.readouterr().out
    reloj.t += 10
    assert v.disponible()                     # 10 s < 15 s
    reloj.t += 6
    assert not v.disponible()


def test_fo_no_envia_peticiones_con_central_caida(capsys):
    fo = fo_de_prueba()
    reloj = Reloj()
    fo.vigia = VigiaCentral(reloj=reloj)
    reloj.t += 20
    assert fo.pedir_riego("WS-04", 10, "r1") is False
    assert fo.producer.enviados == []
    assert "Imposible conectar con WM_Central" in capsys.readouterr().out
    fo.vigia.latido()
    assert fo.pedir_riego("WS-04", 10, "r2") is True
    assert fo.producer.tipos(m.T_REQUESTS, "WS-04") == ["SOLICITUD_RIEGO"]


def test_modo_fichero_no_se_queda_esperando_con_central_caida(tmp_path, monkeypatch):
    fo = fo_de_prueba()
    fo.vigia.seg = 0                          # CENTRAL callada
    monkeypatch.setattr(wm_fo, "PAUSA_ENTRE_PETICIONES", 0)
    ruta = tmp_path / "p.txt"
    ruta.write_text("WS-04#30\nWS-11#30\n", encoding="utf-8")
    inicio = datetime.now()
    fo.modo_fichero(str(ruta))
    # Sin el arreglo esperaría 30 + 60 s por petición.
    assert (datetime.now() - inicio).total_seconds() < 3
    assert fo.producer.enviados == []
