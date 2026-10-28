"""
WM_Central: central de control de WaterManagement.

Uso (desde la raíz del repo):
    python -m central.WM_Central <puerto> <broker ip:puerto> [--bd ruta.db]
    python -m central.WM_Central 9000 localhost:9092

Al arrancar:
  1. Inicializa la BD (todas las WS salen DESCONECTADA).
  2. Carga la Red (máquina de estados) con las estaciones de la BD.
  3. Arranca la parte Kafka (peticiones, telemetría, avisos, estado de red).
  4. Arranca el servidor de sockets para los Monitores.
  5. Pinta el panel cada 3 s.
"""

import argparse
import os
import threading
import time
from collections import deque
from datetime import datetime

from central.db import BD
from central.estados import ORDEN_ESTADOS, Red
from central.kafka_central import KafkaCentral
from central.socket_server import ServidorSockets


class Central:
    """Lo que comparten todos los hilos de CENTRAL: BD, red y log de eventos."""

    def __init__(self, bd: BD, red: Red):
        self.bd = bd
        self.red = red
        # Parte de Kafka (D5). Mientras sea None, las acciones que piden
        # mensajes Kafka solo se anotan en el log.
        self.kafka = None
        self.eventos = deque(maxlen=12)       # últimos eventos para el panel
        self.lock_eventos = threading.Lock()

    def log(self, texto: str) -> None:
        hora = datetime.now().strftime("%H:%M:%S")
        with self.lock_eventos:
            self.eventos.append(f"[{hora}] {texto}")

    def aplicar(self, est, evento, *args, motivo: str = "", **extra) -> list:
        """Ejecuta un evento de la máquina de estados y todo lo que conlleva.

        Ejemplo: central.aplicar(est, est.fuga_detectada, motivo="FUGA")

        Con el lock de la estación cogido: copia los datos del riego actual
        (el evento puede borrarlos y luego hacen falta para el RESUMEN) y
        lanza el evento. Después, ya sin el lock, tras_evento().
        """
        with est.lock:
            datos = est.datos_riego()
            acciones = evento(*args)
        self.tras_evento(est, acciones, datos, motivo, **extra)
        return acciones

    def tras_evento(self, est, acciones, datos=None, motivo="", **extra) -> None:
        """Guarda las banderas en BD y ejecuta las acciones del evento.

        IMPORTANTE: se llama SIN tener cogido el lock de ninguna estación.
        Publicar el estado de la red recorre todas las estaciones cogiendo su
        lock; si un hilo tuviera el de WS-04 y quisiera el de WS-05 mientras
        otro tiene el de WS-05 y quiere el de WS-04, se bloquearían los dos
        para siempre (interbloqueo).
        """
        # Leemos las banderas y las guardamos con el lock cogido: así se
        # guarda siempre el estado más reciente, aunque otro hilo cambie la
        # estación justo a la vez.
        with est.lock:
            self.bd.actualizar_flags(est.ws_id, **est.flags())
        if self.kafka is None:
            if acciones:
                self.log(f"{est.ws_id}: acciones pendientes de Kafka {acciones}")
            return
        try:
            self.kafka.ejecutar(est, acciones, datos, motivo, **extra)
            self.kafka.publicar_estado()
        except Exception as ex:      # un fallo de Kafka no debe tirar el hilo
            self.log(f"ERROR Kafka: {ex}")


# ---------------------------------------------------------------- panel
def pintar_panel(central: Central, cabecera: str) -> None:
    """Panel provisional en consola (el de colores llega después)."""
    os.system("cls" if os.name == "nt" else "clear")
    print(f"=== WM_Central · {cabecera} · {datetime.now():%H:%M:%S} ===\n")
    print(f"{'ID':<8}{'UBICACIÓN':<22}{'ESTADO':<16}{'CAUDAL':>9}{'VOLUMEN':>10}  OPERARIO")
    print("-" * 75)
    for est in central.red.todas():
        with est.lock:
            estado = est.estado_visible()
            regando = estado == "REGANDO"
            caudal = f"{est.caudal:.1f}" if regando else "-"
            volumen = f"{est.volumen:.1f}" if regando else "-"
            operario = (est.fo_actual or est.origen or "-") if regando else "-"
        print(f"{est.ws_id:<8}{est.ubicacion[:21]:<22}{estado:<16}"
              f"{caudal:>9}{volumen:>10}  {operario}")
    resumen = central.red.resumen()
    print("\n" + " · ".join(f"{e}: {resumen[e]}" for e in ORDEN_ESTADOS))
    print("\nÚltimos eventos:")
    with central.lock_eventos:
        for linea in central.eventos:
            print("  " + linea)


def main():
    p = argparse.ArgumentParser(description="CENTRAL de WaterManagement")
    p.add_argument("puerto", type=int,
                   help="puerto del servidor de sockets para los WM_WS_M")
    p.add_argument("broker", help="ip:puerto del bróker Kafka")
    p.add_argument("--bd", default="wm_central.db", help="fichero SQLite")
    args = p.parse_args()

    bd = BD(args.bd)
    bd.inicializar()
    red = Red(bd.listar_estaciones())
    central = Central(bd, red)
    central.log(f"CENTRAL arrancada: {len(red.todas())} estaciones en BD "
                f"(todas DESCONECTADA hasta que conecte su Monitor)")

    # Kafka antes que los sockets: así, cuando se autentique la primera WS,
    # ya se puede publicar su cambio de estado.
    central.kafka = KafkaCentral(central, args.broker)
    central.kafka.arrancar()

    ServidorSockets(central, args.puerto).arrancar()

    try:
        while True:
            pintar_panel(central, f"sockets :{args.puerto} · Kafka {args.broker}")
            time.sleep(3)
    except KeyboardInterrupt:
        print("\nCENTRAL detenida.")


if __name__ == "__main__":
    main()
