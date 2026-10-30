"""
WM_Central: central de control de WaterManagement.

Uso (desde la raíz del repo):
    python -m wm_central.WM_Central <puerto> <broker ip:puerto> [opciones]
    python -m wm_central.WM_Central 9000 127.0.0.1:9092

Opciones:
    --bd RUTA          fichero SQLite (por defecto wm_central.db)
    --puerto-web N     puerto del panel web (por defecto $PORT o 8080)
    --sin-panel        no pinta el panel en consola: escribe los eventos línea
                       a línea. Para Docker y Railway, donde no hay pantalla.

Al arrancar:
  1. Inicializa la BD (todas las WS salen DESCONECTADA).
  2. Carga la Red (máquina de estados) con las estaciones de la BD.
  3. Arranca la parte Kafka (peticiones, telemetría, avisos, estado de red).
  4. Arranca el servidor de sockets para los Monitores.
  5. Arranca el panel web: estado en tiempo real y órdenes (punto 11).
  6. Pinta el panel de consola cada segundo (si no se usa --sin-panel).

Panel (enunciado, "CENTRAL"): ID, ubicación, agua suministrada y estado con
su color de fondo: verde (disponible), verde parpadeando (regando, con caudal,
volumen y operario), rojo (fuga), naranja "Fuera de Servicio" y gris
(desconectada).
"""

from __future__ import annotations      # "X | None" en Python 3.9

import argparse
import os
import signal
import sys
import threading
import time
from collections import deque
from datetime import datetime

from wm_central.db import BD
from wm_central.estados import ORDEN_ESTADOS, Red
from wm_central.kafka_central import KafkaCentral
from wm_central.panel_web import PanelWeb
from wm_central.socket_server import ServidorSockets

# Texto que se enseña para cada estado (en BD y en Kafka va el código).
ETIQUETAS = {
    "DISPONIBLE": "Disponible",
    "RESERVADA": "Reservada",
    "REGANDO": "Regando",
    "FUGA": "Fuga",
    "FUERA_SERVICIO": "Fuera de Servicio",
    "DESCONECTADA": "Desconectada",
}


class Central:
    """Lo que comparten todos los hilos de CENTRAL: BD, red y log de eventos."""

    def __init__(self, bd: BD, red: Red):
        self.bd = bd
        self.red = red
        # Parte de Kafka. Mientras sea None, las acciones que piden mensajes
        # Kafka solo se anotan en el log (lo usan los tests de sockets).
        self.kafka = None
        self.eventos = deque(maxlen=12)       # últimos eventos para el panel
        self.lock_eventos = threading.Lock()
        # True con --sin-panel: cada evento se escribe también por pantalla,
        # para verlo en "docker logs" o en los logs de Railway.
        self.eco = False

    def log(self, texto: str) -> None:
        linea = f"[{datetime.now():%H:%M:%S}] {texto}"
        with self.lock_eventos:
            self.eventos.append(linea)
            if self.eco:
                print(linea, flush=True)

    def foto(self) -> dict:
        """Copia del estado de toda la red, para pintarla (consola y web).

        Se toma estación a estación con su lock, y luego se pinta sin tener
        ningún lock: pintar es lento y no debe frenar a los demás hilos.
        """
        estaciones = []
        for est in self.red.todas():
            with est.lock:
                estado = est.estado_visible()
                regando = estado == "REGANDO"
                estaciones.append({
                    "ws_id": est.ws_id,
                    "ubicacion": est.ubicacion,
                    "estado": estado,
                    "etiqueta": ETIQUETAS[estado],
                    # Caudal, volumen y operario solo tienen sentido regando.
                    "caudal": round(est.caudal, 1) if regando else None,
                    "volumen": round(est.volumen, 2) if regando else None,
                    "operario": (est.fo_actual or est.origen) if regando else None,
                    # Agua suministrada por la WS desde siempre: la de los
                    # riegos terminados (BD) más la del riego en curso.
                    "agua_total": round(est.agua_total + (est.volumen if regando else 0), 2),
                })
        resumen = {}
        for e in estaciones:
            resumen[e["estado"]] = resumen.get(e["estado"], 0) + 1
        with self.lock_eventos:
            eventos = list(self.eventos)
        return {"hora": f"{datetime.now():%H:%M:%S}", "estaciones": estaciones,
                "resumen": {e: resumen.get(e, 0) for e in ORDEN_ESTADOS},
                "eventos": eventos}

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


# ------------------------------------------------------- panel de consola
# Colores con códigos ANSI: "\033[<texto>;<fondo>m" cambia el color hasta el
# siguiente "\033[0m". 41 = fondo rojo, 42 = verde, 102 = verde claro,
# 100 = gris, 48;5;208 = naranja (paleta de 256 colores); 30 = texto negro,
# 97 = texto blanco. Funcionan en Git Bash, en Linux y en la consola de
# Windows 10/11 (allí hay que activarlos, ver activar_colores()).
FONDO = {
    "DISPONIBLE": "\033[30;42m",
    "RESERVADA": "\033[30;42m",
    "FUGA": "\033[97;41m",
    "FUERA_SERVICIO": "\033[30;48;5;208m",
    "DESCONECTADA": "\033[97;100m",
}
# "Verde parpadeando": cada segundo se alterna entre verde claro y verde.
REGANDO_A, REGANDO_B = "\033[30;102m", "\033[97;42m"
RESET = "\033[0m"
ANCHO = 95                       # ancho de cada fila del panel de consola
BORRAR_LINEA = "\033[K"          # borra desde el cursor hasta el final de la línea


def activar_colores() -> None:
    """En la consola de Windows (cmd, PowerShell), los códigos ANSI vienen
    desactivados. Ejecutar cualquier comando con os.system() los activa: es un
    truco conocido y no hace nada en Linux ni en Git Bash."""
    if os.name == "nt":
        os.system("")


def texto_panel(foto: dict, cabecera: str, parpadeo: bool) -> str:
    """Construye el panel entero como un único texto."""
    lineas = [f"=== WM_Central · {cabecera} · {foto['hora']} ===", "",
              f"{'ID':<8}{'UBICACIÓN':<22}{'ESTADO':<20}{'CAUDAL':>11}"
              f"{'VOLUMEN':>11}{'AGUA TOTAL':>13}  OPERARIO",
              "-" * ANCHO]
    for e in foto["estaciones"]:
        total = f"{e['agua_total']:>11.2f} L"
        if e["estado"] == "REGANDO":
            color = REGANDO_A if parpadeo else REGANDO_B
            datos = (f"{e['caudal']:>7.1f} L/m{e['volumen']:>9.2f} L{total}"
                     f"  {e['operario']}")
        else:
            color = FONDO[e["estado"]]
            datos = f"{'-':>11}{'-':>11}{total}  -"
        fila = f"{e['ws_id']:<8}{e['ubicacion'][:21]:<22}{e['etiqueta']:<20}{datos}"
        lineas.append(f"{color}{fila:<{ANCHO}}{RESET}")
    if not foto["estaciones"]:
        lineas.append("(no hay estaciones: arranca un WM_WS_M)")
    lineas += ["", " · ".join(f"{ETIQUETAS[k]}: {v}"
                              for k, v in foto["resumen"].items()),
               "", "Últimos eventos:"]
    lineas += ["  " + ev for ev in foto["eventos"]]
    return "\n".join(lineas)


def pintar_panel(central: Central, cabecera: str, parpadeo: bool) -> None:
    """Redibuja el panel encima del anterior.

    "\\033[H" lleva el cursor arriba del todo y "\\033[J" borra lo que quede
    debajo. Es mejor que borrar la pantalla entera (cls/clear) cada segundo,
    que hace que parpadee todo el panel y no solo las filas que riegan.
    """
    texto = texto_panel(central.foto(), cabecera, parpadeo)
    salida = "\033[H" + texto.replace("\n", BORRAR_LINEA + "\n") + BORRAR_LINEA + "\033[J"
    sys.stdout.write(salida)
    sys.stdout.flush()


def main():
    p = argparse.ArgumentParser(description="CENTRAL de WaterManagement")
    p.add_argument("puerto", type=int,
                   help="puerto del servidor de sockets para los WM_WS_M")
    p.add_argument("broker", help="ip:puerto del bróker Kafka")
    p.add_argument("--bd", default="wm_central.db", help="fichero SQLite")
    # Railway dice en la variable PORT en qué puerto espera la web.
    p.add_argument("--puerto-web", type=int,
                   default=int(os.environ.get("PORT", 8080)),
                   help="puerto del panel web (por defecto $PORT o 8080)")
    p.add_argument("--sin-panel", action="store_true",
                   help="sin panel de consola: eventos línea a línea (Docker)")
    args = p.parse_args()

    # docker stop y Railway (al redesplegar) paran el programa con la señal
    # SIGTERM. La convertimos en KeyboardInterrupt para cerrar igual que con
    # Ctrl+C: así se ejecuta el "finally" y Kafka se cierra bien.
    def al_recibir_sigterm(signum, frame):
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, al_recibir_sigterm)

    bd = BD(args.bd)
    bd.inicializar()
    red = Red(bd.listar_estaciones())
    central = Central(bd, red)
    central.eco = args.sin_panel
    central.log(f"CENTRAL arrancada: {len(red.todas())} estaciones en BD "
                f"(todas DESCONECTADA hasta que conecte su Monitor)")

    # Kafka antes que los sockets: así, cuando se autentique la primera WS,
    # ya se puede publicar su cambio de estado.
    central.kafka = KafkaCentral(central, args.broker)
    central.kafka.arrancar()
    ServidorSockets(central, args.puerto).arrancar()
    # Clave del panel web: si existe la variable WM_PANEL_CLAVE, el navegador
    # la pide antes de enseñar el panel (en Railway, el panel es público).
    PanelWeb(central, args.puerto_web, os.environ.get("WM_PANEL_CLAVE")).arrancar()

    try:
        if args.sin_panel:
            while True:
                time.sleep(3600)       # los hilos trabajan; aquí solo esperamos
        activar_colores()
        sys.stdout.write("\033[2J")    # pantalla limpia una vez al empezar
        cabecera = (f"sockets :{args.puerto} · Kafka {args.broker} · "
                    f"web http://127.0.0.1:{args.puerto_web}")
        parpadeo = False
        while True:
            pintar_panel(central, cabecera, parpadeo)
            parpadeo = not parpadeo
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        central.kafka.detener()
        # RESET: por si se cortó a mitad de una fila de color.
        print(("" if args.sin_panel else RESET) + "\nCENTRAL detenida.", flush=True)


if __name__ == "__main__":
    main()
