"""
Utilidades comunes a los programas de las estaciones y del operario
(WM_WS_M, WM_WS_E, WM_FO) y a tests/fake_central.py.

Estaban dentro de WM_WS_M.py. Se sacaron aquí al separar cada programa en su
carpeta (wm_ws/WS_M, wm_ws/WS_E, WM_FO): así el Engine y el FO no dependen de la
carpeta del Monitor, y cada Dockerfile solo necesita su carpeta y common/.
"""
import argparse
import socket
import threading
import time
from datetime import datetime

_lock_print = threading.Lock()


def log(texto: str) -> None:
    """print con la hora. Lleva lock para que dos hilos no mezclen sus líneas."""
    with _lock_print:
        print(f"[{datetime.now():%H:%M:%S}] {texto}", flush=True)


def ip_puerto(texto: str) -> tuple[str, int]:
    """'192.168.1.50:9000' -> ('192.168.1.50', 9000)."""
    ip, sep, puerto = texto.rpartition(":")
    if not sep or not ip or not puerto.isdigit():
        raise argparse.ArgumentTypeError(f"'{texto}' no tiene el formato ip:puerto")
    return ip, int(puerto)


class VigiaCentral:
    """Sabe si WM_Central está viva, sin hablar con ella directamente.

    El FO y el Engine solo hablan con Kafka, así que no ven un socket que se
    cae. Pero CENTRAL publica ESTADO_RED en wm.ws.status cada 5 s: si pasan
    SEG_SIN_CENTRAL segundos sin ninguno, la damos por caída. Con 15 s (tres
    publicaciones perdidas) un retraso puntual de Kafka no da falsas alarmas.

    Uso:
      - latido(): cada vez que llega un ESTADO_RED.
      - disponible(): True si ha llegado uno hace menos de SEG_SIN_CENTRAL.
      - arrancar(): hilo que avisa por pantalla solo en los CAMBIOS
        (cae / vuelve), no cada segundo.
    """

    SEG_SIN_CENTRAL = 15

    def __init__(self, seg_sin_central: float = SEG_SIN_CENTRAL, reloj=None):
        self.seg = seg_sin_central
        self.reloj = reloj or time.monotonic      # se cambia en los tests
        self.lock = threading.Lock()
        self.ultimo = None                         # monotonic del último ESTADO_RED
        self.inicio = self.reloj()
        self.caida_avisada = False

    def latido(self) -> None:
        with self.lock:
            self.ultimo = self.reloj()

    def disponible(self) -> bool:
        with self.lock:
            # Si aún no ha llegado ninguno, contamos desde que arrancamos:
            # recién arrancado no sabemos nada, pero no hay que alarmar aún.
            referencia = self.ultimo if self.ultimo is not None else self.inicio
            return self.reloj() - referencia < self.seg

    def comprobar(self) -> None:
        """Avisa por pantalla si ha cambiado el estado. La llama el hilo."""
        ok = self.disponible()
        if not ok and not self.caida_avisada:
            self.caida_avisada = True
            log(f"[ERROR] Imposible conectar con WM_Central: no da señales desde "
                f"hace más de {self.seg:g} s. No se pueden pedir riegos.")
        elif ok and self.caida_avisada:
            self.caida_avisada = False
            log("[OK] WM_Central vuelve a estar disponible")

    def arrancar(self, parar: threading.Event) -> None:
        def bucle():
            while not parar.wait(1):
                self.comprobar()
        threading.Thread(target=bucle, daemon=True).start()


def crear_servidor(puerto: int) -> socket.socket:
    """Socket servidor que escucha en 'puerto' en todas las interfaces (0.0.0.0).

    Si el puerto ya está ocupado, bind() lanza OSError con un error claro.
    - Linux: SO_REUSEADDR solo deja reabrir el puerto nada más cerrar el programa
      (sin esperar al estado TIME_WAIT). Es lo que queremos.
    - Windows: SO_REUSEADDR permite que DOS programas escuchen en el mismo puerto
      a la vez (las conexiones van a uno cualquiera). Por eso allí se usa
      SO_EXCLUSIVEADDRUSE, que hace justo lo contrario: el puerto es solo nuestro.
    """
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    if hasattr(socket, "SO_EXCLUSIVEADDRUSE"):        # solo existe en Windows
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
    else:
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("0.0.0.0", puerto))
    return srv
