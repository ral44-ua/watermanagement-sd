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
