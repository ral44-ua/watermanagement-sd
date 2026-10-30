"""
Panel web de CENTRAL: el mismo panel que la consola, en el navegador, y los
botones de las órdenes del punto 11 del enunciado (iniciar riego, bloquear y
activar, en una WS o en todas).

Por qué web: en Railway CENTRAL no tiene pantalla ni teclado. Lo que escribe
solo aparece en los logs, y no se le puede escribir nada. Railway sí da una
dirección https pública a un puerto web, así que el panel y las órdenes se
sirven por ahí. En local también funciona: http://127.0.0.1:8080

Se usa http.server, de la biblioteca estándar de Python: no hace falta
instalar nada. ThreadingHTTPServer atiende cada petición en su propio hilo.

Rutas:
  GET  /             -> la página (panel.html). El navegador pide /api/estado
                        cada segundo con JavaScript y repinta la tabla.
  GET  /api/estado   -> JSON con la foto de la red (Central.foto()).
  POST /api/orden    -> JSON {"orden": "iniciar"|"bloquear"|"activar",
                              "ws_id": "WS-04" o "TODAS",
                              "duracion_s": 30 (solo para "iniciar")}

Seguridad: si se pasa una clave (variable WM_PANEL_CLAVE), se pide con la
autenticación básica de HTTP: el navegador muestra su ventana de usuario y
contraseña (el usuario da igual, se comprueba la contraseña).
"""

from __future__ import annotations      # "X | None" en Python 3.9

import base64
import hmac
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

HTML = Path(__file__).with_name("panel.html")
MAX_CUERPO = 10_000          # bytes; una orden ocupa menos de 100
MAX_DURACION = 3600          # s; límite razonable para un riego pedido a mano


class PanelWeb:
    def __init__(self, central, puerto: int, clave: str | None = None):
        self.central = central
        self.puerto = puerto
        self.clave = clave or None           # "" se trata como "sin clave"

    def arrancar(self) -> int:
        """Abre el puerto y atiende en un hilo aparte. Devuelve el puerto real
        (con puerto 0 lo elige el sistema: lo usan los tests)."""
        panel = self

        # El servidor crea un objeto Manejador por petición. Se define aquí
        # dentro para que vea "panel" (y con él la Central).
        class Manejador(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass                         # sin una línea por petición

            def do_GET(self):
                if not panel._autorizado(self):
                    return
                if self.path == "/":
                    self._responder(200, HTML.read_bytes(), "text/html; charset=utf-8")
                elif self.path == "/api/estado":
                    self._json(200, panel.central.foto())
                else:
                    self._json(404, {"ok": False, "mensaje": "No existe"})

            def do_POST(self):
                largo = int(self.headers.get("Content-Length") or 0)
                if largo > MAX_CUERPO:
                    # No leemos 10 KB+ de nadie; cerramos la conexión.
                    self.close_connection = True
                    return self._json(413, {"ok": False, "mensaje": "Demasiado grande"})
                # El cuerpo se lee SIEMPRE, antes de mirar la clave o la ruta.
                # Si respondiéramos 401/404 sin leerlo, al cerrar la conexión
                # quedarían bytes sin leer en el socket y Windows la corta de
                # golpe (RST): el navegador vería "conexión anulada" en vez de
                # la respuesta. Linux no lo hace, por eso solo fallaba en Windows.
                cuerpo = self.rfile.read(largo)
                if not panel._autorizado(self):
                    return
                if self.path != "/api/orden":
                    return self._json(404, {"ok": False, "mensaje": "No existe"})
                try:
                    orden = json.loads(cuerpo or b"{}")
                    codigo, mensaje = panel.ejecutar_orden(orden)
                except ValueError as ex:          # JSON roto o datos inválidos
                    codigo, mensaje = 400, str(ex)
                self._json(codigo, {"ok": codigo == 200, "mensaje": mensaje})

            def _json(self, codigo, datos):
                cuerpo = json.dumps(datos, ensure_ascii=False).encode("utf-8")
                self._responder(codigo, cuerpo, "application/json; charset=utf-8")

            def _responder(self, codigo, cuerpo, tipo, extra=None):
                self.send_response(codigo)
                self.send_header("Content-Type", tipo)
                self.send_header("Content-Length", str(len(cuerpo)))
                self.send_header("Cache-Control", "no-store")
                for k, v in (extra or {}).items():
                    self.send_header(k, v)
                self.end_headers()
                self.wfile.write(cuerpo)

        # 0.0.0.0: atiende en todas las interfaces (necesario en Docker/Railway).
        self.servidor = ThreadingHTTPServer(("0.0.0.0", self.puerto), Manejador)
        self.servidor.daemon_threads = True  # los hilos mueren al cerrar CENTRAL
        self.puerto = self.servidor.server_address[1]
        threading.Thread(target=self.servidor.serve_forever, daemon=True).start()
        self.central.log(f"Panel web en el puerto {self.puerto}"
                         + (" (con clave)" if self.clave else ""))
        return self.puerto

    # ------------------------------------------------------------ clave
    def _autorizado(self, manejador) -> bool:
        """Comprueba la clave. Si falta o es mala, responde 401 y devuelve False.

        La cabecera es "Authorization: Basic <base64 de usuario:clave>".
        hmac.compare_digest compara en tiempo constante: con == un atacante
        podría deducir la clave midiendo cuánto tarda la respuesta.
        """
        if self.clave is None:
            return True
        cabecera = manejador.headers.get("Authorization", "")
        if cabecera.startswith("Basic "):
            try:
                _, _, clave = base64.b64decode(cabecera[6:]).decode("utf-8").partition(":")
                if hmac.compare_digest(clave.encode("utf-8"), self.clave.encode("utf-8")):
                    return True
            except ValueError:               # base64 o UTF-8 inválidos
                pass
        manejador._responder(401, b"Clave necesaria", "text/plain; charset=utf-8",
                             {"WWW-Authenticate": 'Basic realm="WM_Central"'})
        return False

    # ----------------------------------------------------------- órdenes
    def ejecutar_orden(self, orden: dict) -> tuple[int, str]:
        """Valida la orden del navegador y la pasa a KafkaCentral.

        Todo lo que llega del navegador se comprueba: no nos fiamos de que la
        página mande siempre datos correctos (cualquiera puede hacer un POST).
        """
        if not isinstance(orden, dict):
            raise ValueError("La orden tiene que ser un objeto JSON")
        tipo = orden.get("orden")
        ws_id = orden.get("ws_id")
        if tipo not in ("iniciar", "bloquear", "activar"):
            raise ValueError("Orden desconocida")
        if not isinstance(ws_id, str):
            raise ValueError("Falta ws_id")
        todas = ws_id == "TODAS"
        if not todas and self.central.red.get(ws_id) is None:
            return 404, f"La estación {ws_id} no existe"
        kafka = self.central.kafka
        if kafka is None:
            return 503, "CENTRAL no está conectada a Kafka"

        if tipo == "iniciar":
            duracion = orden.get("duracion_s")
            # bool es un tipo de int en Python: True pasaría como 1 sin esto.
            if (not isinstance(duracion, int) or isinstance(duracion, bool)
                    or not 1 <= duracion <= MAX_DURACION):
                raise ValueError(f"La duración tiene que ser de 1 a {MAX_DURACION} s")
            self.central.log(f"Orden de CENTRAL: regar {ws_id} durante {duracion} s")
            if todas:
                kafka.iniciar_todas(duracion)
            else:
                kafka.iniciar_riego(ws_id, duracion)
        elif tipo == "bloquear":
            if todas:
                kafka.bloquear_todas()
            else:
                kafka.bloquear_ws(ws_id)
        else:
            if todas:
                kafka.activar_todas()
            else:
                kafka.activar_ws(ws_id)
        return 200, f"Orden '{tipo}' enviada a {'todas las estaciones' if todas else ws_id}"
