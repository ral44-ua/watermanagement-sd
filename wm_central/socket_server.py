"""
Servidor de sockets de CENTRAL para los Monitores (WM_WS_M).

Protocolo (contrato, apartado 3, conexión A):
  - Al conectar: el Monitor envía ENQ y CENTRAL responde ACK.
  - Después, tramas <STX>DATA<ETX><LRC> con campos separados por '#':
        REGISTER#WS-04#River Park -> OK#REGISTER o KO#REGISTER#motivo
                                     (alta de una WS nueva en la BD)
        AUTH#WS-04#River Park  -> OK#AUTH  o  KO#AUTH#motivo
                                  (KO#AUTH#WS no registrada si no se dio de alta)
        ALIVE#WS-04            -> OK#ALIVE           (cada 2 s)
        LEAK#WS-04#Engine KO   -> OK#LEAK
        RECOVERED#WS-04        -> OK#RECOVERED
  - La conexión es persistente: si se cae, o pasan 5 s sin ALIVE, la WS
    pasa a DESCONECTADA.

Hilos:
  - uno que acepta conexiones;
  - uno por cada Monitor conectado (cada socket se atiende en su hilo, así
    un Monitor lento no frena a los demás);
  - un watchdog que cada segundo busca estaciones sin ALIVE.
"""

import re
import socket
import threading
import time

from common.protocol import ACK, ENQ, NACK, enviar_trama, leer_byte, recibir_trama

SEG_SIN_ALIVE = 5        # contrato: sin ALIVE durante 5 s -> DESCONECTADA


class ServidorSockets:
    def __init__(self, central, puerto: int, seg_sin_alive: float = SEG_SIN_ALIVE):
        self.central = central          # objeto Central (WM_Central.py)
        self.puerto = puerto
        self.seg_sin_alive = seg_sin_alive
        # ws_id -> socket de su Monitor. Lo usa el watchdog para cerrar el
        # socket de una WS caducada. Varios hilos lo tocan: lleva lock.
        self.conexiones = {}
        self.lock_conexiones = threading.Lock()

    # ------------------------------------------------------------- arranque
    def arrancar(self) -> int:
        """Abre el puerto y lanza los hilos. Devuelve el puerto real."""
        self.servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # Mismo criterio que el Monitor de Rubén (crear_servidor en WM_WS_M):
        #  - Linux: SO_REUSEADDR deja reabrir el puerto nada más reiniciar
        #    CENTRAL, sin el "Address already in use" de unos segundos.
        #  - Windows: SO_REUSEADDR permitiría que DOS programas escuchen en el
        #    mismo puerto a la vez (con CENTRAL abierta dos veces, cada Monitor
        #    iría a una al azar). SO_EXCLUSIVEADDRUSE hace el puerto solo nuestro.
        if hasattr(socket, "SO_EXCLUSIVEADDRUSE"):        # solo existe en Windows
            self.servidor.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
        else:
            self.servidor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.servidor.bind(("0.0.0.0", self.puerto))   # todas las interfaces
        self.servidor.listen()
        self.puerto = self.servidor.getsockname()[1]
        # daemon=True: los hilos mueren solos al cerrar CENTRAL.
        threading.Thread(target=self._aceptar, daemon=True).start()
        threading.Thread(target=self._watchdog, daemon=True).start()
        self.central.log(f"Servidor de sockets escuchando en el puerto {self.puerto}")
        return self.puerto

    def _aceptar(self):
        while True:
            conn, addr = self.servidor.accept()
            threading.Thread(target=self._atender, args=(conn, addr),
                             daemon=True).start()

    # ------------------------------------------------- un hilo por Monitor
    def _atender(self, conn, addr):
        ws_id = None                    # se rellena cuando el AUTH es correcto
        try:
            # El ENQ se lee aparte: recibir_trama() descarta todo lo que no
            # sea STX, así que si lo usáramos el ENQ se perdería sin responder.
            if leer_byte(conn) != ENQ:
                conn.sendall(NACK)
                return
            conn.sendall(ACK)
            self.central.log(f"Conexión de un Monitor desde {addr[0]}")
            while True:
                campos = recibir_trama(conn).split("#")
                respuesta, ws_id = self._despachar(campos, ws_id, conn)
                enviar_trama(conn, respuesta)
        except (OSError, ValueError):
            # OSError incluye ConnectionError (socket cerrado), timeout y el
            # IOError de enviar_trama (sin ACK tras 3 intentos).
            # ValueError: trama que no es UTF-8 válido.
            pass
        finally:
            conn.close()
            self._conexion_perdida(ws_id, conn)

    def _conexion_perdida(self, ws_id, conn):
        if ws_id is None:
            return
        with self.lock_conexiones:
            # Solo desconectamos si este socket sigue siendo el de esa WS.
            # Si el watchdog ya la desconectó, o el Monitor ya se ha vuelto a
            # conectar con otro socket, no hay que tocar nada.
            if self.conexiones.get(ws_id) is not conn:
                return
            del self.conexiones[ws_id]
        est = self.central.red.get(ws_id)
        self.central.log(f"{ws_id}: socket del Monitor cerrado -> DESCONECTADA")
        self.central.aplicar(est, est.desconectar, motivo="DESCONEXION")

    # ----------------------------------------------------- tipos de trama
    def _despachar(self, campos, ws_id, conn):
        """Procesa una trama. Devuelve (respuesta, ws_id de esta conexión)."""
        codigo = campos[0]

        if codigo == "AUTH":
            return self._auth(campos, ws_id, conn)
        if codigo == "REGISTER":
            return self._registrar(campos), ws_id

        if codigo not in ("ALIVE", "LEAK", "RECOVERED"):
            return f"KO#{codigo}#Código desconocido", ws_id
        # Para todo lo demás hay que estar autenticado y con el mismo ID.
        if ws_id is None:
            return f"KO#{codigo}#No autenticado", ws_id
        if len(campos) < 2 or campos[1] != ws_id:
            return f"KO#{codigo}#El ID no coincide con el autenticado", ws_id

        est = self.central.red.get(ws_id)
        if codigo == "ALIVE":
            est.alive()                 # solo apunta la hora: no cambia el estado
        elif codigo == "LEAK":
            motivo = campos[2] if len(campos) > 2 else "sin detalle"
            self.central.log(f"{ws_id}: FUGA detectada ({motivo})")
            self.central.aplicar(est, est.fuga_detectada, motivo="FUGA")
        elif codigo == "RECOVERED":
            self.central.log(f"{ws_id}: fuga reparada")
            self.central.aplicar(est, est.reparada)
        return f"OK#{codigo}", ws_id

    def _auth(self, campos, ws_id, conn):
        if ws_id is not None:
            return "KO#AUTH#Esta conexión ya está autenticada", ws_id
        if len(campos) < 3 or not campos[1] or not campos[2]:
            return "KO#AUTH#Formato: AUTH#<ws_id>#<ubicacion>", None
        nuevo_id = campos[1]

        # Solo se autentica una WS dada de alta antes (REGISTER). Es el caso
        # de denegación que pone de ejemplo la guía de corrección.
        if not self.central.bd.existe_ws(nuevo_id):
            self.central.log(f"{nuevo_id}: autenticación DENEGADA (WS no registrada)")
            return "KO#AUTH#WS no registrada", None
        est = self.central.red.get(nuevo_id)
        if est is None:          # en BD pero no en memoria (no debería pasar)
            est = self.central.red.agregar(nuevo_id, "#".join(campos[2:]))

        # Comprobar y marcar como conectada dentro del lock de la estación:
        # si dos Monitores con el mismo ID llegan a la vez, solo entra uno.
        with est.lock:
            if est.conectada:
                return "KO#AUTH#ID ya conectado", None
            acciones = est.autenticada()
        with self.lock_conexiones:
            self.conexiones[nuevo_id] = conn
        # tras_evento fuera del lock de la estación (ver Central.tras_evento).
        self.central.tras_evento(est, acciones)
        self.central.log(f"{nuevo_id}: autenticada ({est.estado_visible()})")
        return "OK#AUTH", nuevo_id

    def _registrar(self, campos):
        """REGISTER#<ws_id>#<ubicacion>: alta de una estación nueva.

        Registrarse y autenticarse son dos cosas distintas (enunciado:
        "registrarse en CENTRAL" y "recibir peticiones de registro y alta"):
          - REGISTER guarda la estación en la BD, una sola vez en su vida.
          - AUTH abre la sesión de una estación ya registrada, cada vez que su
            Monitor arranca o se reconecta.
        """
        if len(campos) < 3 or not campos[1] or not campos[2]:
            return "KO#REGISTER#Formato: REGISTER#<ws_id>#<ubicacion>"
        ws_id, ubicacion = campos[1], "#".join(campos[2:])
        if not re.fullmatch(r"WS-\d+", ws_id):
            return "KO#REGISTER#ID no válido (formato WS-<número>)"
        if not self.central.bd.registrar_ws(ws_id, ubicacion):
            return "KO#REGISTER#WS ya registrada"
        self.central.red.agregar(ws_id, ubicacion)
        self.central.log(f"{ws_id}: estación NUEVA registrada ({ubicacion})")
        if self.central.kafka is not None:
            try:
                self.central.kafka.publicar_estado()     # los FO la ven ya
            except Exception as ex:
                self.central.log(f"ERROR Kafka: {ex}")
        return "OK#REGISTER"

    # ------------------------------------------------------------ watchdog
    def _watchdog(self):
        """Cada segundo: la WS que lleve más de 5 s sin ALIVE -> DESCONECTADA."""
        while True:
            time.sleep(1)
            ahora = time.monotonic()
            for est in self.central.red.todas():
                with est.lock:
                    caducada = (est.conectada and
                                ahora - est.ultimo_alive > self.seg_sin_alive)
                if not caducada:
                    continue
                with self.lock_conexiones:
                    conn = self.conexiones.pop(est.ws_id, None)
                self.central.log(f"{est.ws_id}: {self.seg_sin_alive} s sin ALIVE -> DESCONECTADA")
                self.central.aplicar(est, est.desconectar, motivo="DESCONEXION")
                if conn is not None:
                    # shutdown despierta al hilo que está bloqueado en recv()
                    # de ese socket; así termina y libera la conexión.
                    try:
                        conn.shutdown(socket.SHUT_RDWR)
                    except OSError:
                        pass
                    conn.close()
