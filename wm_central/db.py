"""
Base de datos SQLite de CENTRAL.

Una sola conexión compartida por todos los hilos de CENTRAL (sockets,
Kafka, panel). Por eso:
  - check_same_thread=False: por defecto sqlite3 prohíbe usar la conexión
    desde un hilo distinto al que la creó. Lo desactivamos...
  - ...y a cambio TODAS las operaciones van dentro de self.lock, para que
    dos hilos no usen la conexión a la vez.
"""

import sqlite3
import threading
from pathlib import Path

# schema.sql está en la misma carpeta que este fichero, se ejecute desde donde se ejecute.
SCHEMA = Path(__file__).with_name("schema.sql")

# Operarios de prueba que se crean al arrancar si no existen.
OPERARIOS_PRUEBA = [("FO-01", "Operario 1"), ("FO-07", "Operario 7"),
                    ("FO-12", "Operario 12")]

# Columnas que actualizar_flags() permite tocar. Los nombres de columna no se
# pueden pasar como parámetro "?" en SQL, así que los metemos en el texto;
# con esta lista blanca evitamos que llegue cualquier otra cosa (inyección SQL).
CAMPOS_EDITABLES = {"conectada", "fuga", "bloqueada", "regando", "ultimo_riego"}


class BD:
    def __init__(self, ruta: str):
        self.conn = sqlite3.connect(ruta, check_same_thread=False)
        # Las filas se pueden leer por nombre de columna: fila["ws_id"].
        self.conn.row_factory = sqlite3.Row
        self.lock = threading.Lock()

    def inicializar(self) -> None:
        """Crea las tablas y deja las estaciones como al arrancar CENTRAL.

        Hasta que un Monitor no se conecte, CENTRAL no sabe el estado real de
        su WS: todas salen DESCONECTADA (conectada=0) y sin regar (regando=0).
        "bloqueada" SÍ se conserva: es una decisión de CENTRAL, no de la WS.
        """
        with self.lock:
            self.conn.executescript(SCHEMA.read_text(encoding="utf-8"))
            self.conn.execute("UPDATE estaciones SET conectada = 0, regando = 0")
            # INSERT OR IGNORE: si el operario ya existe, no hace nada.
            self.conn.executemany(
                "INSERT OR IGNORE INTO operarios (fo_id, nombre) VALUES (?, ?)",
                OPERARIOS_PRUEBA)
            self.conn.commit()

    # ------------------------------------------------------------ estaciones
    def registrar_ws(self, ws_id: str, ubicacion: str) -> bool:
        """Da de alta la estación si no existe. Devuelve True si era nueva."""
        with self.lock:
            cur = self.conn.execute(
                "INSERT OR IGNORE INTO estaciones (ws_id, ubicacion) VALUES (?, ?)",
                (ws_id, ubicacion))
            self.conn.commit()
            return cur.rowcount == 1          # 1 fila insertada = era nueva

    def existe_ws(self, ws_id: str) -> bool:
        with self.lock:
            fila = self.conn.execute(
                "SELECT 1 FROM estaciones WHERE ws_id = ?", (ws_id,)).fetchone()
            return fila is not None

    def actualizar_flags(self, ws_id: str, **flags) -> None:
        """Ejemplo: actualizar_flags("WS-04", conectada=1, fuga=0)."""
        if not flags:
            return
        desconocidos = set(flags) - CAMPOS_EDITABLES
        if desconocidos:
            raise ValueError(f"Campos no permitidos: {desconocidos}")
        # Construye "conectada = ?, fuga = ?" con los nombres que nos pasan.
        asignaciones = ", ".join(f"{campo} = ?" for campo in flags)
        valores = [int(v) if isinstance(v, bool) else v for v in flags.values()]
        with self.lock:
            self.conn.execute(
                f"UPDATE estaciones SET {asignaciones} WHERE ws_id = ?",
                (*valores, ws_id))
            self.conn.commit()

    def listar_estaciones(self) -> list[dict]:
        with self.lock:
            filas = self.conn.execute(
                "SELECT * FROM estaciones ORDER BY ws_id").fetchall()
            return [dict(f) for f in filas]

    # ------------------------------------------------------------- operarios
    def existe_operario(self, fo_id: str) -> bool:
        with self.lock:
            fila = self.conn.execute(
                "SELECT 1 FROM operarios WHERE fo_id = ?", (fo_id,)).fetchone()
            return fila is not None

    # ---------------------------------------------------------------- riegos
    def guardar_riego(self, req_id, ws_id, fo_id, origen, inicio, fin,
                      duracion_s, volumen_total_l, motivo) -> bool:
        """Guarda un riego terminado y actualiza ultimo_riego de la estación.

        Kafka entrega "al menos una vez": el mismo FINISHED podría llegar dos
        veces. Con INSERT OR IGNORE y req_id como clave primaria, el segundo
        no hace nada. Devuelve True si se ha guardado (era nuevo).
        """
        with self.lock:
            cur = self.conn.execute(
                "INSERT OR IGNORE INTO riegos (req_id, ws_id, fo_id, origen, "
                "inicio, fin, duracion_s, volumen_total_l, motivo) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (req_id, ws_id, fo_id, origen, inicio, fin,
                 duracion_s, volumen_total_l, motivo))
            nuevo = cur.rowcount == 1
            if nuevo:
                self.conn.execute(
                    "UPDATE estaciones SET ultimo_riego = ? WHERE ws_id = ?",
                    (fin, ws_id))
            self.conn.commit()
            return nuevo

    def listar_riegos(self, ws_id: str | None = None) -> list[dict]:
        with self.lock:
            if ws_id is None:
                filas = self.conn.execute("SELECT * FROM riegos").fetchall()
            else:
                filas = self.conn.execute(
                    "SELECT * FROM riegos WHERE ws_id = ?", (ws_id,)).fetchall()
            return [dict(f) for f in filas]

    def cerrar(self) -> None:
        with self.lock:
            self.conn.close()
