-- Base de datos de CENTRAL (SQLite).
-- "IF NOT EXISTS": se puede ejecutar en cada arranque sin borrar nada.

-- Estaciones de riego. Guardamos 4 banderas independientes (0/1) y el estado
-- que se pinta se calcula a partir de ellas (ver central/estados.py).
-- "reservada" NO se guarda: es un estado de pocos segundos que solo vive en memoria.
CREATE TABLE IF NOT EXISTS estaciones (
    ws_id         TEXT PRIMARY KEY,          -- "WS-04"
    ubicacion     TEXT NOT NULL,             -- "River Park"
    conectada     INTEGER NOT NULL DEFAULT 0,
    fuga          INTEGER NOT NULL DEFAULT 0,
    bloqueada     INTEGER NOT NULL DEFAULT 0,
    regando       INTEGER NOT NULL DEFAULT 0,
    ultimo_riego  TEXT                       -- ISO 8601 del fin del último riego
);

-- Operarios dados de alta (los FO solo pueden pedir si existen aquí).
CREATE TABLE IF NOT EXISTS operarios (
    fo_id   TEXT PRIMARY KEY,                -- "FO-12"
    nombre  TEXT NOT NULL
);

-- Histórico de riegos. req_id es el UUID de la petición: al ser clave
-- primaria, un mismo riego no se puede guardar dos veces.
CREATE TABLE IF NOT EXISTS riegos (
    req_id           TEXT PRIMARY KEY,
    ws_id            TEXT NOT NULL,
    fo_id            TEXT,                   -- NULL si lo pidió la propia WS
    origen           TEXT NOT NULL,          -- "FO", "WS" o "CENTRAL"
    inicio           TEXT,                   -- ISO 8601
    fin              TEXT,                   -- ISO 8601
    duracion_s       INTEGER,
    volumen_total_l  REAL,
    motivo           TEXT                    -- TIEMPO, MANUAL, FUGA, BLOQUEO, DESCONEXION
);
