# Prompts de IA usados por David

> El enunciado obliga a subir a `prompts/` los prompts de IA usados.
> Añade aquí cada prompt nuevo, con la fecha y el commit al que corresponde.

## 2026-10-22 · Claude · D1–D5 (CENTRAL)

Contexto que se le dio a la IA: `CONTEXTO.md`, el documento de contratos entre módulos, el enunciado de la práctica, el tutorial de Kafka con Docker y la presentación de Kafka.

1. «soy david, dime cómo tengo que empezar con el proyecto»
2. «sigue lo que tienes en tu contexto unicamente»
3. «con los archivos que tengo que hacer?»
4. «dime como clonar el repositorio de github»
5. «Soy David, para que quede mas claro, vuelve a decirme detalladamente paso a paso como empezar y hacer tan solo unos 5 commits segun lo que pone en el archivo 'contexto' que me corresponde hacer, empieza desde que clono el repositorio de github hasta terminar el ultimo commit y guardar todo. acuerdate de explicarlo todo detalladamente y paso a paso, para que se entienda muy claro»

Qué se hizo con la respuesta: se revisó y se probó el código de cada commit (tests con pytest y pruebas a mano con Kafka) antes de subirlo.

## 2026-10-23 · Claude · resumen para el profesor

6. «ayer hice hasta d1 y mi compañero hizo hasta su d4, dime un breve resumen de lo que hemos hecho para decirle al profesor cuando venga a preguntar»
7. «explicame mas facilmente lo que he hecho ya hasta d1 y que es lo que voy a hacer yo en la practica»
8. «dime que le digo en 2 o 3 lineas»

## 2026-10-28 · Claude · D2–D5 (subir y probar)

9. «hola, el otro dia hice hasta d1, dime como seguir»
10. Dudas de Git al subir cada commit: el `git pull` cancelado por `docker-compose.yml`, si los commits iban a `main` o a mi rama, los avisos `LF will be replaced by CRLF`, si podía fusionar yo los pull requests.
11. El test `test_id_ya_conectado` fallaba en Windows (captura). Causa: `localhost` prueba primero IPv6 y cada conexión tardaba 2 s; se cambió a `127.0.0.1` y se separó el watchdog rápido en su propio test.
12. Prueba con el Monitor y el Engine de Rubén (flujos A, B, C y D) a partir de capturas de las terminales.
13. «cuanto me faltaria para terminar?»

## 2026-10-29 · Claude · E1–E3 (panel, Docker y Railway)

14. «mi compañero me ha dicho que ha terminado ya de hacer su parte mira lo que ha hecho el, y teniendo cuenta que ayer terminé con D5 explicame paso a paso igual que la vez anterior como terminar la práctica»

Qué se hizo: compatibilidad con Python 3.9 (aviso de Rubén), panel de consola con colores, panel web con las órdenes del punto 11, un solo consumidor Kafka con cierre ordenado (aviso de Rubén: CENTRAL tardaba ~100 s en atender tras arrancar), creación de topics al arrancar, Dockerfile y guía de Railway. Probado con tests (118, en Python 3.9 y 3.13) y con el Monitor, el Engine y el FO de Rubén sobre un Kafka simulado.

## 2026-10-30 · Claude · revisión con la guía de corrección

Contexto que se le dio a la IA: la guía de corrección (PDF), el enunciado, el contrato entre módulos y el repositorio.

1. «Comprueba si la practica cumple todos estos requisitos»
2. «haz lo que se tiene que arreglar y hazme los tests para comprobar y dime que tengo que cambiar y comprobar»

Qué se hizo: Dockerfiles de WM_WS_M, WM_WS_E y WM_FO; README; registro de WS separado del AUTH (REGISTER); alta automática de operarios nuevos; agua total en el panel; aviso a la WS cuando se deniega su riego manual; Engine AVERIADA sin Monitor; riego del Engine guardado en disco y recuperado tras una caída; el FO recibe los avisos atrasados tras reiniciarse; FO y Engine avisan "Imposible conectar con WM_Central"; CENTRAL recupera los riegos en curso tras reiniciarse. Tests nuevos en tests/ (pytest) y prueba con los procesos reales de CENTRAL, Monitor y Engine. Revisado y probado por nosotros antes de fusionar.
