# Prompts de IA usados por Rubén

> El enunciado obliga a subir a `prompts/` los prompts de IA usados.
> Añade aquí cada prompt nuevo, con la fecha y el commit al que corresponde.

## 2026-10-22 · Claude · R1–R3 (protocolo, Monitor y Engine por sockets)

Contexto que se le dio a la IA: `CONTEXTO.md`, el documento de contratos entre módulos, el enunciado de la práctica, el tutorial de Kafka con Docker y la presentación de Kafka.

1. «Soy ruben y voy a empezar con mi parte del proyecto, guiame y dime porque hacer cada cosa» (R1)
2. «entonces tengo que subir ese archivo ?»
3. «y para que sirve el otro archivo?»
4. «pero esos comandos son para la rama main no para la mia»
5. «puedo hacer la R2 si la D1?»
6. «hazme r2» (R2)
7. «donde van los dos archivos?»
8. Se pegó el error de `socket.timeout` al ejecutar `fake_central` con Python 3.9 (corrección de R2).
9. «que se supone que tiene que pasar cuando pongo los comandos?»
10. Captura de las dos terminales: «asi se ha quedado» (corrección del puerto compartido en Windows).
11. «como llamo a este commit?»
12. «el R3 depende de alguna parte de david?» / «si» (R3)
13. «donde va el archivo» / «puedo probar que funcionan estos archivos?»
14. «R4 depende de alguno ?» / «con el gitpush ya se deberia mostrar en el repositorio de github?»

## 2026-10-23 / 2026-10-27 · Claude · R4 (Engine con Kafka)

15. «vale , d1 ya esta hecho , como junto lo mio con lo que ha hecho mi compañero y seguimos con lo siguiente?» (limpieza antes de fusionar, Kafka local y R4)
16. «Voy a hacer R4 asi que dime como juntar mi rama con la suya y hazme lo necesario para completar R4» (pasos para fusionar las ramas y ficheros finales de R4)

Qué se hizo con las respuestas: se revisó el código y se probó antes de subirlo (tests con pytest, pruebas a mano con el CENTRAL falso, el Monitor y el Engine, y con Kafka). Las pruebas de R4 detectaron que un Engine reiniciado volvía a ejecutar órdenes START antiguas; se corrigió.
