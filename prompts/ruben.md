# Prompts de IA usados por Rubén


## · R1–R3 (protocolo, Monitor y Engine por sockets)
1. «Explícame cómo funciona la comunicación mediante sockets TCP entre el Monitor y CENTRAL y qué información deben intercambiar».
2. «Soy ruben y voy a empezar con mi parte del proyecto, guiame.»
3. «entonces tengo que subir ese archivo ?»
4. «¿Cómo puedo comprobar que el Monitor detecta correctamente cuando el Engine se desconecta o deja de enviar información?».
5. «puedo hacer la R2 si la D1?»
6. «donde van los dos archivos?»
7. «¿Cómo puedo evitar que el Engine vuelva a ejecutar órdenes antiguas cuando se reinicia?».
8. «como llamo a este commit?»
9. «el R3 depende de alguna parte de david?»
10. «donde va el archivo» / «puedo probar que funcionan estos archivos?»
11. «R4 depende de alguno ?» / «con el gitpush ya se deberia mostrar en el repositorio de github?»
12. «Revisa los posibles errores de mi implementación y propón pruebas para comprobar que el sistema sigue funcionando cuando se desconecta o reinicia alguno de sus componentes».

## · (Engine con Kafka) y estructura de carpetas

13. «vale , d1 ya esta hecho , como junto lo mio con lo que ha hecho mi compañero y seguimos con lo siguiente?»
14. «Ayúdame a implementar un consumidor Kafka en el Engine que reciba las órdenes de riego enviadas por CENTRAL y procese cada mensaje correctamente».
15. «¿Cómo puedo configurar los topics de Kafka que necesita el Engine para recibir órdenes y enviar información de telemetría?».
16. «Ayúdame a implementar el procesamiento de mensajes JSON recibidos desde Kafka, comprobando que contienen los campos necesarios antes de ejecutar una orden»
17. «Ayúdame a crear un fichero de configuración para el Engine donde pueda definir la dirección del broker Kafka, los nombres de los topics y otros parámetros necesarios sin tener que modificar el código».

## · Fusión de R4, R5 (FO) y reestructuración

18. «Metete al repositorio de github y dime como va el proyecto y que me falta a mi como ruben»
19. «ayudame a fusionar R4 y hacemos R5»
20. «¿Cómo puedo fusionar correctamente las funcionalidades de R4 y R5 del Front Office?»
21. «Revisa la estructura del proyecto y propón mejoras para facilitar su mantenimiento»

Pruebas de R5: tests de la lectura del fichero con pytest, y prueba completa con Kafka y un
CENTRAL falso que contesta a las peticiones (modo fichero con una petición aceptada, una
denegada y una línea errónea; modo manual).


