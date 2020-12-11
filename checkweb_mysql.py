'''
Nombre del archivo: cheackweb.py
Archivo creado por: Natalia López García y Raúl París Murillo
Fecha: 10/11/2020
Ejemplo de ejecución: python3 script1.py dominios.csv
Formato fichero de dominios: Un dominio por cada linea
Salida: Fichero logsDominios.csv y contentOutput.txt
Parametros a ajustar en código: NUM_THREADS para establecer el número de hilos deseados
'''

import pandas as pd
import requests
import time
import json
import threading
import math
import queue
import sys
import mysql.connector

requests.packages.urllib3.disable_warnings(
    requests.packages.urllib3.exceptions.InsecureRequestWarning)

NUM_THREADS = 100
TABLE_NAME_LOGDOMINIOS = "logDominios"
TABLE_NAME_LOGCONTENT = "logContent"

df = pd.read_csv(sys.argv[1], header=None)
listaDominios = df.iloc[:].values.tolist()
longitudLista = len(listaDominios)

usaHilos = True
dominiosPorHilo = 1
dominiosRestantes = 0
if longitudLista < NUM_THREADS:
    usaHilos = False
else:
    dominiosPorHilo = math.floor(longitudLista/NUM_THREADS)
    dominiosRestantes = int(longitudLista % NUM_THREADS)


def CreaLogs(dominios: list):
    df = pd.DataFrame(columns=['Tiempo', 'Dominio', 'IpResol',
                               'httpResponsecode80', 'httpResponsecode443'])
    listaContenido = list()
    for dominio in dominios:
        contenido = ""
        try:
            r = requests.get(
                'https://'+dominio[0], stream=True, timeout=2, verify=False)  # Por defecto va al puerto 80
            r2 = requests.get(
                'https://'+dominio[0]+':443', stream=True, timeout=2, verify=False)
            tiempoLocal = time.localtime()
            tiempo = time.strftime("%Y/%m/%d %H:%M", tiempoLocal)
            ip = r.raw._fp.fp.raw._sock.getpeername()[0]
            body = "<body" + \
                str(r.content).split("<body")[1].split("body>")[0] + "body>"
            head = "<head" + \
                str(r.content).split("<head")[1].split("head>")[0] + "head>"
            body443 = "<body" + \
                str(r2.content).split("<body")[1].split("body>")[0] + "body>"
            head443 = "<head" + \
                str(r2.content).split("<head")[1].split("head>")[0] + "head>"
            contenido = dominio[0] + ":80" + "\n" + head + "\n" + body + \
                "\n" + dominio[0] + ":443" + "\n" + \
                head443 + "\n" + body443 + "\n"

            listaContenido.append(contenido)
            df = df.append({'Tiempo': tiempo, 'Dominio': dominio[0], 'IpResol': ip,
                            'httpResponsecode80': r.status_code, 'httpResponsecode443': r2.status_code}, ignore_index=True)
        except:
            if dominio[0] == "www.as.com":
                try:  # Por defecto va al puerto 80
                    r = requests.get(
                        'https://'+dominio[0], stream=True, timeout=2, verify=False)
                    r2 = requests.get(
                        'https://'+dominio[0]+':443', stream=True, timeout=2, verify=False)
                    tiempoLocal = time.localtime()
                    tiempo = time.strftime("%Y/%m/%d %H:%M", tiempoLocal)
                    ip = r.raw._fp.fp.raw._sock.getpeername()[0]
                    body = "<body" + \
                        str(r.content).split("<BODY")[
                            1].split("body>")[0] + "body>"
                    head = "<head" + \
                        str(r.content).split("<head")[
                            1].split("head>")[0] + "head>"
                    body443 = "<body" + \
                        str(r2.content).split("<BODY")[
                            1].split("body>")[0] + "body>"
                    head443 = "<head" + \
                        str(r2.content).split("<head")[
                            1].split("head>")[0] + "head>"
                    contenido = dominio[0] + ":80" + "\n" + head + "\n" + body + \
                        "\n" + dominio[0] + ":443" + "\n" + \
                        head443 + "\n" + body443 + "\n"

                    listaContenido.append(contenido)
                    df = df.append({'Tiempo': tiempo, 'Dominio': dominio[0], 'IpResol': ip,
                                    'httpResponsecode80': r.status_code, 'httpResponsecode443': r2.status_code}, ignore_index=True)
                except:
                    pass
            else:
                pass
    return df, listaContenido


listaDataframes = []
listaContenidos = []
dfTotal = pd.DataFrame(columns=['Tiempo', 'Dominio', 'IpResol',
                                'httpResponsecode80', 'httpResponsecode443'])

if usaHilos == True:
    threads = []
    que = queue.Queue()
    utiliceResto = 0
    dominiosRestantesAux = dominiosRestantes
    for ii in range(0, NUM_THREADS):
        if dominiosRestantesAux > 0:  # Si quedan dominios restantes, se reparte 1 dominio a cada hilo hasta terminarlos
            inicio = ii*dominiosPorHilo+utiliceResto
            final = (ii+1)*dominiosPorHilo+utiliceResto+1
            t = threading.Thread(target=lambda q, arg1: q.put(
                CreaLogs(arg1)), args=(que, listaDominios[inicio:final]))
            dominiosRestantesAux -= 1
            utiliceResto = 1
        else:  # Cuando no quedan dominios restantes, cada hilo solo toma dominiosPorHilo dominios
            inicio = ii*dominiosPorHilo+dominiosRestantes
            final = (ii+1)*dominiosPorHilo+dominiosRestantes
            t = threading.Thread(target=lambda q, arg1: q.put(
                CreaLogs(arg1)), args=(que, listaDominios[inicio:final]))
            utiliceResto = 0
        t.start()
        threads.append(t)

    [t.join() for t in threads]
    for tResponse in threads:
        [valor1, valor2] = que.get()
        listaDataframes.append(valor1)
        listaContenidos.append(valor2)
    dfTotal = pd.concat(listaDataframes, ignore_index=True)
else:
    valor1, valor2 = CreaLogs(listaDominios)
    dfTotal = valor1
    listaContenidos.append(valor2)

'''dfTotal.to_csv('logsDominios.csv', index=False, header=True)
text_file = open("contentOutput.txt", "w")
contenidoTotal = ""
for contenido in listaContenidos:
    contenidoAux = '-'.join(contenido)
    contenidoTotal = contenidoTotal+contenidoAux
text_file.write(contenidoTotal)
text_file.close()'''

# Conexion con mysql
mydb = mysql.connector.connect(
    host="mysql",
    user="raul",
    password="pass",
    database="pyteam"
)

mycursor = mydb.cursor()

mycursor.execute("CREATE TABLE IF NOT EXISTS " + TABLE_NAME_LOGDOMINIOS +
                 " (id int(11) NOT NULL AUTO_INCREMENT,PRIMARY KEY (id),Tiempo DATETIME, Dominio VARCHAR(255), IpResol VARCHAR(255), httpResponsecode80 INT, httpResponsecode443 INT)")

mycursor.execute("CREATE TABLE IF NOT EXISTS " + TABLE_NAME_LOGCONTENT +
                 " (id int(11) NOT NULL AUTO_INCREMENT,PRIMARY KEY (id),Dominio VARCHAR(255), Puerto INT, Head MEDIUMTEXT, Body MEDIUMTEXT)")

mycursor.execute("SHOW TABLES")

tablasDisponibles = list()
[tablasDisponibles.append(x[0].decode()) for x in mycursor]

if TABLE_NAME_LOGDOMINIOS in tablasDisponibles and TABLE_NAME_LOGCONTENT in tablasDisponibles:
    print("Las tablas están listas para insertar")

val = []
listaDominiosTotales = dfTotal.iloc[:].values.tolist()

[val.append((str(jj[0]).replace("/", "-")+":00", str(jj[1]),
             str(jj[2]), str(jj[3]), str(jj[4]))) for jj in listaDominiosTotales]

sql = "INSERT INTO " + TABLE_NAME_LOGDOMINIOS + \
    " (Tiempo, Dominio, IpResol, httpResponsecode80, httpResponsecode443) VALUES (%s, %s, %s, %s, %s)"
mycursor.executemany(sql, val)
print(mycursor.rowcount, "Lineas insertadas en logDominios")

val = []
for listaContenidosHilos in listaContenidos:
    for contenido in listaContenidosHilos:
        dominio = contenido.split(":")[0]
        puerto1 = "80"
        puerto2 = "443"
        head = "<head" + \
            contenido.split("<head")[1].split("head>")[0] + "head>"
        body = "<body" + \
            contenido.split("<body")[1].split("body>")[0] + "body>"
        head2 = "<head" + \
            contenido.split(
                dominio+":"+puerto2)[1].split("<head")[1].split("head>")[0] + "head>"
        body2 = "<body" + \
            contenido.split(
                dominio+":"+puerto2)[1].split("<body")[1].split("body>")[0] + "body>"
        val.append((dominio, puerto1, head, body))
        val.append((dominio, puerto2, head2, body2))

sql = "INSERT INTO " + TABLE_NAME_LOGCONTENT + \
    " (Dominio, Puerto, Head, Body) VALUES (%s, %s, %s, %s)"
mycursor.executemany(sql, val)
print(mycursor.rowcount, "Lineas insertadas en logContent")

mydb.commit()
