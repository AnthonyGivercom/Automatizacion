import pywhatkit
import pymssql
import pandas as pd
from datetime import datetime
#from pdf2image import convert_from_path

conexion = pymssql.connect(server='serverbd', user='login', password='password', database='database')
consulta = conexion.cursor()


def enviar_alerta_wsp (numero,embarque,cliente,puerto,etapod):
    hora_actual = datetime.now()
    hora = hora_actual.hour
    minuto = hora_actual.minute
    minuto = minuto + 1
    #numero = "+51989998429"
    mensaje = f"Hola, *{cliente}* \U0001F44B \n"
    mensaje = mensaje + f"Para informales que su embarque:\n" 
    mensaje = mensaje + "\n"
    mensaje = mensaje + f" \U0001F6A2 *{embarque}* arriba a \U00002693 *{puerto}* el día \U0001F5D3 *{etapod}* \n"
    mensaje = mensaje + f"Gracias por confiar en nosotros \U0001F91D \n"
    mensaje = mensaje + "\n"
    mensaje = mensaje + "Cualquier duda o consulta comunicarse a: \U0001F4E7 anthonyb@tpsac.com.ec "

    try:
        pywhatkit.sendwhatmsg(numero,mensaje,hora,minuto,10,True,5)

        pywhatkit.sendwhats_image(numero,"C:\\Users\\anthonyb\\Pictures\\logo.jpg","TRANSOCEAN LOG. CORP. S.A.",15,True,3)
        print('envio exitoso')
    except Exception as e:
        print('Error al enviar la imagen:', str(e))


def obtener_embarques():


    
    consulta.execute('USP_ALERTA_COMERCIAL')
    df = pd.DataFrame(consulta.fetchall()) 
    df.columns = [column[0] for column in consulta.description]
    print(df)
    return df

def ejecutar_mensajes(df):
    for indice, fila in df.iterrows():
        numero = fila['NUMERO']  
        embarque = fila['NROBL']
        cliente = fila['RAZSOCIAL']
        puerto = fila['PUERTO']
        fecha = fila['ETAPOD']
        try:
            enviar_alerta_wsp (numero,embarque,cliente,puerto,fecha)
        except Exception as e:
                print('Error al enviar mensaje:', str(e))



df = obtener_embarques()
ejecutar_mensajes(df)
#enviar_alerta_wsp ('','',"Anthony SAC",'','')
conexion.close()

