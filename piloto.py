from playwright.sync_api import sync_playwright
from datetime import datetime
import time
import schedule
#import pyodbc 
import pymssql
import pandas as pd
import os
import playwright 


data_puerto =[]
data_etd_fecha = []
data_etd_hora = []
data_eta_fecha = []
data_eta_hora = []

def limpiar_variables ():
    global data_puerto
    global data_etd_fecha 
    global data_etd_hora
    global data_eta_fecha 
    global data_eta_hora

    data_puerto =[]
    data_etd_fecha = []
    data_etd_hora = []
    data_eta_fecha = []
    data_eta_hora = []

def scrape_Linescape(nave,viaje,linea):
    with sync_playwright() as p:
        chromium_executable_path = "C:/Users/anthonyb/AppData/Local/ms-playwright/chromium-1067/chrome-win/chrome.exe"
        #p.firefox.executable_path ="C:\\Users\\anthonyb\\AppData\\Local\\ms-playwright\\firefox-1408\\firefox\\firefox.exe"
        browser = p.chromium.launch(headless=True,executable_path=chromium_executable_path)
        page = browser.new_page()
        Compaginacion = []
        pagina = []
        link = 'http://www.linescape.com'
        page.goto(link)
        page.wait_for_load_state('networkidle')

        #cierra la busqueda schudules
        xpath_boton = '//ul[@class="search-buttons open"]'
        boton_vessels = page.locator(xpath_boton)
        boton_vessels.click()
        page.wait_for_load_state('networkidle')

        #habilita busqueda por nave
        xpath_vessels = '//li="Vessels""]'
        boton_vessels2 = page.locator(xpath_vessels)
        page.wait_for_load_state('networkidle')

        #ingresa la nave a buscar
        page.fill('input', nave)
        elemento2 = page.wait_for_selector('//input')
        elemento2.click()
        page.wait_for_load_state('networkidle')
        elemento3 = page.wait_for_selector('//div[@class="auto-complete"]//li')
        elemento3.click()
        elemento = page.wait_for_selector('//div[@class="col-holder"]//a[@class="btn"]')
        elemento.click()
        page.wait_for_load_state('networkidle')


        xpath_compaginacion = '//li/following::div[@class="pagination-holder"]'
        #xpath_compaginacion ='//div[@class="pagination-holder"]//li[@_ngcontent-c13=""]'
        elementos_compaginacion = page.query_selector_all(xpath_compaginacion)
        #print (elementos_compaginacion)

        for elemento in elementos_compaginacion:
            valor = elemento.inner_text()
            if valor:
                Compaginacion.append(valor)

        pagina = [i + 1 for i in range(len(Compaginacion))]
        #print (pagina)
        for numero in pagina:
        #xpath_compaginacion_otro ='//div[@class="pagination-holder"]//li[contains(text(),2)]'

            elemento4 = page.wait_for_selector(f'//div[@class="pagination-holder"]//li[contains(text(),{numero})]')
            elemento4.click()
            page.wait_for_load_state('networkidle')

            seleccionar_dato = f'//b[contains(text(),"{viaje}")]/ancestor::div[@class="results-header-col col-holder"]//a//img'
        
            try:
                seleccionar = page.wait_for_selector(seleccionar_dato)
                seleccionar.click()
                xpath_puerto = f'//b[contains(text(),"{viaje}")]/following::div[@class="col-holder service-ports"]//a'
                elementos_puerto = page.query_selector_all(xpath_puerto)

                for elemento in elementos_puerto:
                    valor = elemento.inner_text()
                    data_puerto.append(valor)

                xpath_etd= f'//b[contains(text(),"{viaje}")]/following::div[@class="column2 etaDateTime"]//span'
                elementos_etd = page.query_selector_all(xpath_etd)

                for i, elemento in enumerate(elementos_etd):
                    dato = elemento.inner_text()
                    if i % 2 == 0:
                        data_etd_fecha.append(dato)
                    else:
                        data_etd_hora.append(dato)

                xpath_eta= f'//b[contains(text(),"{viaje}")]/following::div[@class="column3 etaDateTime"]//span'
                elementos_eta = page.query_selector_all(xpath_eta)

                for i, elemento in enumerate(elementos_eta):
                    dato2 = elemento.inner_text()
                    if i % 2 == 0:
                        data_eta_fecha.append(dato2)
                    else:
                        data_eta_hora.append(dato2)

                page.wait_for_timeout(10000)
                print(data_puerto)
                print(data_etd_fecha)
                print(data_etd_hora)
                print(data_eta_fecha)
                print(data_eta_hora)
                ruta = '//data'
                browser.close()
                break
            except Exception as e:
                print(f'Error al buscar el viaje {viaje} en la pagina {numero}:')

def itinerario(servidor,usuario,clave,bd):
    try:
        conexion = pymssql.connect(server=servidor, user=usuario, password=clave, database=bd)
        consulta = conexion.cursor()
        print('conexion exitosa')

        consulta.execute('USP_TRAZABILIDAD_NAVE_VIAJE') 

        df = pd.DataFrame(consulta.fetchall()) 
        df.columns = [column[0] for column in consulta.description]
        #df.columns = [column[1] for column in consulta.description]
        print(df)
        #columna = df['OCURRE']
        #fila = df.loc[3]
        #print(fila)

        for indice, fila in df.iterrows():
            nave = fila['NAVE']
            
            viaje = fila['VIAJE']
            #linea = fila['LINEA']
            try:
                limpiar_variables ()
                scrape_Linescape(nave,viaje,"prueba")
                print(nave, viaje)
                consulta_sql = f"exec USP_SET_ITINERARIO_CABECERA @NAVE='{nave}', @VIAJE='{viaje}'"
                #consulta.execute('USP_SET_ITINERARIO_CABECERA @NAVE=%s, @VIAJE=%s', (nave,viaje))
                try:
                    graba_cabecera_sql = consulta.execute(consulta_sql)
                    print (data_etd_fecha)
                    #print('rows inserted: ' + str(graba_cabecera_sql))
                    resultado = consulta.fetchone()
                    # Acceder a los valores de cada columna
                    id = resultado[0]
                    conexion.commit()
                    #print(id)
                    for x in range (0,len(data_etd_fecha)):
                        puerto = data_puerto[x]
                        fecha_etd =data_etd_fecha [x]
                        hora_etd = data_etd_hora  [x]
                        fecha_eta =data_eta_fecha [x]
                        hora_eta = data_eta_hora [x] 
                        grabar_detalle_sql = f"exec USP_SET_ITINERARIO_DETALLE @id='{id}', @puerto='{puerto}',@etd='{fecha_etd}{hora_etd}', @eta='{fecha_eta}{hora_eta}'"
                        grabar_sql = consulta.execute(grabar_detalle_sql)
                        print(grabar_detalle_sql)
                        conexion.commit()
        
                    print('Grabado correctamente....')
                except Exception as e:
                    print('Error al grabar en BD:', str(e))

                
            except Exception as e:
                print('Error al buscar la nave en Linescape')
                
        conexion.close()  
    except Exception as e:
        print('Error al conectarse al BD:', str(e))

#pyinstaller --onefile --icon=exe.png piloto.py
#hora_actual = datetime.now()
#hora = hora_actual.hour
#minuto = hora_actual.minute.gi

def ejecutar():

    print ('inicia proceso')
    print (datetime.now())
    

    itinerario('serverbd','login','password','database') #Perú
    print ('termina proceso')
    print (datetime.now())

ejecutar()
schedule.every(24).hours.do(ejecutar)

while True:
    schedule.run_pending()
    time.sleep(1)

#scrape_Linescape(nave,viaje,"cma")
