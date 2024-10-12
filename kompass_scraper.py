import requests
from bs4 import BeautifulSoup
import csv
import time
import random
from fake_useragent import UserAgent
import concurrent.futures

def obtener_session():
    session = requests.Session()
    ua = UserAgent()
    session.headers.update({
        'User-Agent': ua.random,
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'es-ES,es;q=0.8,en-US;q=0.5,en;q=0.3',
        'Referer': 'https://es.kompass.com/',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Cache-Control': 'max-age=0',
    })
    return session

def extraer_datos_pagina(url, session):
    print(f"Accediendo a la URL: {url}")
    respuesta = session.get(url)
    if respuesta.status_code != 200:
        print(f"Error al acceder a la página. Código de estado: {respuesta.status_code}")
        return []

    sopa = BeautifulSoup(respuesta.content, 'html.parser')
    contenedores_empresa = sopa.find_all('div', class_='prod_list')
    resultados = []

    for contenedor in contenedores_empresa:
        nombre = contenedor.find('h2').text.strip() if contenedor.find('h2') else ''
        ubicacion = contenedor.find('span', class_='placeText')
        ubicacion = ubicacion.text.strip() if ubicacion else ''
        resumen = contenedor.find('p', class_='product-summary')
        resumen = resumen.find('span', class_='text').text.strip() if resumen else ''
        productos = contenedor.find('ul')
        productos = [li.text.strip() for li in productos.find_all('li')] if productos else []
        telefono = contenedor.find('input', id=lambda x: x and x.startswith('freePhone--'))
        telefono = telefono['value'] if telefono else ''
        sitio_web = contenedor.find('div', class_='companyWeb')
        sitio_web = sitio_web.find('a')['href'] if sitio_web and sitio_web.find('a') else ''
        
        resultados.append({
            'Nombre': nombre,
            'Ubicación': ubicacion,
            'Resumen': resumen,
            'Productos': ', '.join(productos),
            'Teléfono': telefono,
            'Sitio Web': sitio_web
        })
    
    return resultados

def extraer_datos_kompass(sector, id_sector, num_paginas):
    url_base = f"https://es.kompass.com/s/{sector}/{id_sector}/page-"
    session = obtener_session()
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = []
        for pagina in range(1, num_paginas + 1):
            url = f"{url_base}{pagina}/"
            futures.append(executor.submit(extraer_datos_pagina, url, session))
            time.sleep(random.uniform(2, 5))  # Pausa entre solicitudes
        
        resultados = []
        for future in concurrent.futures.as_completed(futures):
            resultados.extend(future.result())
    
    return resultados

def guardar_en_csv(datos, nombre_archivo):
    with open(nombre_archivo, 'w', newline='', encoding='utf-8') as archivo_csv:
        campos = ['Nombre', 'Ubicación', 'Resumen', 'Productos', 'Teléfono', 'Sitio Web']
        escritor = csv.DictWriter(archivo_csv, fieldnames=campos)
        escritor.writeheader()
        for fila in datos:
            escritor.writerow(fila)

# Uso del script
sector = "construccion"
id_sector = "09"
num_paginas = 2

resultados = extraer_datos_kompass(sector, id_sector, num_paginas)
guardar_en_csv(resultados, 'empresas_construccion_kompass.csv')
print(f"Se han extraído {len(resultados)} empresas y guardado en 'empresas_construccion_kompass.csv'")