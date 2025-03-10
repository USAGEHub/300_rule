import pandas as pd
from shapely.wkt import loads,dumps
import requests
from shapely.geometry import Point, LineString, Polygon
import warnings
import json
import sys
import os
import fiona
import geopandas as gpd
from datetime import datetime
from tqdm import tqdm
import time
from tenacity import retry, stop_after_attempt, wait_fixed
import logging
import urllib3

#Variabili globali
BUFFER = 300  # Distanza del buffer in metri
DEFAULT_CRS = "EPSG:4326"  # CRS per il routing
OSRM_API_URL = "https://routing.openstreetmap.de/routed-car/route/v1/driving/"


# Disabilita il warning per le richieste HTTPS non verificate
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def proiezione_stesso_sr(gdf_civici, gdf_gate):

    try:   
        # Riproiezione di gdf_gate nello stesso sistema di gdf_civici
        if gdf_civici.crs is None:
            print("input_origini non ha un CRS definito.\n")
        elif gdf_gate.crs != gdf_civici.crs:
            print(f"Riproiezione di gdf_gate da {gdf_gate.crs} a {gdf_civici.crs}.\n")
            gdf_gate = gdf_gate.to_crs(gdf_civici.crs)
        else:
            print("I due GeoDataFrame hanno già lo stesso CRS, nessuna riproiezione necessaria.\n")
        return gdf_civici, gdf_gate
    except Exception as e:
        print(f"Errore durante la riproiezione dei GeoDataFrame: {e}.\n", flush=True)

def apply_buffer(gdf_civici, buffer_distance):
    try:
        # Creiamo un nuovo GeoDataFrame con le geometrie bufferizzate
        gdf_buffer = gdf_civici.copy()
        gdf_buffer['geometry'] = gdf_civici.geometry.buffer(buffer_distance)
        print(f"Applicato buffer di {buffer_distance} metri ai civici.\n", flush=True)
        return gdf_buffer
    except Exception as e:
        print(f"Errore durante l'applicazione del buffer: {e}.\n", flush=True)


# Funzione per garantire che il CRS sia EPSG:4326
def check_and_convert_crs(gdf, default_crs):
    if gdf.crs is None:
        print("CRS non definito. Impostazione del CRS a EPSG:4326.")
        gdf.set_crs(default_crs, allow_override=True, inplace=True)
    elif gdf.crs != default_crs:
        print(f"CRS diverso da {default_crs}. Riproiezione da {gdf.crs} a {default_crs}.")
        gdf = gdf.to_crs(default_crs)
    else:
        print(f"Il CRS è già {default_crs}.")
    return gdf


# Configurazione del logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

# Funzione di retry con tenacity: controlla timeout
@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def get_api_response(api_url):
    response = requests.get(api_url, timeout=30, verify=False)
    response.raise_for_status()  # Lancia un'eccezione per risposte non 2xx
    return response.json()
#

def calcola_distanze_gate_civ(gdf_gate_civici, osrm_api_url, subset_size=1000):
    try:
        gdf_gate_civici['distance'] = None
        gdf_gate_civici['is_300'] = False
        
        cur_id_origine = None
        logging.info("Inizio del calcolo delle distanze...")
        
        total_rows = len(gdf_gate_civici)
        for start in tqdm(range(0, total_rows, subset_size), total=(total_rows // subset_size), desc="Elaborando subsets"):
            end = min(start + subset_size, total_rows)
            gdf_subset = gdf_gate_civici.iloc[start:end]
            logging.info(f"Elaborando subset da riga {start} a {end}...")

            for index, row in tqdm(gdf_subset.iterrows(), total=len(gdf_subset), desc="Processando coppie gate/civico"):
                id_origine = row['id_civico']
                if cur_id_origine == id_origine and is_300:
                    continue

                id_destinazione = row['id_gate']
                origine = row['geometry_civici']
                destinazione = row['geometry_gate']
                origine_lon, origine_lat = origine.x, origine.y
                logging.info(f"Elaborando coppia: Civico ID {id_origine} - Gate ID {id_destinazione}")

                destinazione_lon, destinazione_lat = destinazione.x, destinazione.y

                api_url = f'{osrm_api_url}{origine_lon},{origine_lat};{destinazione_lon},{destinazione_lat}?overview=full&geometries=geojson'
                
                try:
                    geojson_result = get_api_response(api_url)
                    logging.info('Geojson result ricevuto')

                    if 'routes' in geojson_result and geojson_result['routes']:
                        geometria_coord = geojson_result['routes'][0]['geometry']['coordinates']
                    else:
                        logging.warning("Nessuna route trovata nella risposta API.")
                        continue
                    
                    is_300 = False
                    if len(geometria_coord) > 1:
                        i = 1
                        for lon, lat in geometria_coord:
                            if i == 1:
                                punto_old = Point(lon, lat)
                                i += 1
                            else:
                                punto = Point(lon, lat)
                                geometria_linea = LineString([punto_old, punto])
                                gdf = gpd.GeoDataFrame(geometry=[geometria_linea], crs="EPSG:4326").to_crs(epsg=3857)
                                dist_or_dest_4326 = gdf.geometry.length.values[0]
                                
                                if dist_or_dest_4326 <= 300:
                                    is_300 = True
                                    gdf_gate_civici.at[index, 'is_300'] = is_300
                                    gdf_gate_civici.at[index, 'distance'] = dist_or_dest_4326
                                    cur_id_origine = id_origine
                                    break

                                punto_old = punto
                                i += 1
                except requests.exceptions.RequestException as e:
                    logging.error(f"Errore nella richiesta all'API: {e}")
                    continue
                except Exception as e:
                    logging.error(f"Errore durante il calcolo della distanza: {e}")
                    continue
        gdf_gate_civici.to_csv('gdf_gate_civici.csv', flush = True)
        return gdf_gate_civici

    except Exception as e:
        logging.error(f"Errore generale nella funzione calcola_distanze_gate_civ: {e}")


def creazione_output(gdf_civici, file_origini, layer_origini):
    
    gdf_civici.to_file(f"{file_origini}_300.gpkg", layer=f"{layer_origini}_300", driver="GPKG")
    return gdf_civici
    
try:
    # Input
    if len(sys.argv) != 5:
        print("Errore: Devi specificare 4 argomenti, il nome del geopckg dei civici e quello dei gate e i layer di riferimento.\n")
        sys.exit(1)
        
    file_origini = sys.argv[1]
    file_gate = sys.argv[2]
    layer_origini = sys.argv[3]
    layer_gate = sys.argv[4]
    inputPath = os.getcwd()
    
    print('Inizio script', datetime.now().strftime('%Y-%m-%d %H:%M:%S'), flush = True) 

    # Leggo il file dei civici 
    with fiona.open(f"{inputPath}/{file_origini}.gpkg", layer=layer_origini) as layer:
        crs_civici = layer.crs  # Prendo il CRS dal layer
        gdf_civici = gpd.GeoDataFrame.from_features(layer, crs=crs_civici)      

    # Leggo il file dei gate
    with fiona.open(f"{inputPath}/{file_gate}.gpkg", layer=layer_gate) as layer:
        crs_gate = layer.crs  # Prendo il CRS dal layer
        gdf_gate = gpd.GeoDataFrame.from_features(layer, crs=crs_gate)
        gdf_gate.reset_index(inplace=True)
        gdf_gate.rename(columns={'index': 'fid'}, inplace=True)   
    
    gdf_civici, gdf_gate = proiezione_stesso_sr(gdf_civici, gdf_gate)
    gdf_civici = gdf_civici[['CIVKEY', 'geometry']]
    gdf_gate = gdf_gate[['fid', 'TIPO_GATE', 'geometry']]
    if gdf_civici['CIVKEY'].duplicated().any():
        print("Attenzione: ci sono valori duplicati in 'CIVKEY' gdf_civici!")  # Log
    else:
        print("OK: 'CIVKEY' è univoco in gdf_civici.")  # Log
    
    if gdf_gate['fid'].duplicated().any():
        print("Attenzione: ci sono valori duplicati in 'fid' gdf_gate!")  # Log
    else:
        print("OK: 'fid' è univoco in gdf_gate.")
    gdf_civici_buffer = apply_buffer(gdf_civici, BUFFER)
    gdf_gate_civici = gpd.sjoin(gdf_gate, gdf_civici_buffer, predicate="within", how="inner")
    gdf_gate_civici = check_and_convert_crs(gdf_gate_civici, DEFAULT_CRS)
    gdf_civici_4326 = check_and_convert_crs(gdf_civici.copy(), DEFAULT_CRS)
    
    gdf_gate_civici = gdf_gate_civici.merge(
    gdf_civici_4326[['CIVKEY', 'geometry']].rename(columns={'geometry': 'geometry_civici'}),
    on='CIVKEY',  
    how='left'
    )
 
    # Ordina e filtra
    gdf_gate_civici = gdf_gate_civici.sort_values(by='fid')
    gdf_gate_civici = gdf_gate_civici[gdf_gate_civici['fid'].notnull()]

    # Rinomina le colonne del GeoDataFrame gdf_gate_civici
    gdf_gate_civici = gdf_gate_civici.rename(columns={
        'fid': 'id_gate',           # Rinomina 'fid_left' in 'id_gate'
        'CIVKEY': 'id_civico',      # Rinomina 'fid_right' in 'id_civico'
        'geometry': 'geometry_gate' # Rinomina 'geometry' in 'geometry_gate'
    })
    
    gdf_gate_civici = calcola_distanze_gate_civ(gdf_gate_civici, OSRM_API_URL)
    gdf_civici['is_300'] = False
    gdf_civici['distanza_m'] = None
    for index, row in gdf_civici.iterrows():
        id_origine = row['CIVKEY']
    
        # Righe in gdf_gate_civici che corrispondono a id_origine
        matching_rows = gdf_gate_civici[gdf_gate_civici['id_civico'] == id_origine]
        
        # Se ci sono righe corrispondenti
        if not matching_rows.empty:
           
            if matching_rows['is_300'].any():
                gdf_civici.at[index, 'is_300'] = True
            gdf_civici.at[index, 'distanza_m'] = matching_rows['distance'].iloc[0]      
        else:
           
            gdf_civici.at[index, 'is_300'] = False
            gdf_civici.at[index, 'distanza_m'] = None
    output = creazione_output(gdf_civici,file_origini,layer_origini)

    print('Script completato.\n', datetime.now().strftime('%Y-%m-%d %H:%M:%S'), flush = True)     
    ###---------------------------------------------------------------------------------
    
except Exception as error:
    print("ERRORE:",flush = True)
    print(error, flush = True)

