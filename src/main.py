#%% Importamos las librerias
from utils.unzipping_files import extraer_archivos
import pandas as pd
import streamlit as st
from datetime import datetime
import matplotlib.pyplot as plt
import numpy as np
import streamlit_folium as st_folium

from utils.feature_engineering import feature_engineering

address = {
'raw_path' : '/home/npalaciosv/Catedra/Geoanalitycs/src/Data/Raw/',
'silver_path' : '/home/npalaciosv/Catedra/Geoanalitycs/src/Data/Silver/',
'db_path' : '/mnt/d/Classes/Data visualization/Mapping data/Database/geo.db',
'clas_geo_path' : '/home/npalaciosv/Catedra/Geoanalitycs/src/Data/Static/Clasificador_geografico.csv',
'coor_int' : '/home/npalaciosv/Catedra/Geoanalitycs/src/Data/Static/Coordenadas_muni_int.csv',
'coor_may': '/home/npalaciosv/Catedra/Geoanalitycs/src/Data/Static/Coordenadas_mayoristas.csv',
'geojson_path' : '/home/npalaciosv/Catedra/Geoanalitycs/src/Data/Static/geojson/'
}

#%% Descomprimimos los archivos
extractor = extraer_archivos()
extractor.descomprimir_archivos()

ic = feature_engineering(address)

ic.borrar_todas_las_tablas()
ic.enviar_bases()

#conn = sqlite3.connect(user_path)
#data = pd.read_sql_query()

# Configurar panel lateral
st.sidebar.header('Filtros')

#Barra desplazable para la selección de fechas
#fecha = st.sidebar.slider('Fecha', min_date, max_date, (min_date, max_date))
#municipio = st.sidebar.multiselect('Municipio', data['Municipio'].unique())
#mayorista = st.sidebar.multiselect('Mayorista', data['Mayorista'].unique())
#alimento = st.sidebar.multiselect('Alimento', data['Alimento'].unique())
#grupo = st.sidebar.multiselect('Grupo', data['Grupo'].unique())

# Aplicar filtros
#df_fact = 1 #sql respectivo
#df_mun = 1 #sql respectivo
#df_may = 1 #sql respectivo
#df_dep = 1 #sql respectivo

#if municipio:
#    df = df[df['Municipio'].isin(municipio)]
#if mayorista:
#    df = df[df['Mayorista'].isin(mayorista)]
#if alimento:
#    df = df[df['Alimento'].isin(alimento)]
#if grupo:
#    df = df[df['Grupo'].isin(grupo)]

#map = crear_map(df_mun,df_may,df_dep)

# Mostrar el mapa en Streamlit
#st_data = st_folium(map, width=700, height=500)