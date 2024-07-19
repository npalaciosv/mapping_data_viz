#%% Importamos las librerias
from utils.unzipping_files import extraer_archivos
from utils.plotting_map import crear_mapa
import pandas as pd
import streamlit as st
from datetime import datetime
import matplotlib.pyplot as plt
import numpy as np
import streamlit_folium as st_folium

#%% Descomprimimos los archivos
extractor = extraer_archivos()
extractor.descomprimir_archivos()

ic = ingenieria_caracteristicas()
ic.enviar_bases()

data = sql.traer_datos()

# Configurar panel lateral
st.sidebar.header('Filtros')

# Barra desplazable para la selección de fechas
fecha = st.sidebar.slider('Fecha', min_date, max_date, (min_date, max_date))
municipio = st.sidebar.multiselect('Municipio', data['Municipio'].unique())
mayorista = st.sidebar.multiselect('Mayorista', data['Mayorista'].unique())
alimento = st.sidebar.multiselect('Alimento', data['Alimento'].unique())
grupo = st.sidebar.multiselect('Grupo', data['Grupo'].unique())

# Aplicar filtros
df_fact = 1 #sql respectivo
df_mun = 1 #sql respectivo
df_may = 1 #sql respectivo
df_dep = 1 #sql respectivo

if municipio:
    df = df[df['Municipio'].isin(municipio)]
if mayorista:
    df = df[df['Mayorista'].isin(mayorista)]
if alimento:
    df = df[df['Alimento'].isin(alimento)]
if grupo:
    df = df[df['Grupo'].isin(grupo)]

map = crear_map(df_mun,df_may,df_dep)

# Mostrar el mapa en Streamlit
st_data = st_folium(map, width=700, height=500)