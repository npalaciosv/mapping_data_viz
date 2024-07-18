#%% Importamos las librerias
from Utils.unzipping_files import extraer_archivos
from Utils.plotting_map import crear_mapa
import pandas as pd
import streamlit as st

#%% Descomprimimos los archivos
extractor = extraer_archivos()
extractor.descomprimir_archivos()

#%% Creamo el mapa y traemos la informacion en un dataframe
creador = crear_mapa()
map,df = creador.pintar_mapa()

#%%
print(df)

#%% Configurar panel lateral
st.sidebar.header('Filtros')
fecha = st.sidebar.date_input('Fecha', [])
municipio = st.sidebar.multiselect('Municipio', df['Municipio'].unique())
mayorista = st.sidebar.multiselect('Mayorista', df['Mayorista'].unique())
alimento = st.sidebar.multiselect('Alimento', df['Alimento'].unique())
grupo = st.sidebar.multiselect('Grupo', df['Grupo'].unique())

Holi=1