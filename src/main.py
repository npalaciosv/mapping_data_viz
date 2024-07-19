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

#%% Creamo el mapa y traemos la informacion en un dataframe
creador = crear_mapa()
map,df = creador.pintar_mapa()