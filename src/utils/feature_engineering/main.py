import pandas as pd
from pathlib import Path
import sqlite3
from tqdm import tqdm

class feature_engineering():

        def __init__(self,address):
                self.raw_path = address['raw_path']
                self.silver_path = address['silver_path']
                self.coordenadas_may_path = address['coor_may']
                self.geo_path = address['clas_geo_path']
                self.coordenadas_muni_int = address['coor_int']
                self.db_path = address['db_path']

        def borrar_todas_las_tablas(self):
                # Conectar a la base de datos
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()

                try:
                        print('---------Proceso de eliminacion de carpetas iniciado---------')
                        # Obtener todos los nombres de las tablas
                        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
                        tablas = cursor.fetchall()

                        # Borrar todas las tablas
                        for tabla in tqdm(tablas,desc=f"Eliminando tablas"):
                                cursor.execute(f"DROP TABLE IF EXISTS {tabla[0]};")
                                print(f"Tabla {tabla[0]} eliminada")

                        # Confirmar cambios
                        conn.commit()
                        print('---------Proceso de eliminacion de carpetas finalizado---------')
                except Exception as e:
                        print(f"Error al eliminar tablas: {e}")
                finally:
                        # Cerrar la conexión
                        cursor.close()
                        conn.close()

        def enviar_bases(self):

                print('---------Proceso de ingenieria de datos iniciado---------')

#-------------------------------------------------------------------------------------------
                # Creamos un diccionario que servira para homologar los nombres correspondientes
                columnas_ajustadas = {
                        'Fuente':'Mayorista',
                        'FechaEncuesta':'Fecha',
                        'Fecha':'Fecha',
                        'Cod. Depto Proc.':'Codigo_departamento',
                        'Cod. Municipio Proc.':'Codigo_municipio',
                        'Departamento Proc.':'Departamento',
                        'Municipio Proc.':'Municipio',
                        'Grupo':'Grupo',
                        'Ali':'Alimento',
                        'Cant Kg':'Cant_Kg',
                        'Código Departamento':'Codigo_departamento',
                        ' Código Municipio ':'Codigo_municipio',
                        'Alimento':'Alimento',
                        'Cuidad, Mercado Mayorista':'Mayorista',
                        'Origen':'Origen',
                        'Unnamed: 9':'Unnamed: 9',
                        'Unnamed: 10':'Unnamed: 10',
                        'Codigo CPC':'Codigo CPC'
                }
#-------------------------------------------------------------------------------------------
                # Creamos un objetio Path con la direccion de los archivos descomprimidos
                silver_files = Path(self.silver_path)

#-------------------------------------------------------------------------------------------
                # Creamos un dataframe vacio que nos permitira consolidar todos los archivos
                df = pd.DataFrame()

                # Recorremos archivo por archivo para irlos consolidando
                for file in tqdm(silver_files.iterdir(), desc=f"Consolidadondo archivos"):

                        # Leemos cada archivo. Usamos 'unicode_escape' ya que es el formato que nos permite leer los archivos sin ningun error
                        df_temp = pd.read_csv(file, encoding='unicode_escape', sep=";", low_memory=False)

                        # Creamos una columna que nos permitira rastear de que archivo esta saliendo la informacion
                        df_temp['Origen'] = file.name

                        # Cambiamos el nombre de las columnas de acuerdo con el diccionario que definimos previamente
                        for column in df_temp.columns:
                                df_temp.rename(columns={column: columnas_ajustadas[column]}, inplace=True)

                        # Concatenamos los dataframes
                        df =pd.concat([df,df_temp], ignore_index=True)

#-------------------------------------------------------------------------------------------
                # Eliminamos las columnas inncesarias
                df = df.drop(['Unnamed: 9','Unnamed: 10','Codigo CPC'],axis=1)

#-------------------------------------------------------------------------------------------
                #1
                df.loc[df["Mayorista"] == "Cali, Santa Elena", "Mayorista"] = "Cali, Santa Helena"

                #2
                df['Codigo_municipio'] = df['Codigo_municipio'].str.replace("'","")

                #3
                df['Codigo_municipio'] = df['Codigo_municipio'].str.strip()

                #4
                df['Codigo_departamento'] = df['Codigo_departamento'].str.replace("'","")

                #5
                df['Codigo_departamento'] = df['Codigo_departamento'].str.strip()

                #6. error = coerce hace que la funcion no falle cuando se encuentra con valores con nan
                df['Cant_Kg'] = pd.to_numeric(df['Cant_Kg'], errors='coerce')

#-------------------------------------------------------------------------------------------
                # Creamos un objeto sqlite3 que nos permitira conectarnos con la base de datos
                conn = sqlite3.connect(self.db_path, timeout=30)

                # Enviamos la información del dataframe a la base de datos en partes
                chunk_size = 1000  # Tamaño del fragmento
                num_chunks = len(df) // chunk_size + 1

                for i in tqdm(range(num_chunks), desc="Creando tabla Fact"):
                        start = i * chunk_size
                        end = start + chunk_size
                        df_chunk = df.iloc[start:end]
                        df_chunk.to_sql('Fact', con=conn, if_exists='append', index=False)

#-------------------------------------------------------------------------------------------
                # Leemos la informacion dentro de un dataframe.
                df_coor_may = pd.read_csv(self.coordenadas_may_path,sep=";",encoding='utf-8')

                chunk_size = 10  # Tamaño del fragmento
                num_chunks = len(df_coor_may) // chunk_size + 1

                for i in tqdm(range(num_chunks), desc="Creando tabla Coordenadas_mayoristas"):
                        start = i * chunk_size
                        end = start + chunk_size
                        df_chunk = df_coor_may.iloc[start:end]
                        df_chunk.to_sql('Coordenadas_mayoristas', con=conn, if_exists='append', index=False)

#-------------------------------------------------------------------------------------------
                # Definirmos los tipos de datos específicos para las columnas
                column_types = {'Código Departamento': str, 'Código Municipio': str, 'Código Centro Poblado': str}

                # Creamos un diccionario que nos permitira homologar los nombres de la base de datos
                column_rename_map = {
                        'Código Departamento': 'Codigo_Departamento',
                        'Código Municipio': 'Codigo_Municipio',
                        'Código Centro Poblado': 'Codigo_Centro_Poblado'
                }

                # Creamos un dataframe con la informacion contenida en el clasificador geografico
                df_geoclas = pd.read_csv(self.geo_path,sep=";",encoding='utf-8', dtype=column_types)

                # Renombramos las columnas correspondientes
                df_geoclas = df_geoclas.rename(columns=column_rename_map)

#-------------------------------------------------------------------------------------------
                # Eliminamos las columnas innecesarias
                df_geoclas = df_geoclas[['Codigo_Departamento','Codigo_Municipio','Longitud','Latitud']]

                # Dejamos solo una coordenada para cada municipio
                df_geoclas = df_geoclas.drop_duplicates(subset=['Codigo_Municipio'])

                # Ajustamos los valors de latitud y longitud para que puedan ser interpretados como numero
                df_geoclas['Latitud'] = df_geoclas['Latitud'].str.replace(",",".")
                df_geoclas['Longitud'] = df_geoclas['Longitud'].str.replace(",",".")

                chunk_size = 100  # Tamaño del fragmento
                num_chunks = len(df_geoclas) // chunk_size + 1

                for i in tqdm(range(num_chunks), desc="Creando tabla Clasificador_geografico"):
                        start = i * chunk_size
                        end = start + chunk_size
                        df_chunk = df_geoclas.iloc[start:end]
                        df_chunk.to_sql('Clasificador_geografico', con=conn, if_exists='append', index=False)

#-------------------------------------------------------------------------------------------
                # Enviamos la informacion a un dataframe
                df_coor_int = pd.read_csv(self.coordenadas_muni_int,sep=";",encoding='utf-8')

                # Ajustamos los valors de latitud y longitud para que puedan ser interpretados como numero
                df_coor_int['Latitud'] = df_coor_int['Latitud'].astype(float)
                df_coor_int['Longitud'] = df_coor_int['Longitud'].astype(float)

                chunk_size = 10  # Tamaño del fragmento
                num_chunks = len(df_coor_int) // chunk_size + 1

                for i in tqdm(range(num_chunks), desc="Creando tabla Clasificador_geografico"):
                        start = i * chunk_size
                        end = start + chunk_size
                        df_chunk = df_coor_int.iloc[start:end]
                        df_chunk.to_sql('Coordenadas_internacionales', con=conn, if_exists='append', index=False)

                print('---------Proceso de ingenieria de datos iniciado---------')

if __name__ == "__main__":
        ic = feature_engineering()
        ic.borrar_todas_las_tablas()
        ic.enviar_bases()