from pathlib import Path
import pandas as pd
import folium
import sqlite3
import matplotlib.colors as mcolors
import streamlit as st
from streamlit_folium import st_folium
import matplotlib.pyplot as plt


class crear_mapa():

    def __init__(
        self,
        raw_path="/home/npalaciosv/Catedra/Geoanalitycs/src/Data/Raw/",
        silver_path="/home/npalaciosv/Catedra/Geoanalitycs/src/Data/Silver/",
    ):
        self.raw_path = raw_path
        self.silver_path = silver_path

    def pintar_mapa(self):
        # -------------------------------------------------------------------------------------------
        # Creamos un diccionario que servira para homologar los nombres correspondientes
        columnas_ajustadas = {
            "Fuente": "Mayorista",
            "FechaEncuesta": "Fecha",
            "Fecha": "Fecha",
            "Cod. Depto Proc.": "Codigo_departamento",
            "Cod. Municipio Proc.": "Codigo_municipio",
            "Departamento Proc.": "Departamento",
            "Municipio Proc.": "Municipio",
            "Grupo": "Grupo",
            "Ali": "Alimento",
            "Cant Kg": "Cant_Kg",
            "Código Departamento": "Codigo_departamento",
            " Código Municipio ": "Codigo_municipio",
            "Alimento": "Alimento",
            "Cuidad, Mercado Mayorista": "Mayorista",
            "Origen": "Origen",
            "Unnamed: 9": "Unnamed: 9",
            "Unnamed: 10": "Unnamed: 10",
            "Codigo CPC": "Codigo CPC",
        }
        # -------------------------------------------------------------------------------------------
        # Creamos un objetio Path con la direccion de los archivos descomprimidos
        silver_files = Path(self.silver_path)

        # -------------------------------------------------------------------------------------------
        # Creamos un dataframe vacio que nos permitira consolidar todos los archivos
        df = pd.DataFrame()

        # Recorremos archivo por archivo para irlos consolidando
        for file in silver_files.iterdir():

            # Leemos cada archivo. Usamos 'unicode_escape' ya que es el formato que nos permite leer los archivos sin ningun error
            df_temp = pd.read_csv(
                file, encoding="unicode_escape", sep=";", low_memory=False
            )

            # Creamos una columna que nos permitira rastear de que archivo esta saliendo la informacion
            df_temp["Origen"] = file.name

            # Cambiamos el nombre de las columnas de acuerdo con el diccionario que definimos previamente
            for column in df_temp.columns:
                df_temp.rename(
                    columns={column: columnas_ajustadas[column]}, inplace=True
                )

            # Concatenamos los dataframes
            df = pd.concat([df, df_temp], ignore_index=True)

        # -------------------------------------------------------------------------------------------
        # Eliminamos las columnas inncesarias
        df = df.drop(["Unnamed: 9", "Unnamed: 10", "Codigo CPC"], axis=1)

        # -------------------------------------------------------------------------------------------
        # 1
        df.loc[df["Mayorista"] == "Cali, Santa Elena", "Mayorista"] = (
            "Cali, Santa Helena"
        )

        # 2
        df["Codigo_municipio"] = df["Codigo_municipio"].str.replace("'", "")

        # 3
        df["Codigo_municipio"] = df["Codigo_municipio"].str.strip()

        # 4
        df["Codigo_departamento"] = df["Codigo_departamento"].str.replace(
            "'", "")

        # 5
        df["Codigo_departamento"] = df["Codigo_departamento"].str.strip()

        # 6. error = coerce hace que la funcion no falle cuando se encuentra con valores con nan
        df["Cant_Kg"] = pd.to_numeric(df["Cant_Kg"], errors="coerce")

        # -------------------------------------------------------------------------------------------
        # Definimos la ruta en la que vamos a almacenar la informacion.
        user_path = "/mnt/d/Classes/Data visualization/Mapping data/Database/geo.db"

        # Creamos un objeto sqlite3 que nos permitira conectarnos con la base de datos
        conn = sqlite3.connect(user_path)

        # Enviamos la informacion del dataframe al base de datos
        df.to_sql("Fact", con=conn, if_exists="replace", index=False)

        print("Check 1: La base de datos ha sido creada")

        # -------------------------------------------------------------------------------------------
        # Creamos la ruta donde se va a consultar la informacion de las coordenadas de los mayoristas
        coordenadas_path = (
            "/home/npalaciosv/Catedra/Geoanalitycs/src/Data/Static/Coordenadas.csv"
        )

        # Leemos la informacion dentro de un dataframe.
        df_coor = pd.read_csv(coordenadas_path, sep=";", encoding="utf-8")

        # Enviamos la informacion a la base de datos creada previamente
        df_coor.to_sql(
            "Coordenadas_mayoristas", con=conn, if_exists="replace", index=False
        )

        # -------------------------------------------------------------------------------------------
        # Iniciamos un objeto map de folium, centrando la vista inicial del mapa en Bogota
        map = folium.Map(
            location=[4.922860659232988, -74.02580517889908], zoom_start=7)

        # Definimos una ruta en la que va a almacenarce el mapa.
        maps_path = "/home/npalaciosv/Catedra/Geoanalitycs/src/Graphs/"

        # -------------------------------------------------------------------------------------------
        # Creamos la sentencia sql
        sentencia_sql = f"""
                        SELECT
                            f.Mayorista,
                            cm.Latitud,
                            cm.Longitud,
                            SUM(f.Cant_Kg) AS Produccion
                        FROM Fact f
                        LEFT JOIN Coordenadas_mayoristas cm ON cm.Mayorista = f.Mayorista
                        WHERE f.Mayorista IS NOT NULL
                        GROUP BY f.Mayorista,cm.Latitud,cm.Longitud
                        ORDER BY Produccion DESC
                        ;
                        """

        # Ejcutamos la sentencia sql y almacenamos el resultado en un dataframe
        df_may = pd.read_sql_query(sentencia_sql, conn)

        # -------------------------------------------------------------------------------------------
        # Convertimos la informacion consultada en listas sobre las que vamos a iterar para agregar elementos al mapa
        lat = df_may["Latitud"].to_list()
        lon = df_may["Longitud"].to_list()
        may = df_may["Mayorista"].to_list()
        prod = df_may["Produccion"].to_list()

        # Combinamos las listas con la funcion zip para que podamos iterar con toda la informacion simultaneamente
        coordenadas = list(zip(lat, lon, may, prod))

        # -------------------------------------------------------------------------------------------
        # Definimos el icono con el que vamos a representar a los mayoristas dentro del mapa
        icon_path = "/home/npalaciosv/Catedra/Geoanalitycs/src/Icons/Mayorista.png"

        # Vamos iterar sobre cada mayorista para localizarlo en el mapa
        for lat, lon, may, prod in coordenadas:
            # Iniciamos el elemento marker desde Folium
            folium.Marker(
                [lat, lon],
                popup=f"{may}",
                icon=folium.CustomIcon(
                    icon_image=icon_path, icon_size=(25, 25)),
            ).add_to(map)

        print("Check 2: Primer layer del mapa se ha construido")

        # -------------------------------------------------------------------------------------------
        # Definirmos los tipos de datos específicos para las columnas
        column_types = {
            "Código Departamento": str,
            "Código Municipio": str,
            "Código Centro Poblado": str,
        }

        # Creamos un diccionario que nos permitira homologar los nombres de la base de datos
        column_rename_map = {
            "Código Departamento": "Codigo_Departamento",
            "Código Municipio": "Codigo_Municipio",
            "Código Centro Poblado": "Codigo_Centro_Poblado",
        }

        # Definimos el path de donde se va a traer la informacion del clasificador geografico
        geopath = "/home/npalaciosv/Catedra/Geoanalitycs/src/Data/Static/Clasificador_geografico.csv"

        # Creamos un dataframe con la informacion contenida en el clasificador geografico
        df_geoclas = pd.read_csv(
            geopath, sep=";", encoding="utf-8", dtype=column_types)

        # Renombramos las columnas correspondientes
        df_geoclas = df_geoclas.rename(columns=column_rename_map)

        # -------------------------------------------------------------------------------------------
        # Eliminamos las columnas innecesarias
        df_geoclas = df_geoclas[
            ["Codigo_Departamento", "Codigo_Municipio", "Longitud", "Latitud"]
        ]

        # Dejamos solo una coordenada para cada municipio
        df_geoclas = df_geoclas.drop_duplicates(subset=["Codigo_Municipio"])

        # Ajustamos los valors de latitud y longitud para que puedan ser interpretados como numero
        df_geoclas["Latitud"] = df_geoclas["Latitud"].str.replace(",", ".")
        df_geoclas["Longitud"] = df_geoclas["Longitud"].str.replace(",", ".")

        # Enviamos la informacion a una tabla dentro de la base de datos
        df_geoclas.to_sql(
            "Clasificador_geografico", con=conn, if_exists="replace", index=False
        )

        # -------------------------------------------------------------------------------------------
        # Traemos la informacion de municipio, longitud y latitud de la tabla Fact
        sentencia_sql = f"""
                        SELECT
                            DISTINCT f.Codigo_Municipio,
                            f.Municipio,
                            cg.Latitud,
                            cg.Longitud
                        FROM Fact f
                        LEFT JOIN Clasificador_geografico cg ON cg.Codigo_Municipio = f.Codigo_Municipio
                            ;
                        """
        # Enviamos la informacion a un dataframe
        df_int = pd.read_sql_query(sentencia_sql, conn)

        # -------------------------------------------------------------------------------------------
        # Definimos el path donde esta el archivo con las coordenadas de los puntos internacionales
        coordenadas_int_path = "/home/npalaciosv/Catedra/Geoanalitycs/src/Data/Static/Coordenadas_internacionales.csv"

        # Enviamos la informacion a un dataframe
        df_coor_int = pd.read_csv(
            coordenadas_int_path, sep=";", encoding="utf-8")

        # Ajustamos los valors de latitud y longitud para que puedan ser interpretados como numero
        df_coor_int["Latitud"] = df_coor_int["Latitud"].astype(float)
        df_coor_int["Longitud"] = df_coor_int["Longitud"].astype(float)

        # -------------------------------------------------------------------------------------------
        # Creamos una lista con todos los municipios "internacionales"
        list_coor_int = df_coor_int["Municipio"].to_list()

        # -------------------------------------------------------------------------------------------
        # Esta funcion hace que cuando haya un municipio "internacional", el dataframe le asigne las coordenadas correspondientes
        for mun_int in list_coor_int:
            df_int.loc[df_int["Municipio"] == mun_int, "Latitud"] = df_coor_int.loc[
                df_coor_int["Municipio"] == mun_int, "Latitud"
            ].values
            df_int.loc[df_int["Municipio"] == mun_int, "Longitud"] = df_coor_int.loc[
                df_coor_int["Municipio"] == mun_int, "Longitud"
            ].values

        # Deja valores unicos para cada municipio internacional
        df_int = df_int.dropna()

        # Enviamos la informacion como una tabla a la base de datos
        df_int.to_sql(
            "Coordenadas_internacionales", con=conn, if_exists="replace", index=False
        )

        # -------------------------------------------------------------------------------------------
        # Definimos el path donde esta el logo que vamos a usar para identificar los municipios productores
        icon_path = "/home/npalaciosv/Catedra/Geoanalitycs/src/Icons/Productor.png"

        # Convertimos la informacion consultada en listas sobre las que vamos a iterar para agregar elementos al mapa
        lat = df_int["Latitud"].to_list()
        lon = df_int["Longitud"].to_list()
        municipio = df_int["Municipio"].to_list()

        # Combinamos las listas con la funcion zip para que podamos iterar con toda la informacion simultaneamente
        coordenadas = list(zip(lat, lon, municipio))

        # Vamos iterar sobre cada municipio para localizarlo en el mapa
        for lat, lon, municipio in coordenadas:
            folium.Marker(
                [lat, lon],
                popup=f"{municipio}",
                icon=folium.CustomIcon(
                    icon_image=icon_path, icon_size=(15, 15)),
            ).add_to(map)

        print("Check 3: Segundo layer del mapa se ha construido")

        # -------------------------------------------------------------------------------------------
        # Definimos el path que nos lleva al archivo geojson
        geopath = (
            "/home/npalaciosv/Catedra/Geoanalitycs/src/Data/Static/colombia.geo.json"
        )

        # Agregamos el layer con la division departamental al mapa
        folium.GeoJson(
            geopath,
            style_function=lambda feature: {
                "color": "black",
                "weight": 2,
                "dashArray": "5, 5",
                "fillOpacity": 0.5,
            },
        ).add_to(map)
        print("Check 4: Tercer layer del mapa se ha construido")

        # -------------------------------------------------------------------------------------------
        # Creamos una consulta sql que nos va traer la informacion de productividad por departamento
        sentencia_sql = f"""
                        SELECT
                            f.Codigo_departamento AS DPTO,
                            SUM(f.Cant_Kg) AS Produccion
                        FROM Fact f
                        WHERE f.Codigo_departamento != 'n.a.'
                        GROUP BY f.Codigo_departamento
                        ORDER BY Produccion DESC
                        ;
                        """

        # Guardamos la informacion en un dataframe
        df_temp = pd.read_sql_query(sentencia_sql, conn)

        # -------------------------------------------------------------------------------------------
        # Convertimos el DataFrame a un diccionario
        temp_dict = df_temp.to_dict(orient="records")

        # -------------------------------------------------------------------------------------------
        # Función para asignar color basado en la producción
        def asignar_color(feature):

            # Calcular el mínimo y máximo de las producciones
            producciones = [item["Produccion"] for item in temp_dict]
            produccion_minima = min(producciones)
            produccion_maxima = (
                max(producciones) * 0.4
            )  # Este valor esta castigado para mejorar un poco la colorimetria del mapa

            # Definir colores en la escala
            color_minimo = "red"
            color_medio = "orange"
            color_maximo = "green"

            # Extramos el codigo del departamento que esta contenido en el archivo geojson
            codigo_departamento = feature["properties"].get("DPTO")

            # Obtenemos la producción de la consulta que guardamos en df_temp o 0 si no está definido
            produccion = next(
                (
                    item["Produccion"]
                    for item in temp_dict
                    if item["DPTO"] == codigo_departamento
                ),
                0,
            )

            # Normalizamos la producción entre 0 y 1
            norm = mcolors.Normalize(
                vmin=produccion_minima, vmax=produccion_maxima)

            # Crear una interpolación lineal de colores
            color_interp = mcolors.LinearSegmentedColormap.from_list(
                "custom_map", [color_minimo, color_medio, color_maximo]
            )

            # Obtener el color según la producción normalizada
            color = mcolors.rgb2hex(color_interp(norm(produccion)))

            return color

        # -------------------------------------------------------------------------------------------
        # Agregamos la division de los mapas coloreados segun su productividad
        folium.GeoJson(
            geopath,
            style_function=lambda feature: {
                "fillColor": asignar_color(feature),
                "color": "black",
                "weight": 2,
                "dashArray": "5, 5",
                "fillOpacity": 0.5,
            },
        ).add_to(map)

        # Guardamos el mapa
        map.save(maps_path + "mapa.html")
        print("El mapa ha sido creado correctamente")

        # Cerramos la conexion
        conn.close()

        return map, df


if __name__ == "__main__":
    mapa = crear_mapa()
    mapa.pintar_mapa()
