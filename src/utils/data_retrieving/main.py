import sqlite3
import pandas as pd
from tqdm import tqdm

class consultar_info():
    def __init__(self,address):
        self.db_path = address['db_path']

    def crear_vista(self):
        print('---------Proceso de creacion de la vista general iniciado---------')
        try:
            sentencia_sql = f"""
                    CREATE VIEW IF NOT EXISTS consulta_consolidada AS
                    WITH municipio_desduplicado AS (
                    SELECT
                        cg.Codigo_Municipio,
                        cg.Latitud,
                        cg.Longitud,
                        ROW_NUMBER() OVER (PARTITION BY Codigo_Municipio ORDER BY (SELECT NULL)) AS rn
                    FROM Clasificador_geografico cg
                    )
                    SELECT
                        f.*,
                        cm.Latitud AS Latitud_may,
                        cm.Longitud AS Longitud_may,
                        CAST(COALESCE(md.Latitud,ci.Latitud) AS DECIMAL) AS Latitud_mun,
                        CAST(COALESCE(md.Longitud,ci.Longitud) AS DECIMAL) AS Longitud_mun
                    FROM Fact f
                    LEFT JOIN Coordenadas_mayoristas cm ON cm.Mayorista = f.Mayorista
                    LEFT JOIN municipio_desduplicado md ON md.Codigo_Municipio = f.Codigo_municipio
                    LEFT JOIN Coordenadas_internacionales ci ON ci.Municipio = f.Municipio
                    WHERE f.Mayorista IS NOT NULL;
                    """
            conn = sqlite3.connect(self.db_path)

            # Crear un cursor para ejecutar la consulta
            cursor = conn.cursor()

            # Ejecutar la sentencia SQL para crear la vista
            cursor.execute(sentencia_sql)

            # Confirmar los cambios
            conn.commit()

            print("La vista ha sido creada exitosamente")

        except sqlite3.DatabaseError as e:
            # Manejo específico para errores de base de datos
            print(f"Error en la base de datos: {e}")
        except Exception as e:
            # Manejo general de errores
            print(f"Error al consultar datos: {e}")
        print('---------Proceso de creacion de la vista general finalizado---------')

    def obtener_datos(self):
        print('---------Proceso de obtencion de datos iniciado---------')
        conn = sqlite3.connect(self.db_path)

        # Ejecutar la consulta y obtener los datos
        query = "SELECT DISTINCT cc.* FROM consulta_consolidada cc;"

        # Obtener la cantidad de registros
        count_query = "SELECT COUNT(*) FROM (SELECT DISTINCT cc.* FROM consulta_consolidada cc);"
        total_rows = pd.read_sql_query(count_query, conn).iloc[0, 0]

        # Leer los datos usando tqdm para mostrar progreso
        chunksize = 1000  # Tamaño del chunk para la lectura
        data_iter = pd.read_sql_query(query, conn, chunksize=chunksize)

        # Inicializar un DataFrame vacío
        data = pd.DataFrame()

        # Usar tqdm para mostrar la barra de progreso
        for chunk in tqdm(data_iter, total=total_rows//chunksize, unit='chunk',desc="Obteniendo informacion"):
            data = pd.concat([data, chunk])

        # Cerrar la conexión a la base de datos
        conn.close()

        data['Fecha'] = pd.to_datetime(data['Fecha'], format='%d/%m/%Y')

        return data

if __name__ == "__main__":
    address = {'db_path': '/mnt/d/Classes/Data visualization/Mapping data/Database/geo.db'}
    consulta = consultar_info(address)
    consulta.crear_vista()
    data = consulta.obtener_datos()
