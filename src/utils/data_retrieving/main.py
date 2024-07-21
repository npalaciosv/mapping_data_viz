import sqlite3

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