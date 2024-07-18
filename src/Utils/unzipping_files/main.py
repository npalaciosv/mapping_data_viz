import zipfile
from pathlib import Path
import shutil

class extraer_archivos():

    def __init__(self,raw_data = '/home/npalaciosv/Catedra/Geoanalitycs/src/Data/Raw',silver_data='/home/npalaciosv/Catedra/Geoanalitycs/src/Data/Silver'):
        self.raw_data = raw_data
        self.silver_data = silver_data

    def descomprimir_archivos(self):
        # Creamos un objetio Path con la direccion de los archivos crudos
        data_files = Path(self.raw_path)

        # Recorremos cada uno de los archivos usando la funcion rglob('*')
        for file in data_files.rglob('*'):

            # Para evitar que los espacios y el guion bajo afecten algun comando, los eliminamos del nombre del archivo
            new_name = file.name.replace(" ","").replace("_","")

            # Ajustamos el path completo ahora con el nuevo nombre
            new_path = file.with_name(new_name)

            # Con esta funcion renombramos el archivo.
            file.rename(new_path)

            # Iniciamos el proceso de extraccion de cada archivo
            with zipfile.ZipFile(f'{self.raw_path+file.name}','r') as extraction:

                # La informacion sera extraida la carpeta Silver
                extraction.extractall(self.silver_path)

        # Creamos un objetio Path con la direccion de los archivos descomprimidos
        silver_files = Path(self.silver_path)

        # Recorremos cada uno de los archivos para verificar su extension. Usamos la funcion iterdir()
        for file in silver_files.iterdir():

            # Verificamos si es un archivo (ya que puede ser una carpeta) y no termina en '.csv'
            if file.is_file() and file.suffix != '.csv':

                # Eliminamos el archivo y lo notificamos
                file.unlink()

        # Recorremos cada uno de los archivos para verificar que sea una carpeta
        for file in silver_files.iterdir():

            # Si archivo es realmente un archivo, sin importar el formato, no pasará nada, pero si es una carpeta movera el archivo fuera de esta
            if file.is_file():
                pass
            else:
                # Nos movemos un nivel mas en el path, es decir, entramos en la carpeta
                subfolder = silver_files.joinpath(file.name)

                #Recorremos la carpeta
                for subfile in subfolder.glob('*'):

                    # Buscamos los archivos .csv y los movemos a un nivel anterior en el path, es decir, los ponemos al nivel de los anteriores
                    if subfile.suffix == '.csv':
                        subfile.replace(self.silver_path+"/"+subfile.name)
                        print(f'Archivo: {subfile.name} movido exitosamente')

                # Eliminamos la carpeta una vez el archivo se ha movido al nivel anterior
                shutil.rmtree(subfolder)

if __name__ == "__main__":
    extractor = extraer_archivos()
    extractor.descomprimir_archivos()