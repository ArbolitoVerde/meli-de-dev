# Tranformación de datos usando Python
Este es un modulo de python que fue creado para generar un dataset en especifico en función de tres diferentes inputs.

# Pre requisitos
- Python 3.8 o mayor
- Docker desktop

# Ejecución Local
Existen dos métodos para ejecutar el código, la primera usando terminal y la segunda vía jupyter notebook. En ambos
casos es necesario habilitar un virtual environment, en función del archivo requirements.txt

Como ejemplo para el caso de ejecutarlo por terminal, ejecutar los siguientes comandos en el directorio del repo:

- python3 -m venv myenv
- source myenv/bin/activate
- pip install --no-cache-dir --upgrade -r ./requirements.txt
- python3 ./app/main.py

# Ejecución docker
Se disponibiliza un archivo Dockerfile para ejecutar el código en un contenedor, y para ejecutarlo basta con los siguientes
comandos en el mismo directorio del repo:

- docker build -t meli-dev .
- docker run meli-dev