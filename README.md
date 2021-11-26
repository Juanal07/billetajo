# billetajo

Práctica Big Data

Necesitas tener instalado python, pip y git.

## Download

`git clone https://github.com/Juanal07/billetajo.git`

`cd billetajo`

## venv

Necesitas la herramienta virtualenv `pip install virtualenv`

Crea un entorno virtual `virtualenv venv`

Actívalo en Linux/MacOS `source venv/bin/activate`

Actívalo en Windows`venv\scripts\activate`

## Dependencies

Instala dependencias `pip install -r requirements.txt`

Si quieres actualizar dependencias:
Necesitas la herramienta pipreqs `pip install pipreqs`

`pipreqs --force .`

Además necesitarás la clave (json) para acceder al bucket, contáctanos.

## Run

Ejecuta spark `python app.py`

Ejecuta front `streamlit run front.py`

Si desea probar la aplicación, está desplegada en este enlace https://bit.ly/bigdata-uem, tiene desabilitado el botón de lanzar spark por limitaciones del servidor.
