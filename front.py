import numpy as np
import streamlit as st
import pandas as pd
from google.cloud import storage
import datetime
from enum import Enum
import os
import json
from streamlit_folium import folium_static
import folium

storage_client = storage.Client.from_service_account_json('big-data-328215-74151e35e325.json')
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "big-data-328215-74151e35e325.json"

st.set_page_config(
   page_title="Almería 2015",
   page_icon="☀️",
   # layout="wide",
   # initial_sidebar_state="expanded",
)

st.title('Almeria 2015')

csv_names=['totalMovsPorHorasSector.csv',
'barriosAlimentacioSinTiendas.csv',
'barriosMayorSalud.csv',
'barriosMayorSector.csv',
'volumenComprasSector.csv',
]


st.write("7.Gráfica con las transacciones de media en cada franja horaria de los 10 sectores")
st.caption('Aquí podrá visualizar el volumen de transaciones por sector y por franja horaria')
df = pd.read_csv('gs://datosbd/{}'.format(csv_names[0]))
df.rename(columns = {'Unnamed: 0':'time'}, inplace = True)
df = df.set_index('time')
st.bar_chart(df)

st.write("8.Barrios donde se compre muchos alimentos pero no hay comercio de alimentación")
st.caption('En este mapa podrá visualizar los puntos donde puede ser más rentable abrir un supermercado')

# df = pd.read_csv('gs://datosbd/{}'.format(csv_names[1]))
# df.drop(df.columns[[0]], axis=1, inplace=True)
# df
# df = pd.DataFrame(
# np.random.randn(1000, 2) / [50, 50] + [37.16, -2.33],
# columns=['lat', 'lon'])
# df
# st.map(df)



df = pd.read_csv('gs://datosbd/{}'.format(csv_names[1]))
df.drop(df.columns[[0]], axis=1, inplace=True)
df['CP_CLIENTE'] = df['CP_CLIENTE'].apply(lambda x: '{0:0>5}'.format(x))
df

with open('datos/almeria_20.json') as f:
  states_topo = json.load(f)

m = folium.Map(location=[37.16, -2.33], zoom_start=9)

# folium.TopoJson(states_topo,'objects.almeria_wm').add_to(m)

folium.Choropleth(
    geo_data=states_topo,
    topojson='objects.almeria_wm',
    name="choropleth",
    data=df,
    columns=["CP_CLIENTE", "total"],
    key_on="feature.properties.COD_POSTAL",
    fill_color="YlGn",
    fill_opacity=0.7,
    line_opacity=0.2,
    legend_name="Barrios con mas compras sin tienda de alimentacion",
).add_to(m)

folium_static(m)


df = pd.read_csv('gs://datosbd/{}'.format(csv_names[2]))
df.drop(df.columns[[0]], axis=1, inplace=True)
df['CP_CLIENTE'] = df['CP_CLIENTE'].apply(lambda x: '{0:0>5}'.format(x))
df


m2 = folium.Map(location=[37.16, -2.33], zoom_start=9)

folium.Choropleth(
    geo_data=states_topo,
    topojson='objects.almeria_wm',
    name="choropleth",
    data=df,
    columns=["CP_CLIENTE", "total"],
    key_on="feature.properties.COD_POSTAL",
    fill_color="YlGn",
    fill_opacity=0.7,
    line_opacity=0.2,
    legend_name="otra",
).add_to(m2)

folium_static(m2)

df = pd.read_csv('gs://datosbd/{}'.format(csv_names[3]))
df.drop(df.columns[[0]], axis=1, inplace=True)
df['CP_CLIENTE'] = df['CP_CLIENTE'].apply(lambda x: '{0:0>5}'.format(x))
df


m3 = folium.Map(location=[37.16, -2.33], zoom_start=9)

folium.Choropleth(
    geo_data=states_topo,
    topojson='objects.almeria_wm',
    name="choropleth",
    data=df,
    columns=["CP_CLIENTE", "total"],
    key_on="feature.properties.COD_POSTAL",
    fill_color="YlGn",
    fill_opacity=0.7,
    line_opacity=0.2,
    legend_name="otra",
).add_to(m3)

folium_static(m3)

# class Section(Enum):
#     INTRO = "Introduction"
    
#     BAR_CHART = "Static Embed: Bar Chart"
#     PENGUINS =  "Static Embed: Scatterplot Matrix Penguins"
#     SPIKE = "Static Embed: Spike Map"
#     VORONOI = "Static Embed: Trader Joes Voronoi Map"
#     WIKI = "Static Embed: Bar Chart Race of Wikipedia Views"

#     FORM = "Bi-Directional Embed: HTML Form"
#     DRAW = "Bi-Directional Embed: Drawing Canvas"
#     COUNTIES = "Bi-Directional Embed: Selecting Counties"
#     MATRIX = "Bi-Directional Embed: Matrix Input"


# sections = list(map(lambda d: d.value, Section))
# section_i = 0
# section_param = st.experimental_get_query_params().get("section")
# if section_param and section_param[0] in sections:
#     section_i = sections.index(section_param[0])

# section = st.sidebar.radio(
#     "Section",
#     sections,
#     index=section_i
# )

# if section == Section.INTRO.value:
#     st.experimental_set_query_params(section=Section.INTRO.value)

#     st.write("""
# # Introducing [`streamlit-observable`](https://github.com/asg017/streamlit-observable)!
# 👋🏼 Hello! This Streamlit app is an introduction to the [`streamlit-observable`](https://github.com/asg017/streamlit-observable) 
# library - a Streamlit custom component for embeding [Observable notebooks](https://observablehq.com)
# into Streamlit apps. You can render, re-use, and recycle any Observable notebook
# found on [observablehq.com](https://observablehq.com), 
# giving you access to hundreds of data visualizations,
# maps, charts, and animations that you can embed into any Streamlit app!
# 👈🏼Check out the sidebar for a deep-dive into different ways you can use 
# `streamlit-observable` in your apps. Each example has a checkbox that looks like this:""")
