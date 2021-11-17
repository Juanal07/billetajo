import numpy as np
import streamlit as st
import pandas as pd
from google.cloud import storage

storage_client = storage.Client.from_service_account_json('big-data-328215-74151e35e325.json')

st.write("Top 5 negocios más rentables")
data = pd.read_csv('gs://datosbd/topIngresosSector.csv')
data.drop(data.columns[[0]], axis=1, inplace=True)
# data = pd.DataFrame(data.total, columns=[data.SECTOR])
st.write(data)
# st.bar_chart(data)

# st.write("En qué sector se gasta más los días lluviosos")
# st.
