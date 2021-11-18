import numpy as np
import streamlit as st
import pandas as pd
from google.cloud import storage
import os

storage_client = storage.Client.from_service_account_json('big-data-328215-74151e35e325.json')
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "big-data-328215-74151e35e325.json"

st.write("Top 5 negocios más rentables")
data = pd.read_csv('gs://datosbd/barriosPorImporte.csv')
data.drop(data.columns[[0]], axis=1, inplace=True)
st.write(data)

st.write("En qué sector se gasta más los días lluviosos")

# data = pd.read_csv('topSectorLluvia.csv')
# st.bar_chart(data)

# data = pd.read_csv('topIngresosSector.csv')
# st.write(data)

df = pd.DataFrame({
  'first column': [10, 2, 3, 4],
  'second column': [10, 20, 30, 40]
})

st.write("Here's our first attempt at using data to create a table:")
st.write(pd.DataFrame({
    'first column': [1, 2, 3, 4],
    'second column': [10, 20, 30, 40]
}))

dataframe = np.random.randn(10, 20)
st.dataframe(dataframe)

chart_data = pd.DataFrame(
     np.random.randn(20, 3),
     columns=['a', 'b', 'c'])

st.line_chart(chart_data)

map_data = pd.DataFrame(
    np.random.randn(1000, 2) / [50, 50] + [36.84, -2.4675],
    columns=['lat', 'lon'])

st.map(map_data)

x = st.slider('x')  # 👈 this is a widget
st.write(x, 'squared is', x * x)

st.text_input("Your name", key="name")

# You can access the value at any point with:
st.session_state.name

if st.checkbox('Show dataframe'):
    chart_data = pd.DataFrame(
       np.random.randn(20, 3),
       columns=['a', 'b', 'c'])

    chart_data

option = st.selectbox(
    'Which number do you like best?',
     df['first column'])

'You selected: ', option

left_column, right_column = st.columns(2)
# You can use a column just like st.sidebar:
left_column.button('Press me!')

# Or even better, call Streamlit functions inside a "with" block:
with right_column:
    chosen = st.radio(
        'Sorting hat',
        ("Gryffindor", "Ravenclaw", "Hufflepuff", "Slytherin"))
    st.write(f"You are in {chosen} house!")

import time
'Starting a long computation...'

# Add a placeholder
latest_iteration = st.empty()
bar = st.progress(0)

# for i in range(100):
#   # Update the progress bar with each iteration.
#   latest_iteration.text(f'Iteration {i+1}')
#   bar.progress(i + 1)
#   time.sleep(0.1)

# '...and now we\'re done!'

col1, col2, col3 = st.columns(3)
col1.metric("Temperature", "70 °F", "1.2 °F")
col2.metric("Wind", "9 mph", "-8%")
col3.metric("Humidity", "86%", "4%")

if st.button('Say hello'):
    st.write('Why hello there')
else:
    st.write('Goodbye')


add_selectbox = st.sidebar.selectbox(
    "Aqui ponemos opciones chulas e interesantes xd",
    ("Email", "Home phone", "Mobile phone")
)
st.title('Uber pickups in NYC')

DATE_COLUMN = 'date/time'
DATA_URL = ('https://s3-us-west-2.amazonaws.com/'
            'streamlit-demo-data/uber-raw-data-sep14.csv.gz')
@st.cache
def load_data(nrows):
    data = pd.read_csv(DATA_URL, nrows=nrows)
    lowercase = lambda x: str(x).lower()
    data.rename(lowercase, axis='columns', inplace=True)
    data[DATE_COLUMN] = pd.to_datetime(data[DATE_COLUMN])
    return data

data_load_state = st.text('Loading data...')
data = load_data(10000)
data_load_state.text("Done! (using st.cache)")
st.write(data)
