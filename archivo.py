import gpxpy
import pandas as pd
import sqlite3


def read_gpx(file: str) -> pd.DataFrame:
  df = None
  points = []

  with open(file) as f:
    gpx = gpxpy.parse(f)
  
  for segment in gpx.tracks[0].segments:
    for p in segment.points:
      points.append({
          'time': p.time,
          'latitude': p.latitude,
          'longitude': p.longitude,
          'elevation': p.elevation
      })
  df = pd.DataFrame.from_records(points)
  return df

df1 = read_gpx('recovery.01-Mar-2022-1533.gpx')
df2 = read_gpx('recovery.05-Mar-2022.1025.gpx')
df3 = read_gpx('recovery.25-May-2022-0907.gpx')

df1['time']=df1['time'].values.astype('datetime64[s]')
df2['time']=df2['time'].values.astype('datetime64[s]')
df3['time']=df3['time'].values.astype('datetime64[s]')

df1 = df1.round({'latitude': 6, 'longitude': 6, 'elevation': 2})
df2 = df2.round({'latitude': 6, 'longitude': 6, 'elevation': 2})
df3 = df3.round({'latitude': 6, 'longitude': 6, 'elevation': 2})

dft = pd.concat([df1, df2, df3], axis=0)
dft = dft.reset_index(drop=True)

# dft['year'] = pd.DatetimeIndex(dft['time']).year
# dft['month'] = pd.DatetimeIndex(dft['time']).month
# dft['day'] = pd.DatetimeIndex(dft['time']).day
# dft['hour'] = pd.DatetimeIndex(dft['time']).time

conn = sqlite3.connect('base_general.db')

# Crear tabla
conn.execute('''CREATE TABLE IF NOT EXISTS datos_gps
                (time TEXT, latitude REAL, longitude REAL, elevation REAL)''')

# Insertar los datos en la tabla, separando los datos por columna
for index, row in dft.iterrows():
    query = "INSERT INTO datos_gps (time, latitude, longitude, elevation) VALUES (?, ?, ?, ?)"
    conn.execute(query, (row['time'].strftime('%Y-%m-%d %H:%M:%S'), row['latitude'], row['longitude'], row['elevation']))


# Guardar los cambios y cerrar la conexión
conn.commit()
conn.close()
