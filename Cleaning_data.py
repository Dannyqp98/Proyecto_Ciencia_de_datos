import gpxpy
import pandas as pd

"""Lectura de datos"""
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

print(df1)
print(df2)
print(df3)

"""Formatos"""
df1['time']=df1['time'].values.astype('datetime64[s]')
df2['time']=df2['time'].values.astype('datetime64[s]')
df3['time']=df3['time'].values.astype('datetime64[s]')


df1 = df1.round({'latitude': 6, 'longitude': 6, 'elevation': 2})
df2 = df2.round({'latitude': 6, 'longitude': 6, 'elevation': 2})
df3 = df3.round({'latitude': 6, 'longitude': 6, 'elevation': 2})
print(df1)
print(df2)
print(df3)

dft = pd.concat([df1, df2, df3], axis=0)
dft = dft.reset_index(drop=True)
print(dft)

"""Campos para realizar análisis"""
dft['year'] = pd.DatetimeIndex(dft['time']).year
dft['month'] = pd.DatetimeIndex(dft['time']).month
dft['day'] = pd.DatetimeIndex(dft['time']).day
dft['hour'] = pd.DatetimeIndex(dft['time']).time
print(dft)

print(dft.year.value_counts())
print(dft.month.value_counts())
