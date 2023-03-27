import gpxpy
import pandas as pd
from geopy.geocoders import Nominatim
import sqlite3

geolocator = Nominatim(user_agent="geoapiExercises")

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

def city_state_country(row):
    coord = f"{row['latitude']}, {row['longitude']}"
    location = geolocator.reverse(coord, exactly_one=True)
    address = location.raw['address']
    barrio=address.get('neighbourhood','')
    #suburb=address.get('suburb','')
    city = address.get('city', '')
    state = address.get('state', '')
    country = address.get('country', '')
    row['barrio'] = barrio
    #row['sub_urb'] = suburb
    row['city'] = city
    row['state'] = state
    row['country'] = country
    return row


df1 = read_gpx('recovery.01-Mar-2022-1533.gpx')
df2 = read_gpx('recovery.05-Mar-2022.1025.gpx')
df3 = read_gpx('recovery.25-May-2022-0907.gpx')

df1 = df1.round({'latitude': 6, 'longitude': 6, 'elevation': 2})
df2 = df2.round({'latitude': 6, 'longitude': 6, 'elevation': 2})
df3 = df3.round({'latitude': 6, 'longitude': 6, 'elevation': 2})

dft=pd.concat([df1,df2,df3],axis=0).reset_index()
dft['time']=dft.time.astype(str)
dft['time']=pd.to_datetime(dft['time'])

dft['Round_lat']=dft.latitude.round(3)
dft['Round_long']=dft.longitude.round(3)
df_group=dft.drop_duplicates(subset=['Round_lat','Round_long'])

df_locs = df_group.apply(city_state_country, axis=1)
df_locs.reset_index()
cols_locs=['time', 'latitude', 'longitude', 'elevation', 'Round_lat',
       'Round_long', 'barrio', 'city', 'state', 'country']

df_locs=df_locs[cols_locs]

cols_merge=[col  for col in df_locs.columns if col not in dft.columns]
keys=['Round_lat','Round_long']
for key in keys:
  cols_merge.append(key)

df_locations_merge=df_locs[cols_merge]

df_export=dft.merge(df_locations_merge,how='left',on=['Round_lat','Round_long']).reset_index()

df_export[cols_merge]=df_export[cols_merge].replace('','Sin data')

replace_chars=[('á','a'),('é','e'),('í','i'),('ó','o'),('ú','u')]

for col in cols_merge:
  df_export[col]=df_export[col].astype(str).str.strip()
  for char in replace_chars:
    df_export[col]=df_export[col].str.replace(char[0],char[1])

df_export['time']=df_export['time'].values.astype('datetime64[s]')
# df_export
# df_export.to_csv('Coordenadas.csv',sep=';',index=False)

# Base de datos 
df_export.drop(['index'], axis=1)
df_export['time']=df_export['time'].values.astype('datetime64[s]')

conn = sqlite3.connect('base_general.db')
conn.execute('''CREATE TABLE datos_crudos (
              level_0 INT,
              time TEXT, 
              latitude FLOAT, 
              longitude FLOAT, 
              elevation FLOAT, 
              Round_lat FLOAT,
              Round_long FLOAT,
              barrio TEXT,
              city TEXT,
              state TEXT,
              country TEXT

)''')

# level_0;index;time;latitude;longitude;elevation;Round_lat;Round_long;barrio;city;state;country

for index, row in df_export.iterrows():
    query = "INSERT INTO datos_crudos (level_0, time, latitude, longitude, elevation, Round_lat, Round_long, barrio, city, state, country ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    conn.execute(query, (
                        row['level_0'], 
                        row['time'].strftime('%Y-%m-%d %H:%M:%S'),
                        row['latitude'], 
                        row['longitude'], 
                        row['elevation'], 
                        row['Round_lat'],
                        row['Round_long'],
                        row['barrio'], 
                        row['city'],
                        row['state'], 
                        row['country']))

conn.commit()
conn.close()