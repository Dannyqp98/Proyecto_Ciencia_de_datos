import gpxpy
import pandas as pd
from geopy.geocoders import Nominatim

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


df_export
df_export.to_csv('Coordenadas.csv',sep=';',index=False)