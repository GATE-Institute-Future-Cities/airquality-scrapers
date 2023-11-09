import pandas as pd
import os
from datetime import datetime, timedelta
import psycopg2
from sqlalchemy import create_engine
from datetime import datetime



# establish a connection to PostgreSQL database
connection = psycopg2.connect(
    host='localhost',
    database='ExEa_main',
    user='postgres',
    password='mohi1234'
)

# this is the path where you want to search
path = 'C:\\Users\\35987\\Downloads'

# this is the extension to detect
extension = '.csv'
all_files = []
station_demo = 'AE1'

for root, dirs_list, files_list in os.walk(path):
    for file_name in files_list:
        if os.path.splitext(file_name)[-1] == extension:
            file_name_path = os.path.join(root, file_name)
            print(file_name_path)  # this is the full path of the file
                    
            df = pd.read_csv(file_name_path, index_col=False)  # reading each csv file in the path Downloads and removing the index

            colsNew = {
                        'Дата/Час': 'Time', 'Фини прахови частици < 10um [PM10] ug/m3': 'PM10',
                        'Фини прахови частици < 2.5um [PM2.5] ug/m3': 'PM2.5', 'Озон [O3] ug/m3': 'O3',
                        'Азотен оксид [NO] ug/m3': 'NO', 'Азотен диоксид [NO2] ug/m3': 'NO2',
                        'Серен диоксид [SO2] ug/m3': 'SO2', 'Въглероден оксид [CO] mg/m3': 'CO',
                        'Бензен [Benzene] ug/m3': 'C6H6', 'Температура [AirTemp] Celsius': 'T',
                        'Посока на вятъра [WD] degree': 'WD','Скорост на вятъра [WS] m/s': 'WS', 
                        'Относителна влажност [UMR] %': 'RH','Атмосферно налягане [Press] mbar': 'p', 
                        'Слънчева радиация [GSR] W/m2': 'SI',
            }   # new names for the columns
                    
                    
            # rename the columns with colsNew
            df.rename(columns=colsNew, inplace=True)
                    
            unnamed_columns = f'Unnamed: {len(df.columns)-1}'
                    
            # delete last Unnamed column if any
            if unnamed_columns in df.columns:
                df.drop(unnamed_columns, inplace=True, axis=1)
                
            if 'Кардинална посока' in df.columns:
                    df.drop('Кардинална посока', inplace=True, axis=1)                    
                    
                    
            df['Time'] = df['Time'].str.replace('24:00', '00:00') ## replacing 24:00 with 00:00           
      
            # drop the 'Time' column after adding it once to the csv
            if 'Time' in df.columns:
                if not all_files:
                    all_files.append(df)
                else:
                    df = df.drop('Time', axis=1)
                    all_files.append(df)
                    
                    
                                       
        # combine all CSV files 
combined_df = pd.concat(all_files, axis=1)
elemets_df = combined_df.columns[1:] ### all the elemets in the dataframe
datetime_df = combined_df['Time'].tolist()


query_stationid = 'SELECT stationid FROM airqualitystation WHERE stationname = %s' # this gets the station id for the currnet station
query_allsensorid = 'SELECT sensorid FROM stationsensor WHERE stationid = %s'

cursor = connection.cursor()

cursor.execute(query_stationid, (station_demo,)) ## get the stationid 
staionid_result = cursor.fetchone()
stationid = staionid_result[0]

cursor.execute(query_allsensorid, (stationid,))
all_sensorid_res = cursor.fetchall()#get all the sensor id that match with the station id 
all_sensorid = tuple(sens[0] for sens in all_sensorid_res)



print(all_sensorid)
print(staionid_result)
print(stationid)

# Reshape the DataFrame using melt to create separate rows for each element
melted_df = pd.melt(combined_df, id_vars=['Time'], value_vars=elemets_df, var_name='measuredparameterid', value_name='measuredvalue')
print(combined_df)
print(melted_df)
print(combined_df.columns)

# Iterate over the rows of the melted DataFrame to insert data into the PostgreSQL table
for index, row in melted_df.iterrows():
    measurementdatetime = datetime.strptime(row['Time'], "%d.%m.%Y %H:%M") ## Time
    measuredparameter = row['measuredparameterid'] # current parameter
    measuredvalue = row['measuredvalue'] # value at the current time for the specific element
    stationid = stationid ## the is of the station    
    print(type(measurementdatetime))
    print(measurementdatetime)
    
    query_paramid = 'SELECT id FROM parametertype WHERE parameterabbreviation = %s'
    cursor.execute(query_paramid, (measuredparameter,))
    paramid_res = cursor.fetchone()
    measuredparameterid = paramid_res[0] ## parameter id 

    query_sensorid = 'SELECT id FROM sensor WHERE parametername = %s AND id IN %s'
    cursor.execute(query_sensorid, (measuredparameterid, all_sensorid,))
    sensorid_res = cursor.fetchone()
    sensorid = sensorid_res[0] ## sensor id
    
    
    insert_query = "INSERT INTO public.airqualityobserved (measurementdatetime, measuredparameterid, measuredvalue, stationid, sensorid) VALUES (%s, %s, %s, %s, %s)"
    cursor.execute(insert_query, (measurementdatetime, measuredparameterid, measuredvalue, stationid, sensorid))
    connection.commit()


#close cursor
cursor.close()
# Close the connection
connection.close()

