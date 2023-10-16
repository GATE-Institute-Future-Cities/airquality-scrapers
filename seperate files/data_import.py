import os
import pandas as pd
import psycopg2
from datetime import datetime, timedelta



# get current date
current_date = datetime.now().date()

# calculate the date of yesterday
dateTo = current_date - timedelta(days=1)
yesterday_str = dateTo.strftime('%Y.%m.%d')

# setting them so it can give me the data from 00:00 - 23:00 yesterday
start_time = pd.to_datetime(f'{yesterday_str} 00:00')
end_time = pd.to_datetime(f'{yesterday_str} 23:00')

# PostgreSQL database credentials
db_host = "localhost"
db_name = "ExEA"
db_user = "postgres"
db_password = "mohi1234"

# establish a connection to PostgreSQL database
connection = psycopg2.connect(
    host=db_host,
    database=db_name,
    user=db_user,
    password=db_password
)

cursor = connection.cursor()

# this is the path where you want to search
path = 'C:\\Users\\35987\\Downloads'

# this is the extension to detect
extension = '.csv'
existing_times = []  # track existing timestamps
rows_to_insert = [] # ---> this contains dic for each element and its value EX: [ {'EL': [V1,V2,V3,V4....]} ,  {'EL2': [V1,V2,V3,V4....]} ...]


for root, dirs_list, files_list in os.walk(path):
    for file_name in files_list:
        if os.path.splitext(file_name)[-1] == extension:
            file_name_path = os.path.join(root, file_name)
            print(file_name_path)  # this is the full path of the file
            
            df = pd.read_csv(file_name_path) # reading each csv file in the path Downloads
            
            colsNew = {
                'Дата/Час': 'Time', 'Фини прахови частици < 10um [PM10] ug/m3': 'PM10',
                'Фини прахови частици < 2.5um [PM2.5] ug/m3': 'PM2', 'Озон [O3] ug/m3': 'O3',
                'Азотен оксид [NO] ug/m3': 'NO', 'Азотен диоксид [NO2] ug/m3': 'NO2',
                'Серен диоксид [SO2] ug/m3': 'SO2', 'Въглероден оксид [CO] mg/m3': 'CO',
                'Бензен [Benzene] ug/m3': 'BENZENE', 'Температура [AirTemp] Celsius': 'TEMP',
                'Посока на вятъра [WD] degree': 'WINDDIR', 'Кардинална посока': 'WINDCDIR',
                'Скорост на вятъра [WS] m/s': 'WINDSPD', 'Относителна влажност [UMR] %': 'HUMIDITY',
                'Атмосферно налягане [Press] mbar': 'PRESSURE', 'Слънчева радиация [GSR] W/m2': 'SOLRAD'
            } # new names for the columns
            
            # rename the columns with colsNew
            df.rename(columns=colsNew, inplace=True)
            
            unnamed_columns = f'Unnamed: {len(df.columns)-1}'
            
            # delete last Unnamed column if any
            if unnamed_columns in df.columns:
                df.drop(unnamed_columns, inplace=True, axis=1)

            # check if 'WINDDIR' column exists and drop it (because we have 3 columns the third one is for the direction of the wind)
            if 'WINDCDIR' in df.columns:
                df.drop('WINDCDIR', inplace=True, axis=1)

            # convert the 'Time' column to datetime format
            df['Time'] = df['Time'].str.replace('24:00', '00:00')
            df['Time'] = pd.to_datetime(df['Time'], format="%d.%m.%Y %H:%M")

            # adjust the dates if the time is "00:00"
            df.loc[df['Time'].dt.hour == 0, 'Time'] += pd.DateOffset(days=1)

            # filter the DataFrame based on the specified date range
            filtered_df = df[(df['Time'] >= start_time) & (df['Time'] <= end_time)]

            # initialize a dictionary to store data for each element
            data_by_element = {}
            for _, row in filtered_df.iterrows():
                values = tuple(row)
                timestamp = values[0]
                if timestamp not in existing_times:
                    existing_times.append(timestamp)

                # Iterate over the column-value pairs starting from the second column because we don't need the time
                for i, value in enumerate(values[1:], start=1):
                    column_name = filtered_df.columns[i]

                    # Check if the dictionary for this element already exists
                    if column_name not in data_by_element:
                        data_by_element[column_name] = []
                        
                    if pd.isnull(value): #checking if the value in the column is empty and replacing it with None 
                        value = None

                    # Append the value to the corresponding element list
                    data_by_element[column_name].append(value)

            rows_to_insert.extend([{element: data_by_element[element]} for element in data_by_element])
            os.remove(file_name_path) # removing the csv file after getting the data

rows_to_insert.insert(0, {'Time': [times for times in existing_times]}) ## inserting the timestamps as the first index because it's the first col

table_name = "pavlovo"
columns = []
values = []
rows = []

while len(rows_to_insert[0]['Time']): ## looping until there is no timestamps 
    for element in rows_to_insert:
        for key, value in element.items():  ###{'EL': [V1,V2,V3,V4....]} --> {'KEY': [V1,V2,V3,V4....]}
            first_value = value[0]
            
            if key not in columns:
                columns.append(key)

            values.append(first_value)## apending the first value of each element so we can add them row by row
            value.pop(0)#removing the element afetr adding it to avoid adding it again
            
    rows.append(tuple(values)) 
    values.clear()#clear the old row to add the new row

# generate placeholders for the columns and values
column_placeholders = ','.join(columns)
value_placeholders = ','.join(['%s'] * len(columns))

# build the insert statement with column names and placeholders
insert_query = f"INSERT INTO {table_name} ({column_placeholders}) VALUES ({value_placeholders})"
print(insert_query)

# execute the insert statement for each row
for row in rows:
    print(row)
    cursor.execute(insert_query, row)

# Commit the changes to the database
connection.commit()

# Close the cursor and connection
cursor.close()
connection.close()
