from selenium import webdriver
from selenium.webdriver.support.select import Select
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
import os
import psycopg2
from sqlalchemy import create_engine




def connectDB(hostN, db, username, passWord):
    
    try:
        
        # establish a connection to PostgreSQL database
        connection = psycopg2.connect(
            host=hostN,
            database=db,
            user=username,
            password=passWord
        )

        # create SQLAlchemy engine
        create_engine(f'postgresql+psycopg2://{username}:{passWord}@{hostN}/{db}')
        
        print('Database successfully connected')
        print('Engine successfully created')
        
        return connection
    
    except psycopg2.OperationalError as e:
        print(f"Error: {e}")
        return None, None  # Return None values to indicate a failed connection
    
    

## 1.ChromedriverPath  2. a dict with stationnames as keys and url to the website as values  3...  the period of the data you want
def scrapeData(chdriverPath, stations, day_start, month_start, year_start, day_end, month_end, year_end):

    
    for station in stations.keys():
        driver = webdriver.Chrome(chdriverPath)
        driver.get(stations[station])


        #Set starting Date
        calendar1= driver.find_element(By.CSS_SELECTOR, '.dateFields1 tr:nth-child(1) .ui-datepicker-trigger')
        calendar1.click() # open calendar
        
        month_scrollbar = driver.find_element(By.CLASS_NAME, 'ui-datepicker-month') ## setting the month for the starting date
        month_scrollbar.click()
        select = Select(month_scrollbar)
        select.select_by_value(str(month_start))
        
        year_scrollbar = driver.find_element(By.CLASS_NAME, 'ui-datepicker-year')## setting the year for the starting date
        year_scrollbar.click()
        select = Select(year_scrollbar)
        select.select_by_value(year_start)

        date1 = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.LINK_TEXT, f'{day_start}')))
        date1.click() 

        calendar1.click() # closing the calendar
        
        
        #Set Ending Date
        calendar2 = driver.find_element(By.CSS_SELECTOR, '.dateFields1 tr:nth-child(2) .ui-datepicker-trigger')
        calendar2.click()
        
        
        month_scrollbar = driver.find_element(By.CLASS_NAME, 'ui-datepicker-month')
        month_scrollbar.click()
        select = Select(month_scrollbar)
        select.select_by_value(str(month_end))
        
        year_scrollbar = driver.find_element(By.CLASS_NAME, 'ui-datepicker-year')## setting the year for the ending date
        year_scrollbar.click()
        select = Select(year_scrollbar)
        select.select_by_value(year_end)
        
        date2 = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.LINK_TEXT, f'{day_end}')))
        date2.click()

        
        calendar2.click() # closing the calendar
        
        
        # Wait for the page to load and locate the checkbox elements
        checkboxes = WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, 'input[type="checkbox"]')))
        
        # Click on each checkbox skipping the first one
        for i in range(1, len(checkboxes)):
            
            loadBut = driver.find_element(By.ID, "scroll") # Get load button
            checkbox = checkboxes[i]  #get the check box button
            checkbox.click()
            loadBut.click()
            
            # Downloading CSV Files
            exportBut = driver.find_element(By.ID, "exportb1") # Get export button
            exportBut.click() # Getting all the files for each button
            
            checkboxes = WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, 'input[type="checkbox"]'))) # Relocate checkboxes this is set to avoid the error stale element after the page changes and to be able to uncheck the current so that we move to next on its own
            checkbox = checkboxes[i]  #get the check box button
            checkbox.click() ## uncheck

        # Close the browse
        driver.close()
        
    return stations
        

## 1.the path of the downloaded files with the scraped data
## 2. the extenstion of the file EX: '.csv' 
def filterData(path, fileExtension):
    all_files = []
    ## extracting the data from the The path in our device
    for root, dirs_list, files_list in os.walk(path):
        for file_name in files_list:
            if os.path.splitext(file_name)[-1] == fileExtension:
                file_name_path = os.path.join(root, file_name)
                print(file_name_path)  # this is the full path of the file
                        
                df = pd.read_csv(file_name_path, index_col=False)  # reading each csv file in the path and removing the index

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
                    
                if 'Кардинална посока' in df.columns: ## uneeded column 
                    df.drop('Кардинална посока', inplace=True, axis=1)
                            

                # drop the 'Time' column after adding it once to the csv
                if 'Time' in df.columns:
                    if not all_files:
                        all_files.append(df)
                    else:
                        df = df.drop('Time', axis=1)
                        all_files.append(df)
                os.remove(file_name_path) ## remove the csv file after reading it
                
    return all_files


def importData():
        all_files = filterData('C:\\Users\\35987\\Downloads', '.csv')
        connection = connectDB('localhost', 'ExEa_main', 'postgres', 'mohi1234')
        
        # combine all CSV files 
        combined_df = pd.concat(all_files, axis=1)
        elemets_df = combined_df.columns[1:] ### all the elemets in the dataframe without the time
        all_files.clear()#clear the previous data in the list


        query_stationid = 'SELECT stationid FROM airqualitystation WHERE stationname = %s' # this gets the station id for the currnet station
        query_allsensorid = 'SELECT sensorid FROM stationsensor WHERE stationid = %s' # get all the sensors for the current station id

        cursor = connection.cursor()

        cursor.execute(query_stationid, ('AE1',)) ## get the stationid 
        staionid_result = cursor.fetchone()
        stationid = staionid_result[0] ##  get the value only

        cursor.execute(query_allsensorid, (stationid,))
        all_sensorid_res = cursor.fetchall() #get all the sensor id that check with the parameter id in the sensors table
        all_sensorid = tuple(sens[0] for sens in all_sensorid_res)

        # Reshape the DataFrame using melt to create separate rows for each element so we can get the time and the value at that time
        melted_df = pd.melt(combined_df, id_vars=['Time'], value_vars=elemets_df, var_name='measuredparameterid', value_name='measuredvalue')


        query_paramid = 'SELECT id FROM parametertype WHERE parameterabbreviation = %s' # getting the param id
        query_sensorid = 'SELECT id FROM sensor WHERE parametername = %s AND id IN %s'   ## gtting the parameter id and checking it with the current paramid and checking the sensor in the all_sensors tuple to get the specific sensor id
        insert_query = "INSERT INTO public.airqualityobserved (measurementdatetime, measuredparameterid, measuredvalue, stationid, sensorid) VALUES (%s, %s, %s, %s, %s)"

        # Iterate over the rows of the melted DataFrame to insert data into the table
        for index, row in melted_df.iterrows():
            measurementdatetime = row['Time'] ## Time
            measuredparameter = row['measuredparameterid'] # current parameter
            measuredvalue = row['measuredvalue'] # value at the current time for the specific element
            stationid = stationid ## the iD of the station
            
            
            cursor.execute(query_paramid, (measuredparameter,))
            paramid_res = cursor.fetchone()
            measuredparameterid = paramid_res[0] ## parameter id 
            
            cursor.execute(query_sensorid, (measuredparameterid, all_sensorid,))
            sensorid_res = cursor.fetchone()
            sensorid = sensorid_res[0] ## sensor id
            
            ##adding all the values row by row
            cursor.execute(insert_query, (measurementdatetime, measuredparameterid, measuredvalue, stationid, sensorid))
            connection.commit()
