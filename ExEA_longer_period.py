from selenium import webdriver
from selenium.webdriver.support.select import Select
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
import os
import psycopg2
from sqlalchemy import create_engine
from datetime import datetime
from config import db_config


month_prompt = 'Please select the month: '

day_start = input('Please select the day for the STARTING date: ')
month_start = int(input(month_prompt)) - 1 ## removing one because the value of the month starts from 0 - 11 / Jan-Dec
year_start = input('Please select the year: ')


day_end = input('Please select the day for the ENDING date: ')
month_end = int(input(month_prompt)) - 1
year_end = input('Please select the year: ')



# establish a connection to PostgreSQL database
connection = psycopg2.connect(**db_config)

# this is the path where you want to search
path = 'C:\\Users\\35987\\Downloads'


# this is the extension to detect
extension = '.csv'
all_files = []

def ScrapeData():
    
    PATH = "C:\\Program Files (x86)\\chromedriver.exe"

    # URL's of all stations
    stations = {
           'AE1': 'https://eea.government.bg/kav/reports/air/qReport/10/01', #pavlovo
           'AE2':'https://eea.government.bg/kav/reports/air/qReport/04/01', #hipodruma
           'AE3': 'https://eea.government.bg/kav/reports/air/qReport/03/01', #nadezhda
           'AE4': 'https://eea.government.bg/kav/reports/air/qReport/102/01', #mladost
           'AE5': 'https://eea.government.bg/kav/reports/air/qReport/01/01', #druzhba
           }
    
    for station in stations.keys():
        driver = webdriver.Chrome()
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
    
        ## extracting the data from the The path in our device which is Dodwnloads
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
                                

                    # drop the 'Time' column after adding it once to the csv
                    if 'Time' in df.columns:
                        if not all_files:
                            all_files.append(df)
                        else:
                            df = df.drop('Time', axis=1)
                            all_files.append(df)
                    os.remove(file_name_path) ## remove the csv file after reading it
                            
                            
        # combine all CSV files 
        combined_df = pd.concat(all_files, axis=1)
        elemets_df = combined_df.columns[1:] ### all the elemets in the dataframe without the time
        all_files.clear()#clear the previous data in the list


        query_stationid = 'SELECT stationid FROM airqualitystation WHERE stationname = %s' # this gets the station id for the currnet station
        query_allsensorid = 'SELECT sensorid FROM stationsensor WHERE stationid = %s' # get all the sensors for the current station id

        cursor = connection.cursor()

        cursor.execute(query_stationid, (station,)) ## get the stationid 
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
            measurementdatetime = datetime.strptime(row['Time'], "%d.%m.%Y %H:%M") ## Time
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


    #close cursor
    cursor.close()
    # Close the connection
    connection.close()

                
                
        
ScrapeData()
