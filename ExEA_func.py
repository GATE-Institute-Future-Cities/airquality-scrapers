from selenium import webdriver
from selenium.webdriver.support.select import Select
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
import os
import psycopg2
from sqlalchemy import create_engine




all_files = []



def connectDB(hostN, db, username, passWord):
    # establish a connection to PostgreSQL database
    psycopg2.connect(
        host=hostN,
        database=db,
        user=username,
        password=passWord
    )

    # create SQLAlchemy engine
    create_engine(f'postgresql+psycopg2://{username}:{passWord}@{hostN}/{db}')
    
    print('Database successfully connected')
    
    
def ScrapeData():
    month_prompt = 'Please select the month: '

    day_start = input('Please select the day for the STARTING date: ')
    month_start = int(input(month_prompt)) - 1 ## removing one because the value of the month starts from 0 - 11 / Jan-Dec
    year_start = input('Please select the year: ')


    day_end = input('Please select the day for the ENDING date: ')
    month_end = int(input(month_prompt)) - 1
    year_end = input('Please select the year: ')
    
    PATH = "C:\Program Files (x86)\chromedriver.exe"

    # URL's of all stations
    stations = {
           'AE1': 'https://eea.government.bg/kav/reports/air/qReport/10/01', #pavlovo
           'AE2':'https://eea.government.bg/kav/reports/air/qReport/04/01', #hipodruma
           'AE3': 'https://eea.government.bg/kav/reports/air/qReport/03/01', #nadezhda
           'AE4': 'https://eea.government.bg/kav/reports/air/qReport/102/01', #mladost
           'AE5': 'https://eea.government.bg/kav/reports/air/qReport/01/01', #druzhba
           }
    
    for station in stations.keys():
        driver = webdriver.Chrome(PATH)
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

def filterData():
    # this is the path where it searches for the scraped csv files on your PC/Laptop
    path = 'C:\\Users\\35987\\Downloads'
    
    # this is the extension to detect
    extension = '.csv'

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
                            'Посока на вятъра [WD] degree': 'WD','Кардинална посока': 'DIRECTION',
                            'Скорост на вятъра [WS] m/s': 'WS', 'Относителна влажност [UMR] %': 'RH',
                            'Атмосферно налягане [Press] mbar': 'p', 'Слънчева радиация [GSR] W/m2': 'SI'
                }   # new names for the columns
                        
                df = df.replace(['С','И','З','Ю','СИ','СЗ','ЮИ','ЮЗ','ССИ','СИИ','ИЮИ','ЮЮИ','ЮЮЗ','ЗЮЗ','ЗСЗ','ССЗ'], #Cardinal directions
                    ['N','E','W','S','NE','NW','SE','SW','NNE','ENE','ESE','SSE','SSW','WSW','WNW','NNW'])
                        
                # rename the columns with colsNew
                df.rename(columns=colsNew, inplace=True)
                        
                unnamed_columns = f'Unnamed: {len(df.columns)-1}'
                        
                # delete last Unnamed column if any
                if unnamed_columns in df.columns:
                    df.drop(unnamed_columns, inplace=True, axis=1)
                            

                # drop the 'Time' column after adding it once to the csv
                if 'Time' in df.columns:
                    if not all_files:
                        all_files.append(df)
                    else:
                        df = df.drop('Time', axis=1)
                        all_files.append(df)
                os.remove(file_name_path) ## remove the csv file after reading it
