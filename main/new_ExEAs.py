from selenium import webdriver
from selenium.webdriver.support.select import Select
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import datetime as dt
import pandas as pd
import os
from datetime import datetime, timedelta
import psycopg2
from sqlalchemy import create_engine


# get current date
current_date = datetime.now().date()

# calculate the date of yesterday
dateTo = current_date - timedelta(days=1)
yesterday_str = dateTo.strftime('%Y.%m.%d')

# setting them so it can give me the data from 00:00 - 23:00 yesterday
start_time = pd.to_datetime(f'{yesterday_str} 00:00')
end_time = pd.to_datetime(f'{yesterday_str} 23:00')


# establish a connection to PostgreSQL database
connection = psycopg2.connect(
    host='localhost',
    database='ExEA',
    user='postgres',
    password='mohi1234'
)

# create SQLAlchemy engine
engine = create_engine('postgresql+psycopg2://postgres:mohi1234@localhost/ExEA')

# this is the path where you want to search
path = 'C:\\Users\\35987\\Downloads'

# this is the extension to detect
extension = '.csv'
all_files = []



def ScrapeData():

    # ChromeDriver path
    PATH = "C:\Program Files (x86)\chromedriver-win64\chromedriver.exe"
    
    # Dates to set for the calendar
    today_date = dt.date.today()
    two_days_ago = today_date - dt.timedelta(days=2)
    yesterday = today_date - dt.timedelta(days=1)

    month = two_days_ago.month - 1
    print(month)
    # Get the day numbers
    dayFrom = two_days_ago.day
    dayTo = yesterday.day

    # URL's of all stations
    stations = {
           'pavlovo': 'https://eea.government.bg/kav/reports/air/qReport/10/01',
           'mladost': 'https://eea.government.bg/kav/reports/air/qReport/102/01',
           'druzhba': 'https://eea.government.bg/kav/reports/air/qReport/01/01',
           'nadezhda': 'https://eea.government.bg/kav/reports/air/qReport/03/01',
           'hipodruma':'https://eea.government.bg/kav/reports/air/qReport/04/01'
           }
    
    for station in stations.keys():
        driver = webdriver.Chrome(PATH)
        driver.get(stations[station])


        #Set starting Date
        calendar1= driver.find_element(By.CSS_SELECTOR, '.dateFields1 tr:nth-child(1) .ui-datepicker-trigger')
        calendar1.click() # open calendar

        date1 = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.LINK_TEXT, f'{dayFrom}')))
        date1.click() # <- only select date as the month will already be set by default
        time.sleep(2)

        calendar1.click() # closing the calendar
        
        
        #Set Ending Date
        calendar2 = driver.find_element(By.CSS_SELECTOR, '.dateFields1 tr:nth-child(2) .ui-datepicker-trigger')
        calendar2.click()
        time.sleep(2)
            
        # Adjust the month when its the begining of the month because by default it will take the current as the end date which has no data yet
        if today_date.day == 1 or today_date.day == 2:
            month_scrollbar = driver.find_element(By.CLASS_NAME, 'ui-datepicker-month')
            month_scrollbar.click()
            select = Select(month_scrollbar)
            select.select_by_value(str(month))
            time.sleep(2)
            
        date2 = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.LINK_TEXT, f'{dayTo}')))
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
            
            
            driver.back() # Going back to the page to extract the following checkboxes's data
            
            time.sleep(2)# Wait for the page to load
            
            # Setting the dates once again because the calendar goes back to default input after using driver.back from another page
            #Set starting Date
            calendar1= driver.find_element(By.CSS_SELECTOR, '.dateFields1 tr:nth-child(1) .ui-datepicker-trigger')
            calendar1.click() # open calendar

            date1 = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.LINK_TEXT, f'{dayFrom}')))
            date1.click() # <- only select date as the month will already be set by default
            time.sleep(2)

            calendar1.click() # closing the calendar
            
            
            #Set Ending Date
            calendar2 = driver.find_element(By.CSS_SELECTOR, '.dateFields1 tr:nth-child(2) .ui-datepicker-trigger')
            calendar2.click()
            time.sleep(2)
            # Adjust the month when its the begining of the month because by default it will take the current as the end date which has no data yet
            if today_date.day == 1 or today_date.day == 2 :
                month_scrollbar = driver.find_element(By.CLASS_NAME, 'ui-datepicker-month')
                month_scrollbar.click()
                select = Select(month_scrollbar)
                select.select_by_value(str(month))
                time.sleep(2)

            date2 = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.LINK_TEXT, f'{dayTo}')))
            date2.click()
            
            calendar2.click() # closing the calendar
            
            checkboxes = driver.find_elements(By.CSS_SELECTOR, 'input[type="checkbox"]')#locate the checkbox elements again
            checkbox = checkboxes[i] # Reassign the correct checkbox element
            checkbox.click() #unchecking the checkbox that was alredy checked
            

                        
        time.sleep(2)    

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
                                'Фини прахови частици < 2.5um [PM2.5] ug/m3': 'PM2', 'Озон [O3] ug/m3': 'O3',
                                'Азотен оксид [NO] ug/m3': 'NO', 'Азотен диоксид [NO2] ug/m3': 'NO2',
                                'Серен диоксид [SO2] ug/m3': 'SO2', 'Въглероден оксид [CO] mg/m3': 'CO',
                                'Бензен [Benzene] ug/m3': 'BENZENE', 'Температура [AirTemp] Celsius': 'TEMP',
                                'Посока на вятъра [WD] degree': 'WINDDIR','Кардинална посока': 'DIRECTION',
                                'Скорост на вятъра [WS] m/s': 'WINDSPD', 'Относителна влажност [UMR] %': 'HUMIDITY',
                                'Атмосферно налягане [Press] mbar': 'PRESSURE', 'Слънчева радиация [GSR] W/m2': 'SOLRAD'
                            }  # new names for the columns
                            
                    df = df.replace(['С','И','З','Ю','СИ','СЗ','ЮИ','ЮЗ','ССИ','СИИ','ИЮИ','ЮЮИ','ЮЮЗ','ЗЮЗ','ЗСЗ','ССЗ'], #Cardinal directions
                        ['N','E','W','S','NE','NW','SE','SW','NNE','ENE','ESE','SSE','SSW','WSW','WNW','NNW'])
                            
                    # rename the columns with colsNew
                    df.rename(columns=colsNew, inplace=True)
                            
                    unnamed_columns = f'Unnamed: {len(df.columns)-1}'
                            
                    # delete last Unnamed column if any
                    if unnamed_columns in df.columns:
                        df.drop(unnamed_columns, inplace=True, axis=1)
                    
                    if 'WINDCDIR' in df.columns:
                        df.drop('WINDCDIR', inplace=True, axis=1)
                                
                    df['Time'] = df['Time'].str.replace('24:00', '00:00') ## replacing 24:00 with 00:00
                    df['Time'] = pd.to_datetime(df['Time'], format="%d.%m.%Y %H:%M")

                    # adjust the dates if the time is "00:00"
                    df.loc[df['Time'].dt.hour == 0, 'Time'] += pd.DateOffset(days=1) ##if the time is 00:00 it skipks to the next day because the day is set to the previous one

                    # filter the DataFrame based on the specified date range
                    df = df[(df['Time'] >= start_time) & (df['Time'] <= end_time)] ## filter the times so its from 00:00 until 23:00 of the previous day
                            
                    # drop the 'Time' column after adding it once to the csv
                    if 'Time' in df.columns:
                        if not all_files:
                            all_files.append(df)
                        else:
                            df = df.drop('Time', axis=1)
                            all_files.append(df)
                   
                            
                            
                # combine all CSV files 
        combined_df = pd.concat(all_files, axis=1)
        print(combined_df)
        all_files.clear()#clear the previous data in the list


        # Import the combined_df into the PostgreSQL database
        table_name = station  # Specify the name of the table in the database  I am getting station based on the loop above
        print(table_name)
        
        combined_df.to_sql(table_name, engine, if_exists='replace', index=False) ## sending the data to the database to the specified table

        # Close the connection
        connection.close()
                
        
ScrapeData()
