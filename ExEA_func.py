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
    
