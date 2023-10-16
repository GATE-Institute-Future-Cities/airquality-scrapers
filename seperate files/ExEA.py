from selenium import webdriver
from selenium.webdriver.support.select import Select
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
import time
import datetime as dt

options = Options()
options.add_argument("--headless")

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
           'AE1': 'https://eea.government.bg/kav/reports/air/qReport/10/01', #pavlovo
           }
    
    for station in stations.keys():
        driver = webdriver.Chrome(PATH, options=options)
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
        if today_date.day == 1:
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
            if today_date.day == 1:
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
        
        
ScrapeData()
