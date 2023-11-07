from selenium import webdriver
from selenium.webdriver.support.select import Select
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
import os
import psycopg2
from sqlalchemy import create_engine



month_prompt = 'Please select the month: '

day_start = input('Please select the day for the STARTING date: ')
month_start = int(input(month_prompt)) - 1 ## removing one because the value of the month starts from 0 - 11 / Jan-Dec
year_start = input('Please select the year: ')


day_end = input('Please select the day for the ENDING date: ')
month_end = int(input(month_prompt)) - 1
year_end = input('Please select the year: ')

def connectDB(hostN, db, username, passWord):
    # establish a connection to PostgreSQL database
    connection = psycopg2.connect(
        host=hostN,
        database=db,
        user=username,
        password=passWord
    )


    # create SQLAlchemy engine
    engine = create_engine(f'postgresql+psycopg2://{username}:{passWord}@{hostN}/{db}')