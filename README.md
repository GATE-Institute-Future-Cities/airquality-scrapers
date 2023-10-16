# ExEa-Scraper


The method (Scraper) for collecting data on the quality of atmospheric air (KAV) from the stations of the Executive Environmental Agency (IAOS) into a database.
I wrote code that downloads data from the IAOS platform for each KAV factor (e.g., temperature, NO2, etc.) separately, for the previous day, and then compiles the data in one place for each element separately in a single CSV file. The code collects data from each of the five stations (Sofia, Pavlovo, Hippodrome, Druzhba, Mladost). 
The code takes into account the time characteristics of the data, automatically filling in missing records with a pre-defined (and freely selectable) value.
This was necessary to filter the data for each element and record it for the previous day because the IAOS platform provided unnecessary data for each element.
The method uploads the read data to the specified Postgres database. It is highly automated and equipped with notification functions for events such as successful data recording for the day, errors, and more.
