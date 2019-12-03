Data Engineer Pairing Exercise
==============================


# The Task

The task involves developing a data pipeline and its underlying infrastructure. In order to complete the following user story sample data sources will be provided.


### User story:

we would like to explore and process the flights data in order to answer questions such as:
- how many days does the flights table cover ?
- how many departure cities the flight database covers ?
- what is the relationship between flights and planes tables ?
- which airplane manufacturer incurred the most delays in the analysis period ?
- what are the two most connected cities?


### The input data sources are comprised of (csv files in data.tar.gz):

- airlines
- airports
- flights
- planes
- weather


### Output:

- answer the above user story questions
- provide instructions on how to create the pipeline and the infrastructure
- the data is kept small for the exercice, what considerations can be made when dealing with real data.
- what considerations can be made to promote this work to production.


# Extra Credit
Another user story related to the launch of a loyalty program has reached us,
customer data will be coming in json format.  
how can we use this customer data alongside the above data to enable this program ?


# Acknowledgements
The flight delay and cancellation data was collected and published by the DOT's Bureau of Transportation Statistics.  
the files in data tarball are sourced from the following repos:

https://raw.githubusercontent.com/hadley/nycflights13/master/data-raw/airlines.csv
https://raw.githubusercontent.com/hadley/nycflights13/master/data-raw/airports.csv
https://raw.githubusercontent.com/hadley/nycflights13/master/data-raw/planes.csv
https://raw.githubusercontent.com/hadley/nycflights13/master/data-raw/weather.csv  
https://github.com/rich-iannone/so-many-pyspark-examples/raw/master/data-files/nycflights13.csv (renamed to flights.csv)
