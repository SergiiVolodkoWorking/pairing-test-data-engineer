Answers
==============================
# The user story questions

## 1. How many days does the flights table cover ?
- The question id explored in `data-exploration` notebook
- The data is prepared by `get_covered_days_count` step of the pipeline and stored to `general_statistics.json` output file.
- All used transformations are covered with unit tests

## 2. How many departure cities the flight database covers ?
- The question id explored in `data-exploration` notebook
- The data is prepared by `get_departure_cities_count` step of the pipeline and stored to `general_statistics.json` output file.
- All used transformations are covered with unit tests

## 3. What is the relationship between flights and planes tables ?
- The question is investigated in `flights-and-planes-analysis` notebook
- Conclusions:
- -- Planes of MCDONNELL DOUGLAS AIRCRAFT CO, MCDONNELL DOUGLAS, EMBRAER and BOMBARDIER INC are used for flights <1600 miles
- -- Planes of AIRBUS INDUSTRIE are used for flights <2600 miles
- -- Planes of AIRBUS, BOEING are covering around 5000 mi flights

## 4. Which airplane manufacturer incurred the most delays in the analysis period ?
- The question is investigated in `flights-and-planes-analysis` notebook
- Conclusions:
- -- About 50% of all flights departs earlier
- -- BOEING and AIRBUS has the most delayed departures
- -- 25% of all flights arrive more then 10 minutes earlier
- -- Other 25% of flights arrive at least 2 minutes earlier
- -- BOEING and AIRBUS has the most delayed arrivals
- The data is prepared by `find_biggest_delays` step of the pipeline and stored to output files: `arrival_delays.csv`, `departure_delays.csv`
- All used transformations are covered with unit tests

# 5. What are the two most connected cities?
- The question is investigated in `cities-connectivity-analysis` notebook
- The data is prepared by `calculate_cities_connectivity` step of the pipeline and stored to `most_connected_cities.csv` output file
- All used transformations are covered with unit tests

# Installation instructions
1. Install anaconda
- `brew cask install anaconda`
- `export PATH="/usr/local/anaconda3/bin:$PATH"`
- Make sure that your system now has this python as default: `/usr/local/anaconda3/bin/python`
2. Add Metaflow to anaconda
- For anaconda's python execute:
`pip install metaflow`

# Running the pipeline
1. Data must be placed in `data` folder
2. From project root execute `python pipeline.py run`

The pipeline results will appear in `data/output`


# Changes for way bigger data
- **Extract remote data**
Metaflow provides easy way of working with data stored in Amazon s3. The switch there is expected to be straight forward. For other cloud platforms data extraction steps require some changes. Those steps are isolated and existing python libraries must be enough to download files from cloud and read them a `pandas` dataframe.

- **Optimization: save data right after the transformation**

- **Switch to pyspark and AWS clusterisation infrastructure**
In case of enormous data.

- **Performance monitoring will be needed**

# Considerations on deploying current solution to production
- Current version can be deployed directly to a dedicated VM as any other python utility software.
- Pipeline scheduling must be set on that machine manually or with use some scheduling tools.
- In current implementation the data we want to pump must be placed into the `data` folder. It can be done by scheduling a script that can get this data from somewhere.
- Accessing external data sources will require secrets management.
- Setup own logging or figure out how to use one embedded into metaflow.
- Some notifications (like Slack message on pipeline completed) will be very useful.