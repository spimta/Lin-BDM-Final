# BDM final
Covid impact, runs in NYU-HPC

To assess the food access problem in NYC before and during the COVID-19 pandemic, we would like to plot the visit patterns for all food stores (including restaurants, groceries, deli's, etc.)

we suspect that the visit patterns may vary across different type of stores. Our hypothesis is that we have changed our shopping behavior during the pandemic. For example, we visit Fast Food and Whole Saler restaurants more often comparing to full service restaurants and typical supermarkets. In particular, we are interested in the following store categories with their **NAICS** codes:

* *Big Box Grocers*: `452210` and `452311`
* *Convenience Stores*: `445120`
* *Drinking Places*: `722410`
* *Full-Service Restaurants*: `722511`
* *Limited-Service Restaurants*: `722513`
* *Pharmacies and Drug Stores*: `446110` and `446191`
* *Snack and Bakeries*: `311811` and `722515`
* *Specialty Food Stores*: `445210`, `445220`, `445230`, `445291`, `445292`, and `445299`
* *Supermarkets (except Convenience Stores)*: `445110`

The result is showned as:
* `year`: column is used for showing the trend line category (orange or blue).
* `date`: denotes the day of the year for each data point, for which we project to to year 2020. We chose 2020 as the base year because it is a leap year and would have all possile dates (i.e. month + day combination). The actual date for the data point would be month and day from `date` combined with the year in `year`.
* `median`: is used to draw the solid line describing the median visit counts across all stores for that date.
* `low`: the lower bound of the "confidence interval". In our plot, it is the `median` minus the standard deviation but will be kept at 0 or above.
* `high`: the higher bound of the "confidence interval". In our plot, it is the `median` plus the standard deviation but will be kept at 0 or above.

**NOTES**
* `low` and `high` value will be used to create the transparent area that we see in the plot.
* `low`, `median`, `high` should be computed not only for stores that had visits but also for all stores in `Core Places` that fit the category. As we learned previously, restaurants with no visits will not be reported in the `Weekly Pattern` data set.


## Objective
The task is to produce the visit pattern data for each of the store category above so that we can plot them in a similar way to our first plot for compare and contrast. You must process the 2 year worth of pattern data on the cluster, and produce 9 CSV-formated folders (header must be included), one for each category. Your code will be evaluated using 50 cores (5 executors and 10 cores per executor).

##**RUN COMMAND**
```
PYTHON_VERSION=3 spark-submit --num-executors 10 --executor-cores 5 file_name.py OUTPUT_PREFIX
```
where `OUTPUT_PREFIX` is a user-supplied argument to specify where to your code should create 9 sub-folders for the output.

##**INPUT DATA (on HDFS)**
You will need the Places as well as the Weekly Pattern data set for this homework. Both have been filtered to the NYC area, and are available on HDFS under the folder `/data/share/bdm/`:
```
hdfs:///data/share/bdm/core-places-nyc.csv
hdfs:///data/share/bdm/weekly-patterns-nyc-2019-2020/*
```

##**OUTPUT DATA (on HDFS)**

Your code must create the following 9 sub-folders (corresponding to 9 categories) under the `OUTPUT_PREFIX` provided in the command line argument:

* `big_box_grocers`
* `convenience_stores`
* `drinking_places`
* `full_service_restaurants`
* `limited_service_restaurants`
* `pharmacies_and_drug_stores`
* `snack_and_bakeries`
* `specialty_food_stores`
* `supermarkets_except_convenience_stores`

Each folder contains the CSV records for each category with the same schema specified above, **sorted by `year` and `date`**. For example, if I run your code with the following command.

```
PYTHON_VERSION=3 spark-submit --num-executors 10 --executor-cores 5 file_name.py test
```

I should have the following 9 folders populated with the expected output:
```
test/big_box_grocers
test/convenience_stores
test/drinking_places
test/full_service_restaurants
test/limited_service_restaurants
test/pharmacies_and_drug_stores
test/snack_and_bakeries
test/specialty_food_stores
test/supermarkets_except_convenience_stores
```

In addition, when I run the following command:
```
hadoop fs -getmerge test/big_box_grocers big_box_grocers.csv
hadoop fs -getmerge test/convenience_stores convenience_stores.csv
...
```

I should get 9 CSV files ready to run with the `linePlot()` defined below. A sample output `fast_food_chains.csv` is provided for your reference. This is the visit pattern for all fast food chains (restaurants associated with a brand) in NYC.

##**TIME LIMITATION: 2 minute**
Using 50 cores (the command line specified above), the code must take **less than 2 minute** to complete.


# File Explaination

* BDM3.ipynb: 
we use Spark to explore [Safegraph data](https://www.safegraph.com/covid-19-data-consortium) to better understand how NYC response to the COVID-19 pandemic. Similarly, we will be looking at the [Core Places](https://docs.safegraph.com/v4.0/docs#section-core-places) and the [Weekly Pattern](https://docs.safegraph.com/v4.0/docs/places-schema#section-patterns) data sets to answer the following two inquiries:


  1.   How many restaurants in NYC were closed right when the city shut down on March 17, 2020, and how many were closed by April 1, 2020?

  2.   For those that were open on/after April 1, 2020 in [1], which ones still received a high volume of visits (in any day on/after April 1)? What were the median dwelling time at each  establishment in the first week of March (3/2-3/9) and in the first week of April (3/30-4/6)?
  
* BDM4.ipynb:
The notebook file that implements the analysis procedure for sampled data, and final visualization result.


* BDM_Final.py: Final working code, time optimized, should be work on the server and get the result <2 min.

* result folder:
.png files contains the final result visualization. 
