## Week 1 Homework

In this homework we'll prepare the environment 
and practice with terraform and SQL


## Question 1. Google Cloud SDK

Install Google Cloud SDK. What's the version you have? 

To get the version, run `gcloud --version`

## Google Cloud account 

Create an account in Google Cloud and create a project.


## Question 2. Terraform 

Now install terraform and go to the terraform directory (`week_1_basics_n_setup/1_terraform_gcp/terraform`)

After that, run

* `terraform init`
* `terraform plan`
* `terraform apply` 

Apply the plan and copy the output (after running `apply`) to the form.

It should be the entire output - from the moment you typed `terraform init` to the very end.

## Prepare Postgres 

Run Postgres and load data as shown in the videos

We'll use the yellow taxi trips from January 2021:

```bash
wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2021-01.csv
```

You will also need the dataset with zones:

```bash 
wget https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv
```

Download this data and put it to Postgres

## Question 3. Count records 

How many taxi trips were there on January 15? 

Consider only trips that started on January 15.

54024

select count(1)
FROM yellow_taxi_data
WHERE DATE(tpep_pickup_datetime) = '2021-01-15'
limit 10

## Question 4. Largest tip for each day

Find the largest tip for each day. 
On which day it was the largest tip in January?

Use the pick up time for your calculations.

(note: it's not a typo, it's "tip", not "trip")

2021-01-20 => 1140.44

select MAX(tip_amount) as max_tip, DATE(tpep_pickup_datetime) as pickup_day
FROM yellow_taxi_data
GROUP BY DATE(tpep_pickup_datetime)
ORDER BY max_tip DESC
limit 1

## Question 5. Most popular destination

What was the most popular destination for passengers picked up 
in central park on January 14?

Use the pick up time for your calculations.

Enter the zone name (not id). If the zone name is unknown (missing), write "Unknown" 

Upper East Side North = 2234

SELECT count(zdo."Zone") as dropoff_cnt, zdo."Zone"
FROM 
	yellow_taxi_data t,
	zones zpu,
	zones zdo
WHERE
	t."PULocationID" = zpu."LocationID"
	AND
	t."DOLocationID" = zdo."LocationID"
	AND
	zpu."Zone" = 'Central Park'
GROUP BY zdo."Zone"
ORDER BY dropoff_cnt DESC

## Question 6. Most expensive locations

What's the pickup-dropoff pair with the largest 
average price for a ride (calculated based on `total_amount`)?

Enter two zone names separated by a slash

For example:

"Jamaica Bay / Clinton East"

If any of the zone names are unknown (missing), write "Unknown". For example, "Unknown / Clinton East". 

Alphabet City/ unknown = 2292.4

SELECT AVG(total_amount) as money, CONCAT(zpu."Zone", '/ ', zdo."Zone") as pairs
FROM 
yellow_taxi_data t JOIN zones zpu 
	ON t."PULocationID" = zpu."LocationID" 
	JOIN zones zdo
	ON t."DOLocationID" = zdo."LocationID"
GROUP BY pairs
ORDER BY money DESC
limit 10

## Submitting the solutions

* Form for submitting: https://forms.gle/yGQrkgRdVbiFs8Vd7
* You can submit your homework multiple times. In this case, only the last submission will be used. 

Deadline: 26 January (Wednesday), 22:00 CET


## Solution

Here is the solution to questions 3-6: [video](https://www.youtube.com/watch?v=HxHqH2ARfxM&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb)

