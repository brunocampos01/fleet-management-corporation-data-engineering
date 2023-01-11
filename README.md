# Fleet management corporation

# Introduction
As a newly hired data engineer in a fleet management corporation,
you are asked to write an application to detect possible speeding
events in the stream of data coming from the customer's trucks.
Further, the application will be used by the data scientists team.
They are currently working on a classifier that predicts whether
a speeding event is going to happen in the nearest future or not.
Thereby, your application will be used to test the classifier.

# Problem Statement
Your task will be to prepare raw data for further verification of
model/classifier performance.
## Data

You will be working with a tabular data of the following structure:

* `customer_id` - identifier of a customer,
* `vehicle_id` - identifier of a vehicle,
* `driver_id` - identifier of a driver,
* `location_x`, `location_y` - columns with x and y positions respectivelly on a 2D grid (in kilometers)
* `timespan` - datetime in epoch seconds,
* `speed_limit` - speed limit the vehicle should obey in a given moment (in km/h),
* `will_be_speeding` - an output from the classifier, whether a speeding event
  will likely happen in the next `N` further records of the same ride.

The same ride means a chronological sequence of records for a given
`(customer_id, vehicle_id, driver_id)` tuple.

## Tasks

### Task 1: Detecting speeding events

Your task is to implement `detect_speeding_events` function in `app.detector` module.
The function accepts `logs` containing the data as described in the *Data* section
and should output another `DataFrame` with an additional `is_speeding` column.

The value in this column should represent whether speeding has happened between
the current record and the previous one.

The speeding is defined as traveling with the speed higher than the speed limit
defined in the current record.

For the first record in the sequence both `False` and `None` are valid values.

### Task 2: Prepare data for the classifier validation

Your task is to implement a `predict_speeding_event` function in `app.detector` module.
The function accepts `logs_with_speeding` and `prediction_horizon` and should output
another `DataFrame` with an additional `actually_speeding` column. The `logs_with_speeding`
is a `DataFrame` containing speeding events detected in the previous task.

The function should calculate the ride status over a prediction horizon, so the
value in new column represents whether a speeding event is going
to happen in the next `prediction_horizon` steps of the same ride.
