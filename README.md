# Mekari Recruitment Test - Associate Data Engineer
## Introduction
This project is part of a data engineering test for a Data Engineer position at Mekari. The objective is to analyze employee data and timesheets to calculate the salary per hour per branch on a monthly basis.

The project involves two components:

1. SQL Script: This component uses PostgreSQL and is intended to run daily in full-snapshot mode.
2. Python Script: This component uses Python and PostgreSQL, designed to run daily in incremental mode.
SQL Script
Prerequisites
PostgreSQL database with appropriate permissions.
CSV files for employees and timesheets.
Implementation
Create a PostgreSQL schema and load CSV files into the employees and timesheets tables.
Data Cleaning:
Remove leading and trailing double quotes in the checkin and checkout columns.
Delete rows with missing checkin or checkout data.
Convert checkin and checkout columns from varchar to time.
Assume today's date for employees with no resign_date data.
Convert resign_date from varchar to date.
Create a new table to store the results.
Calculate salary per hour per branch on a monthly basis.
Insert the data from the output table into the new table named salary_per_hour.
The script should be scheduled to run daily in full-snapshot mode.
Python Script
Prerequisites
PostgreSQL database with appropriate permissions.
CSV files for employees and timesheets.
Implementation
Define a database connection and read data from CSV files.
Data Cleaning:
Remove leading and trailing double quotes in the checkin and checkout columns.
Delete rows with missing checkin or checkout data.
Convert checkin and checkout columns from varchar to time.
Assume today's date for employees with no resign_date data.
Convert resign_date from varchar to date.
Load the max timesheet_id from the timesheets table into a table named last_processed_timesheet.
Check if there are any timesheet_id values larger than the last processed timesheet_id.
If new data is found, calculate salary per hour per branch on a monthly basis and append the result to the destination table.
If no new data is found, print "No new data to process."
The script should be scheduled to run daily in incremental mode.
Note
Before running the Python script, ensure that you adjust the database connection details to match your environment.
