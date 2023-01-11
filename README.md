### Summary

This is the data engineering and visualization part of the capstone project that aims to investigate sewer gases in housing estate with collected data. The team collects data using Arduino devices and the data is imorted to a local PC. This github project transform the local data and visualize the result through a data pipeline. 

### Acknowledgement
This is part of the effort of MECH 4429 capstone project.  All the code and the github repository is created and maintained by William Ng.



**Table of Contents**

[TOCM]

[TOC]

# Objective
## objective of the capstone project
This capstone project aims to analysis the source and the change in concentrations of sewer gases wthin the drainage system of residential housing with respective to time and space. Several sensors are place in the opening of various draing pipe to record the concentration of H2S and CH4 for several days. The data is gathered and tranformed into graphs to identify useful insight in the following data project.
##objective of this data project
The sensors record the data(concentration of CH4, H2S, time, humidity and temperature) at various locations at a rate of 1 record/second in TXT format. The data is import to PC local storage as the source of the data pipeline. This github project will create a data pipeline that transform the data to tables in bigquery for plotting useful graphs. 
#Data Pipeline

## Procedures:
1. move the raw dataset from the local storage to GCP storage
2. move the dataset from the GCP storage to bigquery
3. transform the data in Bigquery using SQL 
4. Visualize the data with Google DataStudio/Metabase

## Software used:
**Infrastructure**: Terraform
**Containization**: Docker
**Orchestration**: Airflow
**Data Lake**: Google Cloud Storage
**Data Warehouse**: Bigquery
**Data Visualization**: Google DataStudio, Metabase

#Result
The following link plot the change in concentration of gases of a particular building in 7 days.
 [Click here](https://datastudio.google.com/s/lr6wp8qRx-U "Heading link")
 
Example graphs using metabase
/assets/image/CH4_Temp_1610.png
/assets/image/RO17_10.png


