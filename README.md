# CZ4031-Project-2 (Group 12)
## Creating database and uploading of data into PostgreSQL
1. PostgreSQL is available for download here: PostgreSQL: Downloads
2. TPC-H dataset can be downloaded from here: TPC Download Current Specs/Source 
Alternatively, the already processed TPC-H dataset into CSV can be downloaded from here from OneDrive: TPC-H dataset
3. Open up Windows command prompt.    
3a. If the PostgreSQL bin is already added to PATH, then enter the command below and your password when prompted.   
```console
psql -U username
```  
3b. Otherwise, in the command prompt change directory to where the PostgreSQL bin is found, then enter the command below and your password when prompted.   
```console
psql -U username
``` 
4. Create tables using postgresql-scripts/create-tpc-h-tables.sql. One can just simply copy and paste the statements into the PostgreSQL command prompt to create the tables. 
5. Import data using postgresql-scripts/import-tpc-h-data.sql. Change directory to where your TPC-H data is found. Suppose your work directory is /home/user/Desktop and the CSVs are stored in /home/user/Desktop/TPC-H then the command to load data into PostgreSQL should look like this
```console
\copy "region" from '/home/user/Desktop/TPC-H/region.csv' DELIMITER ',' CSV;
``` 
Repeat the command to load all data into PostgreSQL. 
6. One can also refer to this documentation to set up the TPC-H database: Setting up TPC-H dataset - VerdictDB Documentation

## Overall Instructions to run the flask web application
1. Clone the repository from Github or download the zip file
2. Create a python virtual environment by running in command prompt the following command
```console
python -m venv .venv
``` 
3. Install dependencies by running the following command in command prompt
```console
pip install -r requirement.txt
```
4. Change the parameters in **database.ini** file to your own postgresql settings. For example, if the name of the database you used is **TPC-H**, and Postgresql is running on **port 5432 (default)**, **hosted locally**, username and password of the user that owns the database are both **postgres** then your database.ini will look as follows.

    [postgresql]    
    host=localhost    
    database=TPC-H   
    user=postgres   
    password=postgres   
    port=5432   

5. Ensure that the postgresql service is running. 
6. Run project.py to use our web application by in command prompt the following command
```console
python project.py
```
5. By default, the local web application will be hosted on http://127.0.0.1:5000/ .
