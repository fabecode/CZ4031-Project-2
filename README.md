# CZ4031-Project-2 (Group 12)
## Creating database and uploading of data into PostgreSQL
1. PostgreSQL is available for download here: [PostgreSQL: Downloads](https://www.postgresql.org/download/) 
2. TPC-H dataset can be downloaded from here: [TPC Download Current Specs/Source](https://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp). Alternatively, the already processed TPC-H dataset into CSV can be downloaded from OneDrive: [TPC-H dataset](https://entuedu-my.sharepoint.com/personal/royl0003_e_ntu_edu_sg/_layouts/15/onedrive.aspx?id=%2Fpersonal%2Froyl0003%5Fe%5Fntu%5Fedu%5Fsg%2FDocuments%2FTPC%2DH%20dataset&ga=1)
3. Open up Windows command prompt.  
    a. If the PostgreSQL bin is already added to PATH, then enter the command below and your password when prompted.   
    ```console
    psql -U username
    ```    
    b. Otherwise, in the command prompt change directory to where the PostgreSQL bin is found, then enter the command below and your password when prompted.   
    ```console
    psql -U username
    ``` 
4. Create tables using [postgresql-scripts/create-tpc-h-tables.sql](https://github.com/fabecode/CZ4031-Project-2/blob/main/postgresql_scripts/create-tpc-h-tables.sql). One can just simply copy and paste the statements into the PostgreSQL command prompt to create the tables. 
5. Import data using [postgresql-scripts/import-tpc-h-data.sql](https://github.com/fabecode/CZ4031-Project-2/blob/main/postgresql_scripts/import-tpc-h-data.sql). Change directory to where your TPC-H data is found. Suppose your work directory is /home/user/Desktop and the CSVs are stored in /home/user/Desktop/TPC-H then the command to load data into PostgreSQL should look like this
    ```console
    \copy "region" from '/home/user/Desktop/TPC-H/region.csv' DELIMITER ',' CSV;
    ``` 
    Repeat the command to load all data into PostgreSQL. 
6. One can also refer to this documentation to set up the TPC-H database: Setting up [TPC-H dataset - VerdictDB Documentation](https://docs.verdictdb.org/tutorial/tpch/#postgresql)

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
4. Change the parameters in **database.ini** file to your own PostgreSQL settings. For example, if the name of the database you used is **TPC-H**, and PostgreSQL is running on **port 5432 (default)**, **hosted locally**, **username and password** of the user that owns the database are both **postgres** then your database.ini will look as follows.
    ```
    [postgresql]    
    host=localhost    
    database=TPC-H   
    user=postgres   
    password=postgres   
    port=5432   
    ```
5. Ensure that the PostgreSQL service is running. 
6. Run project.py to use our web application by in command prompt the following command
    ```console
    python project.py
    ```
5. By default, the local web application will be hosted on http://127.0.0.1:5000/ .
