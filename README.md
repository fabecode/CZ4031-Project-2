# CZ4031-Project-2
## How to use
Change the parameters in **database.ini** file to your own postgresql settings.   
For example, if your database is running on port **5432 (default)**, **hosted locally**, **username** and **password**
are both **postgres** then your **database.ini** will look as follows.

    [postgresql]    
    host=localhost    
    database=postgres   
    user=postgres   
    password=postgres1234   
    port=5432   

Run project.py and to test any query, edit line 6 in project.py.

    database.query("...")

For debugging purpose, QEPs will be saved to a json file for now.