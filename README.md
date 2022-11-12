# CZ4031-Project-2 (Group 12)
## Instructions
1. Create a python virtual environment 
2. Install dependencies by running the following command in command prompt
```console
pip install -r requirement.txt
```

3. Change the parameters in **database.ini** file to your own postgresql settings.   
For example, if your database is running on port **5432 (default)**, **hosted locally**, **username** and **password**
are both **postgres** then your **database.ini** will look as follows.

    [postgresql]    
    host=localhost    
    database=TPC-H   
    user=postgres   
    password=postgres   
    port=5432   

4. Run project.py to use our web application
```console
python3 project.py
```
5. By default, the project will be host on port 5000. (http://127.0.0.1:5000)

For debugging purpose, QEPs will be saved to a json file for now.