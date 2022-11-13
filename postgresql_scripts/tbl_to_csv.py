import os
import pandas as pd

for file in os.listdir("."):
    output = []
    print(file)
    if file.endswith(".tbl"):
        with open(file, "r") as f:
            print("read file: {}".format(file))
            temp = f.readlines()
        
        for i in temp:
            new = "".join(i.rsplit("|", 1))
            output.append(new)
        
        df = pd.DataFrame([j.split("|") for j in output])
        df.to_csv("../csv_data/"+file.split(".")[0]+".csv",index=False, header=False)
