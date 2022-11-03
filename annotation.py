'''
Types of scans algorithom used, based off the postgresql documentation at
https://www.postgresql.org/docs/9.2/runtime-config-query.html#RUNTIME-CONFIG-QUERY-CONSTANTS

bitmapscan
hashagg
hashjoin
indexscan
indexonlyscan
mergejoin
nestloop
seqscan
sort
tidscan
'''



class htmlStyle:

    #Green and Red
    greenStart = "<span style='color:green'>"
    redStart = "<span style='color:red'>"
    colorEnd = "</span>"

    #Bold and Italic
    boldStart = "<b>"
    boldEnd = "</b>"
    italicStart = "<em>"
    italicEnd = "</em>"

def green(string):
    return htmlStyle.greenStart + string + htmlStyle.colorEnd
def red(string):
    return htmlStyle.redStart + string + htmlStyle.colorEnd
def bold(string):
    return htmlStyle.boldStart + string + htmlStyle.boldEnd
def italic(string):
    return htmlStyle.italicStart + string + htmlStyle.italicEnd

### Annotation ###

##########################################################################
#Using as test for now
def defaultAnno(queryInput):
    result = f"Processing {italic(queryInput['Node Type'])} operation."

    if "indexCond" in queryInput:
        result = result + " Condition found: " + green(queryInput["indexCond"].replace('::text','') + ".") #::text is unlimited char in postgresql

    result = result + f" The resulting record from the table is as such."

    if "filter" in queryInput:
        result = result + f" There is further refinement on the record with these filter {bold(queryInput['filter'].replace('::text',''))}."

    return result
##########################################################################


def bitmapAnno(queryInput):

    return result

def hashaggAnno(queryInput):
    result = f"With {italic(queryInput['Node Type'])} function, the DBMS hashes the query rows into memory, for use by its parent operation."
    
    return result

def hashjoinAnno(queryInput):
    result = f"This operation {italic(queryInput['Node Type'])} joins the results from the previous operations by using a hash {bold(query_plan['Join Type'])} {bold('Join')}"

    if "Hash Join" in queryInput:
        result += f" on the condition: {green(queryInput['Hash Cond'].replace('::text', ''))}"

    result += "."
    return result

def indexAnno(queryInput):
    #Default mentioning type of scan and definition
    result = f"With {italic(queryInput['Node Type'])} operation, the DBMS is scanning the row in specified range of index."

    if "indexCond" in queryInput:
        result = result + " Condition found: " + green(queryInput["indexCond"].replace('::text','') + ".") #::text is unlimited char in postgresql

    result = result + f" The resulting record from the table is as such."

    if "filter" in queryInput:
        result = result + f" There is further refinement on the record with these filter {bold(queryInput['filter'].replace('::text',''))}."

    return result

def indexonlyAnno(queryInput):

    result = f"With {italic(queryInput['Node Type'])} operation, the DBMS is scanning the row using an index table {bold(queryInput['Index Name'])}"

    if "indexCond" in queryInput:
        result += " with condition" + green(
            queryInput["indexCond"].replace("::text", "")
        )

    result += ". The resulting record obtained from the index table is as such."

    # Obtain the filtered attribute and remove unnecessary strings
    if "Filter" in queryInput:
        result += f" The result will then be filtered by {bold(queryInput['Filter'].replace('::text', ''))}."

    return result

def mergejoinAnno(queryInput):

    result = f"The operation {italic(queryInput['Node Type'])} joins the sorted results using join keys from sub-operations"

    if "Merge Join" in queryInput:
        result += " with condition " + green(
            queryInput["Merge Join"].replace("::text", "")
        )

    # Checking join type
    if "Join Type" == "Semi":
        result += " results only returns records from the left relation"

    result += "."

    return result

def nestloopAnno(queryInput):

    result = f"The operation {italic(queryInput['Node Type'])} performs a join or search. For every row the first child produces, the corresponding node will be looked up in the second node."
    return result

def seqscanAnno(queryInput):

    result = f"The operation {italic(queryInput['Node Type'])} operation performs a sequential scan on the relation "

    # Retrieve relation name from query input
    if "Relation Name" in queryInput:
        result += green(queryInput["Relation Name"])

    # Retrieve the alias from query plan if there is an alternative name
    if "Alias" in queryInput:
        if queryInput["Relation Name"] != queryInput["Alias"]:
            result += f" has alias named {red(queryInput['Alias'])}"

    # Obtain the filtered attribute and remove unnecessary strings
    if "Filter" in queryInput:
        result += f" will be filtered with the condition {bold(queryInput['Filter'].replace('::text', ''))}"

    result += "."

    return result

def sortAnno(queryInput):

    result = (
        f"The {italic(queryInput['Node Type'])} operation performs a sort on the rows "
    )

    # If sorting in descending order
    if "DESC" in queryInput["Sort Key"]:
        result += (
            green(str(queryInput["Sort Key"].replace("DESC", "")))
            + " in descending order"
        )

    # If sorting in ascending order
    elif "ASC" in queryInput["Sort Key"]:
        result += (
            red(str(queryInput["Sort Key"].replace("ASC", "")))
            + " in ascending order"
        )

    # Else specify the attribute
    else:
        result += f"based on {bold(str(queryInput['Sort Key']))}"

    result += "."

    return result

def tidAnno(queryInput):

    return result

class Annotation(object):
    algoDict = {
        "bitmap": bitmapAnno,
        "hashagg": hashaggAnno,
        "hashjoin": hashjoinAnno,
        "index": indexAnno,
        "indexonly": indexonlyAnno,
        "mergejoin": mergejoinAnno,
        "nestloop": nestloopAnno,
        "seqscan": seqscanAnno,
        "sort": sortAnno,
        "tid": tidAnno,
    }

if __name__ == "__main__":
    queryInput = {
        "Node Type": "NONE OF THE ABOVE",
        #"indexCond": "do this",
        #"filter": "do that"
        }
    annotation = Annotation().algoDict.get(queryInput["Node Type"], defaultAnno(queryInput))
    print(annotation)
