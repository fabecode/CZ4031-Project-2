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

    return result

def hashjoinAnno(queryInput):

    return result

def indexAnno(queryInput):
    #Default mentioning type of scan and definition
    result = f"With {italics(query_plan['Node Type'])} operation, the DBMS is scanning the row in specified range of index."

    if "indexCond" in queryInput:
        result = result + " Condition found: " + green(queryInput["indexCond"].replace('::text','') + ".") #::text is unlimited char in postgresql

    result = result + f" The resulting record from the table is as such."

    if "filter" in queryInput:
        result = result + f" There is further refinement on the record with these filter {bold(queryInput['filter'].replace('::text',''))}."

    return result

def indexonlyAnno(queryInput):

    return result

def mergejoinAnno(queryInput):

    return result

def nestloopAnno(queryInput):

    return result

def seqscanAnno(queryInput):

    return result

def sortAnno(queryInput):

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
