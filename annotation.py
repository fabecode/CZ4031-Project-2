'''
Postgresql documentation:
https://www.postgresql.org/docs/9.2/runtime-config-query.html#RUNTIME-CONFIG-QUERY-CONSTANTS

TODO: Add annotations for Aggregate, Limit, Gather, Gather Merge
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

class Annotation:
    """
    Annotations for query plan
    """
    def __init__(self):
        self.annoDict = {
        "Bitmap Heap Scan": self.bitmapAnno,
        "Hash Agg": self.hashaggAnno,
        "Hash Join": self.hashjoinAnno,
        "Index Scan": self.indexAnno,
        "Index Only Scan": self.indexonlyAnno,
        "Merge Join": self.mergejoinAnno,
        "Nested Loop": self.nestloopAnno,
        "Seq Scan": self.seqscanAnno,
        "Sort": self.sortAnno,
        "Tid Scan": self.tidAnno,
        }

    ########################### HIGH LEVEL ANNOTATIONS ############################
    def compareScanAnno(self, qep, scanDict):
        """
        Scan annotations and compares QEP will all other relevant AQPs
        :param qep: Query Execution Plan
        :param scanDict: Dictionary of Alternative Scans in AQPs
        :return: annotation string
        """
        key = qep["Relation Name"]
        possible = scanDict[key]
        seen = {}
        for j in possible:
            if j["Node Type"] not in seen:
                seen[j["Node Type"]] = j["Total Cost"]
            elif j["Node Type"] in seen and seen[j["Node Type"]] > j["Total Cost"]:
                seen[j["Node Type"]] = j["Total Cost"]
        seen[qep["Node Type"]] = qep["Total Cost"]

        result = ""
        result += f"{qep['Node Type']} done on {qep['Relation Name']} with a cost of {qep['Total Cost']}. "
        for key, value in seen.items():
            if key != qep["Node Type"]:
                result += f"{qep['Node Type']} is chosen as choosing {key} costs {(value/seen[qep['Node Type']]):.3f} times more with a cost of {value}. "
        if qep["Node Type"] == "Index Scan":
            result += self.indexAnno(qep)
        elif qep["Node Type"] == "Index Only Scan":
            result += self.indexonlyAnno(qep)
        elif qep["Node Type"] == "Seq Scan":
            result += self.seqscanAnno(qep)
        elif qep["Node Type"] == "Bitmap Heap Scan":
            result += self.bitmapAnno(qep)
        return result

    def compareJoinAnno(self, qep, joinDict, joinCond):
        """
        Join annotations and compares QEP will all other relevant AQPs
        :param qep: Query Execution Plan
        :param scanDict: Dictionary of Alternative Scans in AQPs
        :param joinCond: Join condition
        :return: annotation string
        """
        possible = joinDict[joinCond]
        seen = {}
        for j in possible:
            if j["Node Type"] not in seen:
                seen[j["Node Type"]] = j["Total Cost"]
            elif j["Node Type"] in seen and seen[j["Node Type"]] > j["Total Cost"]:
                seen[j["Node Type"]] = j["Total Cost"]
        seen[qep["Node Type"]] = qep["Total Cost"]

        result = ""
        result += f"{qep['Node Type']} done on {joinCond} with a cost of {qep['Total Cost']}. "
        for key, value in seen.items():
            if key != qep["Node Type"]:
                result += f"{qep['Node Type']} is chosen as choosing {key} costs {(value/seen[qep['Node Type']]):.3f} times more with a cost of {value}. "
        
        if qep["Node Type"] == "Hash Join":
            result += self.hashjoinAnno(qep)
        elif qep["Node Type"] == "Merge Join":
            result += self.mergejoinAnno(qep)

        return result

    ########################### SPECIFIC ANNOTATIONS ############################
    def defaultAnno(self, qep):
        result = f"The {italic(qep['Node Type'])} operation is executed."

        if "indexCond" in qep:
            result = result + " Condition found: " + green(qep["indexCond"].replace('::text','') + ".") #::text is unlimited char in postgresql

        if "filter" in qep:
            result = result + f" There is further refinement on the record with these filter {bold(qep['filter'].replace('::text',''))}."

        return result

    def bitmapAnno(self, qep):
        result = ""
        return result

    def hashaggAnno(self, qep):
        result = f"With {italic(qep['Node Type'])} function, the DBMS hashes the query rows into memory, for use by its parent operation."
        
        return result

    def hashjoinAnno(self, qep):
        result = f"The {italic(qep['Node Type'])} joins the results from the previous operations by using an {bold(qep['Join Type'])} {bold('Join')}"

        if "Hash Join" in qep:
            result += f" on the condition: {green(qep['Hash Cond'].replace('::text', ''))}"

        result += "."
        return result

    def indexAnno(self, qep):
        #Default mentioning type of scan and definition
        result = f"With {italic(qep['Node Type'])} operation, the DBMS is scanning the row in specified range of index."

        if "indexCond" in qep:
            result = result + " Condition found: " + green(qep["indexCond"].replace('::text','') + ".") #::text is unlimited char in postgresql

        if "filter" in qep:
            result = result + f" There is further refinement on the record with these filter {bold(qep['filter'].replace('::text',''))}."

        return result

    def indexonlyAnno(self, qep):

        result = f"With {italic(qep['Node Type'])} operation, the DBMS is scanning the row using an index table {bold(qep['Index Name'])}"

        if "indexCond" in qep:
            result += " with condition" + green(
                qep["indexCond"].replace("::text", "")
            )

        # Obtain the filtered attribute and remove unnecessary strings
        if "Filter" in qep:
            result += f" The result will then be filtered by {bold(qep['Filter'].replace('::text', ''))}."

        return result

    def mergejoinAnno(self, qep):

        result = f"The operation {italic(qep['Node Type'])} joins the sorted results using join keys from sub-operations"

        if "Merge Join" in qep:
            result += " with condition " + green(
                qep["Merge Join"].replace("::text", "")
            )

        # Checking join type
        if "Join Type" == "Semi":
            result += " results only returns records from the left relation"

        result += "."

        return result

    def nestloopAnno(self, qep):

        result = f"The operation {italic(qep['Node Type'])} performs a join or search. For every row the first child produces, the corresponding node will be looked up in the second node."
        return result

    def seqscanAnno(self, qep):

        result = f"The operation {italic(qep['Node Type'])} operation performs a sequential scan on the relation "

        # Retrieve relation name from query input
        if "Relation Name" in qep:
            result += green(qep["Relation Name"])

        # Retrieve the alias from query plan if there is an alternative name
        if "Alias" in qep:
            if qep["Relation Name"] != qep["Alias"]:
                result += f" has alias named {red(qep['Alias'])}"

        # Obtain the filtered attribute and remove unnecessary strings
        if "Filter" in qep:
            result += f" will be filtered with the condition {bold(qep['Filter'].replace('::text', ''))}"

        result += "."

        return result

    def sortAnno(self, qep):

        result = (
            f"The {italic(qep['Node Type'])} operation performs a sort on the rows "
        )

        # If sorting in descending order
        if "DESC" in qep["Sort Key"]:
            result += (
                green(str(qep["Sort Key"].replace("DESC", "")))
                + " in descending order"
            )

        # If sorting in ascending order
        elif "ASC" in qep["Sort Key"]:
            result += (
                red(str(qep["Sort Key"].replace("ASC", "")))
                + " in ascending order"
            )

        # Else specify the attribute
        else:
            result += f"based on {bold(str(qep['Sort Key']))}"

        result += "."

        return result

    def tidAnno(self, qep):
        result = ""
        return result

