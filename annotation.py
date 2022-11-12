class htmlStyle:
    """
    HTML Styling for Query Plan Annotations
    """
    #Blue and Red
    blueStart = "<span style='color:#3698B8'>"
    redStart = "<span style='color:red'>"
    colorEnd = "</span>"

    #Bold and Italic
    boldStart = "<b>"
    boldEnd = "</b>"
    italicStart = "<em>"
    italicEnd = "</em>"

def blue(string):
    return htmlStyle.blueStart + string + htmlStyle.colorEnd
def red(string):
    return htmlStyle.redStart + string + htmlStyle.colorEnd
def bold(string):
    return htmlStyle.boldStart + string + htmlStyle.boldEnd
def italic(string):
    return htmlStyle.italicStart + string + htmlStyle.italicEnd
def blueItalicBold(string):
    return blue(italic(bold(string)))

class Annotation:
    """
    Annotations for query plan
    """
    def __init__(self):
        self.annoDict = {
        "Bitmap Heap Scan": self.bitmapAnno,
        "Hash Agg": self.hashaggAnno,
        "Hash Join": self.hashjoinAnno,
        "Hash": self.hashAnno,
        "Index Scan": self.indexAnno,
        "Index Only Scan": self.indexonlyAnno,
        "Merge Join": self.mergejoinAnno,
        "Nested Loop": self.nestloopAnno,
        "Seq Scan": self.seqscanAnno,
        "Sort": self.sortAnno,
        "Gather Merge": self.gathermergeAnno,
        "Gather": self.gatherAnno,
        "Limit": self.limitAnno,
        "Aggregate": self.aggregateAnno
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
        result += f"{blueItalicBold(qep['Node Type'])} is done on {blue(qep['Relation Name'])} with a cost of {bold(str(qep['Total Cost']))}. "
        for key, value in seen.items():
            if key != qep["Node Type"]:
                result += f"{blueItalicBold(qep['Node Type'])} is chosen as as it is more efficient than {italic(key)}, which costs {value/seen[qep['Node Type']]:.3f} times more with a cost of {value} in the AQP. "
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
        seen[qep["Node Type"]] = qep["Total Cost"] - qep["Plans"][0]["Total Cost"] - qep["Plans"][1]["Total Cost"]

        joinCost = qep['Total Cost'] - qep["Plans"][0]["Total Cost"] - qep["Plans"][1]["Total Cost"]
        joinCost = round(joinCost, 3)
        result = ""
        result += f"{blueItalicBold(qep['Node Type'])} is done on {bold(joinCond)} with a cost of {bold(str(joinCost))}. "
        for key, value in seen.items():
            if key != qep["Node Type"]:
                result += f"{blueItalicBold(qep['Node Type'])} is chosen as it is more efficient than {italic(key)}, which costs {value/seen[qep['Node Type']]:.3f} times more with a cost of {value:.3f} in the AQP. "
        
        if qep["Node Type"] == "Hash Join":
            result += self.hashjoinAnno(qep)
        elif qep["Node Type"] == "Merge Join":
            result += self.mergejoinAnno(qep)

        return result

    ########################### SPECIFIC ANNOTATIONS ############################
    def defaultAnno(self, qep):
        result = f"The {blueItalicBold(qep['Node Type'])} operation is executed."

        if "indexCond" in qep:
            result = result + " Condition found: " + blue(qep["indexCond"].replace('::text','') + ".") #::text is unlimited char in postgresql

        if "filter" in qep:
            result = result + f" There is further refinement on the record with these filter {bold(qep['filter'].replace('::text',''))}."

        return result

    def bitmapAnno(self, qep):
        result = "The Bitmap Heap Scan takes a row location bitmap generated by a Bitmap Index Scan and looks up the relevant data."
        return result

    def hashaggAnno(self, qep):
        result = f"With {blueItalicBold(qep['Node Type'])} function, the DBMS hashes the query rows into memory, for use by its parent operation."
        
        return result

    def hashjoinAnno(self, qep):
        result = f"The {blueItalicBold(qep['Node Type'])} joins the results from the previous operations by using an {bold(qep['Join Type'])} {bold('Join')}"

        if "Hash Join" in qep:
            result += f" on the condition: {blue(qep['Hash Cond'].replace('::text', ''))}"

        result += "."
        return result

    def hashAnno(self, qep):
        result = f"The {blueItalicBold(qep['Node Type'])} operation hashes the query rows into memory, for use by its parent operation."
        return result

    def indexAnno(self, qep):
        #Default mentioning type of scan and definition
        result = f"With {blueItalicBold(qep['Node Type'])} operation, the DBMS is scanning the row in specified range of index."

        if "indexCond" in qep:
            result = result + " Condition found: " + blue(qep["indexCond"].replace('::text','') + ".") #::text is unlimited char in postgresql

        if "filter" in qep:
            result = result + f" There is further refinement on the record with these filter {bold(qep['filter'].replace('::text',''))}."

        return result

    def indexonlyAnno(self, qep):

        result = f"With {blueItalicBold(qep['Node Type'])} operation, the DBMS is scanning the row using an index table {bold(qep['Index Name'])}"

        if "indexCond" in qep:
            result += " with condition" + blue(
                qep["indexCond"].replace("::text", "")
            )

        # Obtain the filtered attribute and remove unnecessary strings
        if "Filter" in qep:
            result += f" The result will then be filtered by {bold(qep['Filter'].replace('::text', ''))}."

        return result

    def mergejoinAnno(self, qep):

        result = f"The operation {blueItalicBold(qep['Node Type'])} joins the sorted results using join keys from sub-operations"

        if "Merge Join" in qep:
            result += " with condition " + blue(
                qep["Merge Join"].replace("::text", "")
            )

        # Checking join type
        if "Join Type" == "Semi":
            result += " results only returns records from the left relation"

        result += "."

        return result

    def nestloopAnno(self, qep):

        result = f"The operation {blueItalicBold(qep['Node Type'])} performs a join or search. For every row the first child produces, the corresponding node will be looked up in the second node."
        return result

    def seqscanAnno(self, qep):

        result = f"The operation {blueItalicBold(qep['Node Type'])} operation performs a sequential scan on the relation "

        # Retrieve relation name from query input
        if "Relation Name" in qep:
            result += blue(qep["Relation Name"])

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
            f"The {blueItalicBold(qep['Node Type'])} operation performs a sort on the rows "
        )

        # If sorting in descending order
        if "DESC" in qep["Sort Key"]:
            result += (
                blue(str(qep["Sort Key"].replace("DESC", "")))
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
            result += f"based on {bold(str(qep['Sort Key']))} with a cost of {bold(str(qep['Total Cost']))}"

        result += "."

        return result

    def gathermergeAnno(self, qep):

        result = f"The {blueItalicBold(qep['Node Type'])} operation indicates that each process executing the parallel portion of the plan is producing tuples in sorted order, and the leader is performing an order-preserving merge."

        return result

    def gatherAnno(self, qep):

        result = f"The {blueItalicBold(qep['Node Type'])} operation reads tuples from the background workers processes in whatever order is convenient, destroying any sort order that may have existed."

        return result

    def limitAnno(self, qep):

        result = f"With the {blueItalicBold(qep['Node Type'])} operation, the DBMS takes only {bold(str(qep['Plan Rows']))} records and disregards the rest."

        return result

    def aggregateAnno(self, qep):
        result = f"The {blueItalicBold(qep['Node Type'])} operation is used to perform aggregate operations on single results from multiple input rows."

        if "Group Key" in qep:
            result += f" where the tuples are aggregated by "

            for key in qep["Group Key"]:
                result += bold(key) + ","
            
            result = result[:-1] + "group keys."

        return result