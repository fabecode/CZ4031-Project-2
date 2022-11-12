import psycopg2
import configparser
import json
from annotation import Annotation


class Database:
    def __init__(self):
        """
        Starts database connection
        """
        self.config = configparser.ConfigParser()
        self.config.read('database.ini')
        self.conn = psycopg2.connect(
            host=self.config['postgresql']['host'],
            database=self.config['postgresql']['database'],
            user=self.config['postgresql']['user'],
            password=self.config['postgresql']['password'],
            port=self.config['postgresql']['port'],
            connect_timeout=3,
            keepalives=1,
            keepalives_idle=5,
            keepalives_interval=2,
            keepalives_count=2,
            options='-c statement_timeout=6000'
        )
        self.conn.set_isolation_level(0)
        self.db_name = self.config['postgresql']['database']
        self.cursor = self.conn.cursor()
        self.possible = ["enable_bitmapscan",
                         "enable_hashagg",
                         "enable_hashjoin",
                         "enable_indexscan",
                         "enable_indexonlyscan",
                         "enable_material",
                         "enable_mergejoin",
                         "enable_nestloop",
                         "enable_seqscan",
                         "enable_sort",
                         "enable_tidscan"]
        self.annotation = Annotation()
        self.scanDict = {}
        self.joinDict = {}
        self.altQueryPlans = []
        self.queryPlanList = []

    def printQueryPlan(self):
        print(self.queryPlanList)

    def generateQueryPlan(self, qep):
        """
        Recursively generates query plan
        :param qep: Qeury Execution Plan
        :return: None, annotation saved in self.queryPlanDict
        """
        if qep == {}:
            return

        ##################### SCAN TYPE NODES #####################
        if "Relation Name" in qep and qep["Relation Name"] in self.scanDict:
            output = self.annotation.compareScanAnno(qep, self.scanDict)
            self.queryPlanList.append([qep["Relation Name"].upper() + " table", output])

        ##################### JOIN TYPE NODES #####################
        # merge join type nodes
        elif "Merge Cond" in qep and qep["Merge Cond"] in self.joinDict:
            output = self.annotation.compareJoinAnno(qep, self.joinDict, qep["Merge Cond"])
            self.queryPlanList.append([qep["Merge Cond"], output])

        # hash join type nodes
        elif "Hash Cond" in qep and qep["Hash Cond"] in self.joinDict:
            output = self.annotation.compareJoinAnno(qep, self.joinDict, qep["Hash Cond"])
            self.queryPlanList.append([qep["Hash Cond"], output])
        
        ##################### SORT TYPE NODES #####################
        elif "Sort Key" in qep:
            procedure = self.annotation.annoDict.get(qep["Node Type"], self.annotation.defaultAnno)
            output = procedure(qep)
            self.queryPlanList.append([qep["Sort Key"], output])

        ################## OTHER TYPE OF NODES ###################
        else:
            procedure = self.annotation.annoDict.get(qep["Node Type"], self.annotation.defaultAnno)
            output = procedure(qep)
            self.queryPlanList.append([qep["Node Type"], output])

        ##################### RECURSIVE CALL #####################
        if "Plans" in qep:
            for i in qep["Plans"]:
                self.generateQueryPlan(i)
    

    def checkValidQuery(self, query):
        """
        Tries to execute query and check for vaildity. Returns a single row on valid query. Else returns None
        :param query:
        :return:
        """
        try:
            self.cursor.execute(query)
            results = self.cursor.fetchone()
            return results
        except Exception as e:
            self.cursor.execute("ROLLBACK")
            return None


    def query(self, query):
        """
        Executes query and returns the qep.
        :param query: SQL query to execute
        :return: QEP, scanDict, joinDict
        """

        self.cursor.execute("EXPLAIN (FORMAT JSON)" + query)
        qep = self.cursor.fetchall()[0][0][0]

        self.processPlans(qep["Plan"])
        self.AQPwrapper(query)
        return qep

    def AQPwrapper(self, query):
        """
        Generates all possible combinations of AQPs
        :param query: query to be executed
        :return: None
        """
        temp = set()
        output = list()
        encode = {
            1: "OFF",
            0: "ON"
        }
        # encodes each possible combination as a bitstring, 1==OFF, 0==ON
        # MSB = enable_bitmapscan, LSB = enable_tidscan
        for bitstring in range(2048):
            setQuery = f"SET enable_bitmapscan={encode[(bitstring&1024)>>10]}; " \
                       f"SET enable_hashagg={encode[(bitstring&512)>>9]}; " \
                       f"SET enable_hashjoin={encode[(bitstring&256)>>8]}; " \
                       f"SET enable_indexscan={encode[(bitstring&128)>>7]}; " \
                       f"SET enable_indexonlyscan={encode[(bitstring&64)>>6]}; " \
                       f"SET enable_material={encode[(bitstring&32)>>5]}; " \
                       f"SET enable_mergejoin={encode[(bitstring&16)>>4]}; " \
                       f"SET enable_nestloop={encode[(bitstring&8)>>3]}; " \
                       f"SET enable_seqscan={encode[(bitstring&4)>>2]}; " \
                       f"SET enable_sort={encode[(bitstring&2)>>1]}; " \
                       f"SET enable_tidscan={encode[bitstring&1]};"

            aqp = self.aqp(query, setQuery)
            t = json.dumps(aqp)
            if t not in temp:
                if bitstring <= 6:
                    self.altQueryPlans.append(aqp)
                temp.add(t)
                output.append(aqp)

        self.resetState()
        for i in output:
            self.processPlans(i["Plan"])

    def aqp(self, query, setQuery):
        """
        Executes query and returns the aqp.
        :param query: query to be executed
        :param setQuery: Combination of planner method configuration
        :return: aqp
        """
        self.cursor.execute(setQuery)

        self.cursor.execute("EXPLAIN (FORMAT JSON)" + query)
        aqp = self.cursor.fetchall()[0][0][0]
        return aqp

    def processPlans(self, qep):
        """
        Recursively grab all the scans, merge join and hash join type nodes in a QEP/AQP which can be used for comparison later.
        :param qep: generated qep
        :return: None.
        """
        if qep == {}:
            return

        #################### SCAN TYPE NODES ####################
        # grabbing scan type nodes
        if "Relation Name" in qep and qep["Relation Name"] in self.scanDict:
            if qep not in self.scanDict[qep["Relation Name"]]:
                self.scanDict[qep["Relation Name"]].append(qep)
        elif "Relation Name" in qep and qep["Relation Name"] not in self.scanDict:
            self.scanDict[qep["Relation Name"]] = [qep]

        #################### JOIN TYPE NODES ####################
        # for join types, minus off the total cost from the left and right child.

        # grabbing merge join type nodes
        if "Node Type" in qep and qep["Node Type"] == "Merge Join":
            temp = qep.copy()
            temp["Total Cost"] -= (temp["Plans"][0]["Total Cost"] + temp["Plans"][1]["Total Cost"])
            temp.pop("Plans")
            if temp["Merge Cond"] in self.joinDict:
                if temp not in self.joinDict[temp["Merge Cond"]]:
                    self.joinDict[temp["Merge Cond"]].append(temp)
            else:
                self.joinDict[temp["Merge Cond"]] = [temp]

        # grabbing hash join type nodes
        if "Node Type" in qep and qep["Node Type"] == "Hash Join":
            temp = qep.copy()
            temp["Total Cost"] -= (temp["Plans"][0]["Total Cost"] + temp["Plans"][1]["Total Cost"])
            temp.pop("Plans")
            if temp["Hash Cond"] in self.joinDict:
                if temp not in self.joinDict[temp["Hash Cond"]]:
                    self.joinDict[temp["Hash Cond"]].append(temp)
            else:
                self.joinDict[temp["Hash Cond"]] = [temp]

    
        #################### RECURSIVE CALL ####################
        if "Plans" in qep:
            for i in qep["Plans"]:
                self.processPlans(i)

    def retrieveAllDbs(self):
        self.cursor.execute("SELECT datname FROM pg_database")
        dbs = self.cursor.fetchall()
        db_list = []
        for db in dbs:
            db_list.append(db[0])

        return db_list
        
    def closeConnection(self):
        """
        Close connection to database
        :return: None
        """
        self.cursor.close()
        self.conn.close()

    def resetState(self):
        """
        Resets the state of the database planner method configuration.
        :return: None
        """
        # restore to default (all ON)
        setQuery = f"SET enable_bitmapscan=ON; " \
                   f"SET enable_hashagg=ON; " \
                   f"SET enable_hashjoin=ON; " \
                   f"SET enable_indexscan=ON; " \
                   f"SET enable_indexonlyscan=ON; " \
                   f"SET enable_material=ON; " \
                   f"SET enable_mergejoin=ON; " \
                   f"SET enable_nestloop=ON; " \
                   f"SET enable_seqscan=ON; " \
                   f"SET enable_sort=ON; " \
                   f"SET enable_tidscan=ON;"
        self.cursor.execute(setQuery)
