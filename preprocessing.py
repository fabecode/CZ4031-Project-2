import psycopg2
import configparser
import json
from annotation import *


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
            port=self.config['postgresql']['port']
        )
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

        self.scanHash = {}
        self.joinHash = {}
        self.comparison = {}

    def print(self):
        print(self.comparison)

    def compare(self, qep):
        """
        Recursively compares the qep with all other aqp
        :param qep: Qeury Execution Plan
        :return: None, annotation saved in self.comparison
        """
        if qep == {}:
            return

        # grabbing scan type nodes
        if "Relation Name" in qep and qep["Relation Name"] in self.scanHash:
            key = qep["Relation Name"]
            possible = self.scanHash[key]
            seen = {}
            for j in possible:
                if j["Node Type"] not in seen:
                    seen[j["Node Type"]] = j["Total Cost"]
                elif j["Node Type"] in seen and seen[j["Node Type"]] > j["Total Cost"]:
                    seen[j["Node Type"]] = j["Total Cost"]
            seen[qep["Node Type"]] = qep["Total Cost"]
            output = ""
            output += f"{qep['Node Type']} done on {qep['Relation Name']} with a cost of {qep['Total Cost']}. "
            for key, value in seen.items():
                if key != qep["Node Type"]:
                    output += f"{qep['Node Type']} is chosen as choosing {key} costs {(value/seen[qep['Node Type']]):.3f} more with a cost of {value}. "
            self.comparison[qep["Relation Name"]] = output


        # grabbing merge join type nodes
        if "Node Type" in qep and qep["Node Type"] == "Merge Join":
            key = qep["Merge Cond"]
            possible = self.joinHash[key]
            for j in possible:
                cost = j["Total Cost"]

        # grabbing hash join type nodes
        if "Node Type" in qep and qep["Node Type"] == "Hash Join":
            key = qep["Hash Cond"]
            possible = self.joinHash[key]
            for j in possible:
                cost = j["Total Cost"]

        if "Plans" in qep:
            for i in qep["Plans"]:
                self.compare(i)

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
            return None


    def query(self, query):
        """
        Executes query and returns the qep. For debugging purpose will store to json for now.
        :param query: SQL query to execute
        :return: QEP
        """

        self.cursor.execute("EXPLAIN (FORMAT JSON)" + query)
        qep = self.cursor.fetchall()[0][0][0]
        #with open('data.json', 'w', encoding='utf-8') as f:
        #    json.dump(qep, f, ensure_ascii=False, indent=4)

        self.scans(qep["Plan"])
        self.AQPwrapper(query)
        with open(f'scanhash.json', 'w', encoding='utf-8') as f:
            json.dump(self.scanHash, f, ensure_ascii=False, indent=4)
        with open(f'joinhash.json', 'w', encoding='utf-8') as f:
            json.dump(self.joinHash, f, ensure_ascii=False, indent=4)
        return qep

    def AQPwrapper(self, query):
        """
        Generates all possible combinations of AQPs
        :param query:
        :return:
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
                temp.add(t)
                output.append(aqp)

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
        print(len(temp))
        for i in output:
            self.scans(i["Plan"])

    def aqp(self, query, setQuery):
        self.cursor.execute(setQuery)

        self.cursor.execute("EXPLAIN (FORMAT JSON)" + query)
        aqp = self.cursor.fetchall()[0][0][0]
        return aqp

    def scans(self, qep):
        """
        Recursively grab all the scans type nodes in a QEP/AQP which can be used for comparison later
        :param qep:
        :return: None. Results is store in scanHash dictionary
        """
        if qep == {}:
            return

        # grabbing scan type nodes
        if "Relation Name" in qep and qep["Relation Name"] in self.scanHash:
            if qep not in self.scanHash[qep["Relation Name"]]:
                self.scanHash[qep["Relation Name"]].append(qep)
        elif "Relation Name" in qep and qep["Relation Name"] not in self.scanHash:
            self.scanHash[qep["Relation Name"]] = [qep]

        # grabbing merge join type nodes
        if "Node Type" in qep and qep["Node Type"] == "Merge Join":
            temp = qep.copy()
            temp.pop("Plans")
            if temp["Merge Cond"] in self.joinHash:
                if temp not in self.joinHash[temp["Merge Cond"]]:
                    self.joinHash[temp["Merge Cond"]].append(temp)
            else:
                self.joinHash[temp["Merge Cond"]] = [temp]

        # grabbing hash join type nodes
        if "Node Type" in qep and qep["Node Type"] == "Hash Join":
            temp = qep.copy()
            temp.pop("Plans")
            if temp["Hash Cond"] in self.joinHash:
                if temp not in self.joinHash[temp["Hash Cond"]]:
                    self.joinHash[temp["Hash Cond"]].append(temp)
            else:
                self.joinHash[temp["Hash Cond"]] = [temp]

        if "Plans" in qep:
            for i in qep["Plans"]:
                self.scans(i)

    def closeConnection(self):
        """
        Close connection to database
        :return: None
        """
        self.cursor.close()
        self.conn.close()


if __name__=='__main__':
    db = Database()
    #query = "select s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment from PART, SUPPLIER, PARTSUPP, NATION, REGION where p_partkey = ps_partkey and s_suppkey = ps_suppkey and p_size = 30 and p_type like '%STEEL' and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'ASIA' and ps_supplycost = (select min(ps_supplycost) from PARTSUPP, SUPPLIER, NATION, REGION where p_partkey = ps_partkey and s_suppkey = ps_suppkey and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'ASIA') order by s_acctbal desc, n_name, s_name, p_partkey limit 100;"
    query = 123
    if db.checkValidQuery(query) != None:
        qep = db.query(query)
        print(qep["Plan"])
        db.compare(qep["Plan"])
        db.print()
    else:
        pass
    db.closeConnection()
