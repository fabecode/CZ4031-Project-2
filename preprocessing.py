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
        with open(f'hash.json', 'w', encoding='utf-8') as f:
            json.dump(self.scanHash, f, ensure_ascii=False, indent=4)
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

    def aqp(self, query, setQuery):
        self.cursor.execute(setQuery)

        self.cursor.execute("EXPLAIN (FORMAT JSON)" + query)
        aqp = self.cursor.fetchall()[0][0][0]
        return aqp


    # def aqp(self, query):
    #     temp = set()
    #     output = list()
    #     for i in self.possible:
    #         setQuery = f"SET {i}=OFF"
    #         showQuery = f"SHOW {i}"
    #         self.cursor.execute(setQuery)
    #         self.cursor.execute(showQuery)
    #         #print(self.cursor.fetchall())
    #
    #         self.cursor.execute("EXPLAIN (FORMAT JSON)" + query)
    #         aqp = self.cursor.fetchall()[0][0][0]
    #         j = json.dumps(aqp)
    #         if j not in temp:
    #             temp.add(j)
    #             output.append((i, aqp))
    #
    #         setQuery = f"SET {i}=ON"
    #         self.cursor.execute(setQuery)
    #         self.cursor.execute(showQuery)
    #         #print(self.cursor.fetchall())
    #
    #     for i in output:
    #         with open(f'{i[0]}.json', 'w', encoding='utf-8') as f:
    #             json.dump(i[1], f, ensure_ascii=False, indent=4)
    #     self.test(query)

    # def test(self, query):
    #     setQuery = f"SET enable_hashjoin=OFF"
    #     self.cursor.execute(setQuery)
    #     setQuery = f"SET enable_indexscan=OFF"
    #     self.cursor.execute(setQuery)
    #     self.cursor.execute("EXPLAIN (FORMAT JSON)" + query)
    #     aqp = self.cursor.fetchall()[0][0][0]
    #
    #     with open(f'enable_hashjoin_enable_indexscan.json', 'w', encoding='utf-8') as f:
    #         json.dump(aqp, f, ensure_ascii=False, indent=4)
    #
    #     setQuery = f"SET enable_hashjoin=ON"
    #     self.cursor.execute(setQuery)
    #     setQuery = f"SET enable_indexscan=ON"
    #     self.cursor.execute(setQuery)

    def scans(self, qep):
        """
        Recursively grab all the scans type nodes in a QEP/AQP which can be used for comparison later
        :param qep:
        :return: None. Results is store in scanHash dictionary
        """
        if qep == {}:
            return

        if "Plans" in qep:
            for i in qep["Plans"]:
                for j in range(len(qep["Plans"])):
                    if "Relation Name" in qep["Plans"][j] and qep["Plans"][j]["Relation Name"] in self.scanHash:
                        if qep["Plans"][j] not in self.scanHash[qep["Plans"][j]["Relation Name"]]:
                            self.scanHash[qep["Plans"][j]["Relation Name"]].append(qep["Plans"][j])
                    elif "Relation Name" in qep["Plans"][j] and qep["Plans"][j]["Relation Name"] not in self.scanHash:
                        self.scanHash[qep["Plans"][j]["Relation Name"]] = [qep["Plans"][j]]

                self.scans(i)


    def closeConnection(self):
        """
        Close connection to database
        :return: None
        """
        self.cursor.close()
        self.conn.close()
