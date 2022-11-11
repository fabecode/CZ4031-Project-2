from flask import Flask, render_template, redirect, request
from preprocessing import Database
import networkx as nx
import matplotlib.pyplot as plt
import random

##################################### Flask App #####################################
class FlaskApp:
    def __init__(self):
        self.app = Flask(__name__)
        self.db = Database()

        @self.app.route('/', methods=["GET"])
        def requestQuery():
            #Currently hardcoded till linked to db
            db_schemas = ['TBC-H', 'Others']
            return render_template('home.html', db_schemas = db_schemas)

        @self.app.route("/queryplan", methods=["POST", "GET"])
        def queryPlan():
            if request.method == "POST":
                query = request.form["queryText"]
                print("Query:", query)
                if self.db.checkValidQuery(query):
                    qep = self.db.query(query)
                    self.db.generateQueryPlan(qep["Plan"])
                    render_args = {
                        "query": query,
                        "annotations": self.db.queryPlanList,
                        "total_cost": qep["Plan"]["Total Cost"],
                        "total_plan_rows": qep["Plan"]["Plan Rows"]
                    }
                    # restore to default
                    self.db.queryPlanList = []
                    self.scanDict = {}
                    self.joinDict = {}
                    return render_template("queryplan.html", **render_args)
            return redirect('/')
           
    def run(self):
        self.app.run()
