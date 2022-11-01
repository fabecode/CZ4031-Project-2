from flask import Flask, render_template, redirect, request

class FlaskApp:
    def __init__(self):
        self.app = Flask(__name__)

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
                if self.checkValidQuery(query):
                    #Currently hardcoded till linked to db
                    render_args = {
                        "query": query,
                        "annotations": ["Testing explanation 1", "Testing explanation 2"],
                        "total_cost": 100,
                        "total_plan_rows": 100,
                        "total_seq_scan": 100,
                        "total_index_scan": 100
                    }
                    return render_template("queryplan.html", **render_args)
            return redirect('/')
           
    def run(self):
        self.app.run()
    
    def checkValidQuery(self, query):
        if len(query) == 0:
            return False
        else:     
        # self.cursor.execute(query)
        # try:
        #     self.cursor.fetchone()
        # except:
        #     return False
            return True