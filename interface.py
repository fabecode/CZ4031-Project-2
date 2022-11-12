from flask import Flask, render_template, redirect, request
from preprocessing import Database
import networkx as nx
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import random
import os
import time
from annotation import *
import sqlparse
import math

##################################### Flask App #####################################
class FlaskApp:
    def __init__(self):
        self.app = Flask(__name__)
        self.db = Database()

        @self.app.route('/', methods=["GET"])
        def requestQuery():
            #Currently hardcoded till linked to db
            db_schemas = self.db.retrieveAllDbs()
            return render_template('home.html', db_schemas = db_schemas)

        @self.app.route("/queryplan", methods=["POST", "GET"])
        def queryPlan():
            if request.method == "POST":
                query = request.form["queryText"]
                if self.db.checkValidQuery(query):
                    qep = self.db.query(query)
                    self.db.generateQueryPlan(qep["Plan"])
                    
                    qp = QueryPlan(qep["Plan"])
                    # self.db.altQueryPlans
                    graphfile = qp.save_graph_file()
                    render_args = {
                        "query": sqlparse.format(query, reindent=True, keyword_case='upper'),
                        "annotations": self.db.queryPlanList,
                        "total_cost": qep["Plan"]["Total Cost"],
                        "total_operations": qp.get_num_nodes(),
                        "qep_graph": graphfile
                    }
                    # restore to default
                    self.db.queryPlanList = []
                    self.db.scanDict = {}
                    self.db.joinDict = {}
                    return render_template("queryplan.html", **render_args)
            return redirect('/')
           
    def run(self):
        self.app.run()

##################################### Node & Graph #####################################

class Node:
    def __init__(self, node_type, cost):
        """Initialises a node with its type and total cost

        Args:
            node_type (str):  Node type of itself
            cost (str): Total cost until this node
        """
        self.node_type = node_type
        self.total_cost = cost

    def __str__(self):
        """Overrides the __str__ method to represent the class objects as a string.

        Returns:
            str: String representation of Node.
        """
        return f"{self.node_type}\ncost: {self.total_cost}"


class QueryPlan:
    def __init__(self, query):
        """Initialises the root node with the root query plan.
        Constructs the graph and calculate attributes of the QEP:
        1. Total cost
        2. Plan rows
        3. Number of sequential scan nodes
        4. Number of index scan nodes
        4. Explanation of the query plan

        Args:
            query (dict): Query plan that is generated by PostgreSQL
        """
        self.graph = nx.DiGraph()
        self.root = Node(query["Node Type"],query["Total Cost"])
        self.create_graph_node(self.root,query)
        self.tree_depth = self.calculate_graph_depth(query)
        self.tree_width = self.calculate_graph_width(query)
    
    def calculate_graph_depth(self, query):
        """Compute the depth of QEP plan.

        Args:
            query (dict): The query plan.
        """
        res = 0

        def helper(query):
            nonlocal res
            currdep = []

            if "Plans" in query:
                for child in query['Plans']:
                    currdep.append(helper(child))
                res = max(res,max(currdep))
            else:
                return 1
            
            return 1 + max(currdep)
        
        return helper(query)
    
    def calculate_graph_width(self,query):
        """Compute the graph width.

        Args:
            query (dict): Query plan.
        """
        if 'Plans' not in query:
            return 0
        q = []
        maxWidth = 0
    
        q.append(query)
    
        while q:
            # Get the size of queue when the level order traversal for one level finishes
            count = len(q)
    
            # Update the maximum node count value
            maxWidth = max(count, maxWidth)
    
            while count:
                count = count-1
                temp = q.pop(0)
                if 'Plans' in temp:
                    for child in temp['Plans']:
                        q.append(child)
    
        return maxWidth


    def create_graph_node(self, root,query):
        """Constructs the graph recursively by forming an edge between each node
        and each of its child nodes.

        Args:
            root (Node): The parent node.
        """
        self.graph.add_node(root)
        if "Plans" not in query:
            return
        for child in query["Plans"]:
            child_node = Node(child["Node Type"],child["Total Cost"])
            self.graph.add_edge(root, child_node)
            self.create_graph_node(child_node,child)

    def save_graph_file(self) -> str:
        """Renders the graph and save the figure as an .png file
        in the 'static' folder.
        The frontend then renders the image on the UI to visualise the QEP.

        Returns:
            str: File name of graph
        """
        plt.figure(figsize=( 3*(2+2/(1+ math.exp(-self.tree_width))) , self.tree_depth * 1.5))
        plt.axis('equal')
        graph_name = f"qep_{str(time.time())}.png"
        file_name = os.path.join(os.getcwd(), "static", graph_name)
        plot_formatter_position = get_tree_node_pos(self.graph, self.root)
        node_labels = {x: str(x) for x in self.graph.nodes}
        the_base_size = 100
        nx.draw(
            self.graph,
            plot_formatter_position,
            with_labels=True,
            labels=node_labels,
            font_size=6,
            node_size=[len(v.__str__()) * the_base_size for v in self.graph.nodes()],
            node_color="#E2FAB5",
            node_shape="s",
            alpha=1,
        )
        plt.savefig(file_name)
        plt.clf()
        return graph_name

    def get_num_nodes(self) -> int:
        """Returns the number of nodes in the graph.
        Returns:
            int: Number of nodes
        """     
        return len(self.graph.nodes)

def get_tree_node_pos(G, root=None, width=1.0, height=1, vert_gap=0.2, vert_loc=0, xcenter=0.5):
    """From Joel's answer at https://stackoverflow.com/a/29597209/2966723.
    Licensed under Creative Commons Attribution-Share Alike

    Recursive program to define the positions. The recursion happens in _hierarchy_pos, which is called by get_tree_node_pos.
    The main role of hierarchy_pos is to do a bit of testing to make sure the graph is appropriate before entering the recursion.

    If the graph is a tree this will return the positions to plot this in a hierarchical layout.

    Args:
        G (DiGraph): The graph (must be a tree).
        root (Node, optional): The root node of the current branch.
        - If the tree is directed and this is not given, the root will be found and used.
        - If the tree is directed and this is given, the positions will be just for the descendants of this node.
        - If the tree is undirected and not given, a random choice will be used.
        Defaults to None.
        width (float, optional): Horizontal space allocated for this branch. Defaults to 1.0.
        height (int, optional): Vertical space allocated for this branch. Defaults to 1.
        vert_gap (float, optional):  Gap between levels of hierarchy. Defaults to 0.1.
        vert_loc (int, optional): Vertical location of root. Defaults to 0.
        xcenter (float, optional): Horizontal location of root. Defaults to 0.5.

    Raises:
        TypeError: Graph is not a tree.

    Returns:
        dict: [description]
    """

    if not nx.is_tree(G):
        raise TypeError("cannot use hierarchy_pos on a graph that is not a tree")

    if root is None:
        if isinstance(G, nx.DiGraph):
            root = next(iter(nx.topological_sort(G)))  # allows back compatibility with nx version 1.11
        else:
            root = random.choice(list(G.nodes))

    path_dict = dict(nx.all_pairs_shortest_path(G))
    max_height = 0
    for value in path_dict.values():
        max_height = max(max_height, len(value))
    vert_gap = height / max_height

    def _hierarchy_pos(G, root, width=1., vert_gap = 0.2, vert_loc = 0, xcenter = 0.5, min_dx=0.05 ):
        '''If there is a cycle that is reachable from root, then result will not be a hierarchy.

        G: the graph
        root: the root node of current branch
        width: horizontal space allocated for this branch - avoids overlap with other branches
        vert_gap: gap between levels of hierarchy
        vert_loc: vertical location of root
        xcenter: horizontal location of root
        '''

        def h_recur(G, root, width=width, vert_gap = vert_gap, vert_loc = vert_loc, xcenter = xcenter, 
                    pos = None, parent = None, parsed = [] ):
            if(root not in parsed):
                parsed.append(root)
                if pos == None:
                    pos = {root:(xcenter * 2,vert_loc)}
                else:
                    pos[root] = (xcenter * 2, vert_loc)
                neighbors = list(G.neighbors(root))
                if not isinstance(G, nx.DiGraph) and parent != None:
                    neighbors.remove(parent)
                if len(neighbors)!=0:
                    dx = max(min_dx, width / len(neighbors))
                    nextx = xcenter - width / 2 - max(min_dx, dx / 2)
                    for neighbor in neighbors:
                        nextx += dx
                        pos = h_recur(G,neighbor, width = dx, vert_gap = vert_gap, 
                                            vert_loc = vert_loc-vert_gap, xcenter=nextx, pos=pos, 
                                            parent = root, parsed = parsed)
            return pos
        return h_recur(G, root, width=width, vert_gap = 0.2, vert_loc = vert_loc, xcenter = xcenter)
    return _hierarchy_pos(G, root, width, vert_gap, vert_loc, xcenter)
