from main import main_solver, solve_session
from flask import Flask, jsonify, request, make_response, abort
import pickle
from flask_httpauth import HTTPBasicAuth
import threading
import ast
import os
auth = HTTPBasicAuth()
from waitress import serve
import setproctitle as spt
app = Flask(__name__)


spt.setproctitle("on_demand_service")

@app.route('/on_demand/run_solver', methods=['GET'])
@auth.login_required
def run_solve():
    date  = request.args.get('date', None)
    ls_cluster_id  = request.args.get('cluster_id', None)
    print(type(ls_cluster_id))
    ls_cluster_id = ast.literal_eval(ls_cluster_id) if str(type(ls_cluster_id)) == "<class 'str'>" else ls_cluster_id
    obj_solve_session = solve_session(start_date=date, ls_cluster_id=ls_cluster_id)
    thread = threading.Thread(target=obj_solve_session.run_solver)
    thread.start()

    return jsonify({"solve_session_token":obj_solve_session.solve_token ,'task':
        "solver is running for cluster id's {}".format(str(ls_cluster_id))})

@app.route('/on_demand/solve_session_status', methods=['GET'])
@auth.login_required
def solve_status():
    token = request.args.get('solve_token', None)
    solve_token_location = os.path.dirname(os.path.realpath(__file__)).replace("\\", "/") + "/Local/Files/"
    pkl_file = solve_token_location + "solve_session.pkl"
    file = open(pkl_file, "rb")
    dict_master = pickle.load(file)[token]
    file.close()
    return jsonify(dict_master)

@app.errorhandler(404)
def not_found(error):
    return make_response(jsonify({'error': 'Not found'}), 404)

@auth.get_password
def get_password(username):
    if username == 'rtt_on_demand':
        return 'bPgwQj2VyVMn5xIj'
    return None

# if __name__ == '__main__':
#     #website_url = '10.99.69.222'
#     #app.config['SERVER_NAME'] = website_url
#     app.run(host='0.0.0.0', port=5000)

# website_url = 'solverTest.rtt.co.za'
# app.config['SERVER_NAME'] = website_url
serve(app, host='0.0.0.0', port=5000, threads=1) #WAITRESS!