from flask import Flask, Blueprint, request, redirect,url_for, render_template
from new_commands import commands_main
from server import mongo_main
from flask import jsonify
from flask_cors import CORS, cross_origin
import pandas as pd 
import csv 
import search
import search_mongo

# app = Blueprint('app', __name__, static_folder='static', static_url_path='/static/admin')
app = Flask(__name__)
CORS(app)
app.config['CROSS_HEADERS'] = 'Content-Type'
cors = CORS(app, resources={r"/sql/*": {"origins": "*"}})


#sql
@app.route('/sql/readPartition',methods = ['GET'])
def readPartition():
    args = request.args
    path = args['path']
    partition = args['partition']
    json = commands_main("readPartition",path,partition)
    return json


@app.route('/sql/rm',methods = ['GET'])
def rm():
    args = request.args
    path = args['path']
    json = commands_main("rm",path)
    return json


@app.route('/sql/getPartitionLocations',methods = ['GET'])
def getPartitionLocations():
    args = request.args
    path = args['path']
    json = commands_main("getPartitionLocations",path)
    return json


@app.route('/sql/cat',methods = ['GET'])
def cat():
    args = request.args
    path = args['path']
    json = commands_main("cat",path)
    return json

@app.route('/sql/put',methods = ['GET','POST'])
def put():
    args = request.args
    path = args['path']
    partitionNum = args['no']
    hashCol = args['field']
    # file = request.files["file"]
    file = request.files['file']

    if not partitionNum:
        partitionNum = 5 
    if not hashCol:
        hashCol = 'sort_index' 

    path = path.split('/')
    filename = path[-1]

    path = path[:-1]
    path = '/'.join(path)
    if not path:
        path = '/'
    
    file.save(filename)
    json = commands_main("put",path,partitionNum,filename,hashCol)
    return json


@app.route('/sql/mkdir',methods = ['GET'])
def mkdir():
    args = request.args
    path = args['path']
    json = commands_main("mkdir",path)
    return json


@app.route('/sql/ls',methods = ['GET'])
def ls():
    args = request.args
    path = args['path']
    json = commands_main("ls",path)
    return json

# Search Functionality SQL
@app.route('/sql/search', methods = ["POST"])
def search_sql():
    args = request.json
    # print(args)
    print(f"Type is: {type(args)}")
    search_result = search.main(args)
    return search_result


@app.route('/mongodb/readPartition',methods = ['GET'])
def readPartitionMongo():
    args = request.args
    path = args['path']
    partition = args['partition']
    json = mongo_main("readPartition",path,None,partition)
    return json


@app.route('/mongodb/rm',methods = ['GET'])
def rmMongo():
    args = request.args
    path = args['path']
    json = mongo_main("rm",path)
    return json


@app.route('/mongodb/getPartitionLocations',methods = ['GET'])
def getPartitionLocationsMongo():
    args = request.args
    path = args['path']
    json = mongo_main("getPartitionLocations",path)
    return json


@app.route('/mongodb/cat',methods = ['GET'])
def catMongo():
    args = request.args
    path = args['path']
    json = mongo_main("cat",path)
    return json



@app.route('/mongodb/put',methods = ['GET', 'POST'])
def putMongo():

    args = request.args
    path = args['path']
    partitionNum = args['no']
    hashCol = args['field']
    file = request.files['file']

    if not partitionNum:
        partitionNum = 5 
    if not hashCol:
        hashCol = 'sort_index' 
    
    print(f"Path is {path}")
    path = path.split('/')
    filename = path[-1]

    path = path[:-1]
    path = '/'.join(path)
    print(f"Path is {path}")
    file.save(filename)
    json = mongo_main("put",path,filename, partitionNum,hashCol)
    print('mongo_main returns', json)
    return json


@app.route('/mongodb/mkdir',methods = ['GET'])
def mkdirMongo():
    args = request.args
    path = args['path']
    json = mongo_main("mkdir",path)
    return json


@app.route('/mongodb/ls',methods = ['GET'])
def lsMongo():
    args = request.args
    path = args['path']
    json = mongo_main("ls",path)
    return json


@app.route('/mongodb/search', methods = ["POST"])
def searchMongo():
    args = request.json
    print(f"Type is: {type(args)}")
    search_result = search_mongo.main(args)
    return search_result


if __name__ == '__main__':
    app.run()
