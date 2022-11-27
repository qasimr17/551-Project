from flask import Flask, Blueprint, request, redirect,url_for, render_template
from commands import commands_main
from server import mongo_main
from flask import jsonify
from flask_cors import CORS, cross_origin
import pandas as pd 
import csv 
import search

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
    # del json['tableName']
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
    # json = jsonify(json)
    # json.headers.add('Access-Control-Allow-Origin','*')
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
    
    # cols = data[0].split(',')
    # print(cols)
    # print(type(cols))
    # print(data[1])
    # df = pd.DataFrame(data=data[1:], columns=list(cols))
    # print(df)
    print(f"Path is {path}")
    path = path.split('/')
    filename = path[-1]

    path = path[:-1]
    path = '/'.join(path)
    print(f"Path is {path}")
    file.save(filename)
    json = commands_main("put",path,partitionNum,filename,hashCol)
    return json

    #Working Solution 1: 
    # file.save('try.csv')
    # df = pd.read_csv('./try.csv')


    #print(df)
    # saving and reading the csv 
    # next, pass the location of this csv to the put function 
    

    # f = request.form.get('file')
    # print(f)
    return "1"
    # f.save(secure_filename(f.filename))
    # f.stream.seek(0)
    # stream = io.StringIO(f.stream.read().decode("UTF8"), newline=None)
    # reader = csv.reader(stream)
    # i=0
    # for row in reader:
    #     print(row)
    #     i+=1
    #     if i >3:
    #         break

    # file = request.body
    # print(file)
    # return '1'

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
    # json = jsonify(json)
    # json.headers.add('Access-Control-Allow-Origin','*')
    return json


# @app.route('/sql/search',methods = ['GET'])
# def search():
#     args = request.args
#     query = args['query']
#     json = commands_main(query)
#     return json

# Search Functionality SQL
@app.route('/sql/search', methods = ["POST"])
def search_sql():
    args = request.json
    # print(args)
    print(f"Type is: {type(args)}")
    search_result = search.main(args)
    return search_result
#mongodb

@app.route('/mongodb/readPartition',methods = ['GET'])
def readPartitionMongo():
    args = request.args
    path = args['path']
    partition = args['partition']
    json = mongo_main("readPartition",path,partition)
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



@app.route('/mongodb/put',methods = ['GET'])
def putMongo():
    args = request.args
    path = args['path']
    partitionNum = args['no']
    hashCol = args['field']
    file = request.files['file']

    filename = 'try.csv'
    file.save('try.csv')
    json = commands_main("put",path,partitionNum,filename,hashCol)
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






if __name__ == '__main__':
    app.run()
