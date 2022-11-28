from genericpath import exists
from bson import is_valid
import pymongo
from pprint import pprint
from bson.objectid import ObjectId
from datetime import datetime
import pandas as pd

myclient = pymongo.MongoClient("mongodb://localhost:27017/")

mydb = myclient["Namenode"]
metadata = mydb["metadata"]
dir_structure = mydb['dir']
Datanode = myclient['Datanode']
ROOT_ID = metadata.find({}).sort('_id', 1).limit(1).next()['_id']



def is_valid_path(path):
    pass

def partitionErrorOutput(flag, path):

    if flag == -1:
        # print('Root is a special directory. No partitions exist for a Directory.') 
        return {"flag":-1,"desc":f"Root is a directory"}
    elif flag == -2:
        # print('This file does not exist.') 
        return {"flag":-1,"desc":f"File does not exist"}
    elif flag == -3:
        name = path.split('/')[-1] 
        # print(f"{name} is a Directory. No partitions exist for a Directory.")
        return {"flag":-1,"desc":f"{name} is a directory"}
    return


def pathValidatorMakeDir(FULL_PATH):

    """ Takes as input a full directory path.
        Returns True if the path is a valid path, False otherwise."""
    cleansed_path = FULL_PATH.rstrip('/')

    # First, check if the FULL_PATH was just the root directory -> if user entered 0 or any number of '/'
    if not cleansed_path:
        return False 

    # now, the other case considers all other paths
    pathsList = cleansed_path.split('/')
    def validator_helper(parent, fname):
        children = []
        # QUERY = f"SELECT name FROM (SELECT m.id, m.type, m.name, d.parent FROM meta_data m LEFT OUTER JOIN dir_structure d ON m.id = d.child) t WHERE parent = {parent}"
        QUERY = [
            {
                '$match': {
                    'parent': ObjectId(parent)
                }
            },
            {
                '$lookup': {
                    'from': 'metadata',
                    'localField': 'child',
                    'foreignField': '_id',
                    'as': 'joinedResult'
                }
            }
        ]
        cursor = dir_structure.aggregate(QUERY)
        for row in cursor:
            children.append(row['joinedResult'][0]['name'])
        return fname in children

    def getFileID(parent, fname):
        # QUERY = f"SELECT id FROM (SELECT m.id, m.type, m.name, d.parent FROM meta_data m LEFT OUTER JOIN dir_structure d ON m.id = d.child) t WHERE parent = {parent} and name = '{fname}'"
        QUERY = [

            {
                '$lookup': {
                    'from': 'metadata',
                    'localField': 'child',
                    'foreignField': '_id',
                    'as': 'joinedResult'
                }
            },
            {
                '$match': {
                    '$and': [
                        {
                            'parent': ObjectId(parent)
                        },
                        {
                            'joinedResult.name': fname
                        }
                    ]
                }

            }
        ]
        cursor = dir_structure.aggregate(QUERY)
        for x in cursor:
            id = x['joinedResult'][0]['_id']
        return id

    def alreadyExists(parent, fname):
        # QUERY = f"SELECT name FROM (SELECT m.id, m.type, m.name, d.parent FROM meta_data m LEFT OUTER JOIN dir_structure d ON m.id = d.child) t WHERE parent = {parent}"
        files = []
        QUERY = [
            {
                '$match': {
                    'parent': ObjectId(parent)
                }
            },
            {
                '$lookup': {
                    'from': 'metadata',
                    'localField': 'child',
                    'foreignField': '_id',
                    'as': 'joinedResult'
                }
            }
        ]
        cursor = dir_structure.aggregate(QUERY)
        for row in cursor:
            files.append(row['joinedResult'][0]['name'])
        return fname in files 

    def recursive(parent, paths_list, idx):
        currentFile = paths_list[idx]
        # incorrect path
        # In the end, check if the immediate parent of the folder I want to create has a child with the
        # ... same name as me or not. If yes, then we cannot create another folder with the same name.
        if len(paths_list) - 1 == idx:
            if alreadyExists(parent, currentFile):
                return -1 
            else:
                return 1

        if not validator_helper(parent, currentFile):
            print("Invalid Path")
            return -2 

        # else, get the id of this child 
        currentFileID = getFileID(parent, currentFile)

        return recursive(currentFileID, paths_list, idx + 1)
        
    return recursive(ROOT_ID, pathsList, 1)

    
# mainly used for the mkdir command
def getFolderID(PATH):

    if PATH == '/':
        return 1 

    pathsList = PATH.split('/')
    def validator_helper(parent, fname):
        children = []
        # QUERY = f"SELECT name FROM (SELECT m.id, m.type, m.name, d.parent FROM meta_data m LEFT OUTER JOIN dir_structure d ON m.id = d.child) t WHERE parent = {parent}"
        QUERY = [
            {
                '$match': {
                    'parent': ObjectId(parent)
                }
            },
            {
                '$lookup': {
                    'from': 'metadata',
                    'localField': 'child',
                    'foreignField': '_id',
                    'as': 'joinedResult'
                }
            }
        ]
        cursor = dir_structure.aggregate(QUERY)
        for row in cursor:
            children.append(row['joinedResult'][0]['name'])
        return fname in children

    def getFileID(parent, fname):
        # QUERY = f"SELECT id FROM (SELECT m.id, m.type, m.name, d.parent FROM meta_data m LEFT OUTER JOIN dir_structure d ON m.id = d.child) t WHERE parent = {parent} and name = '{fname}'"
        QUERY = [

            {
                '$lookup': {
                    'from': 'metadata',
                    'localField': 'child',
                    'foreignField': '_id',
                    'as': 'joinedResult'
                }
            },
            {
                '$match': {
                    '$and': [
                        {
                            'parent': ObjectId(parent)
                        },
                        {
                            'joinedResult.name': fname
                        }
                    ]
                }

            }
        ]
        cursor = dir_structure.aggregate(QUERY)
        for x in cursor:
            id = x['joinedResult'][0]['_id']
        return id 

    def recursive(parent, paths_list, idx):
        currentFile = paths_list[idx]
        # incorrect path
        if not validator_helper(parent, currentFile):
            # print("Invalid Path")
            return False 

        # else, get the id of this child 
        currentFileID = getFileID(parent, currentFile)

        # In the end, check if the immediate parent of the folder I want to create has a child with the
        # ... same name as me or not. If yes, then we cannot create another folder with the same name.
        if len(paths_list) - 1 == idx:
            return currentFileID
        else:
            return recursive(currentFileID, paths_list, idx + 1) 

    return recursive(ROOT_ID, pathsList, 1)


def ls(FULL_PATH):
    # print("In ls")
    cleansed_path = FULL_PATH.rstrip('/')
    if not cleansed_path:
        files = []
        cursor = dir_structure.aggregate([
            {
                '$match': {
                    'parent': ObjectId(ROOT_ID)
                }
            },
            {
                '$lookup': {
                    'from': 'metadata',
                    'localField': 'child',
                    'foreignField': '_id',
                    'as': 'joinedResult'
                }
            }
        ])
        for row in cursor:
            fname = row['joinedResult'][0]['name']
            files.append(fname)
        
        return files

    pathsList = cleansed_path.split('/')
    def validator_helper(parent, fname):
        children = []
        QUERY = [
            {
                '$match': {
                    'parent': ObjectId(parent)
                }
            },
            {
                '$lookup': {
                    'from': 'metadata',
                    'localField': 'child',
                    'foreignField': '_id',
                    'as': 'joinedResult'
                }
            }
        ]
        cursor = dir_structure.aggregate(QUERY)
        for row in cursor:
            children.append(row['joinedResult'][0]['name'])
        return fname in children

    def getFileID(parent, fname):
        QUERY = [

            {
                '$lookup': {
                    'from': 'metadata',
                    'localField': 'child',
                    'foreignField': '_id',
                    'as': 'joinedResult'
                }
            },
            {
                '$match': {
                    '$and': [
                        {
                            'parent': ObjectId(parent)
                        },
                        {
                            'joinedResult.name': fname
                        }
                    ]
                }

            }
        ]
        cursor = dir_structure.aggregate(QUERY)
        for x in cursor:
            id = x['joinedResult'][0]['_id']
        return id

    def recursive(parent, paths_list, idx):
        currentFile = paths_list[idx]
        # incorrect path
        if not validator_helper(parent, currentFile):
            # print("Invalid Path")
            return 

        # else, get the id of this child 
        currentFileID = getFileID(parent, currentFile)

        if len(paths_list) - 1 == idx:
            files = []
            QUERY = [
                {
                    '$match': {
                        'parent': ObjectId(currentFileID)
                    }
                },
                {
                    '$lookup': {
                        'from': 'metadata',
                        'localField': 'child',
                        'foreignField': '_id',
                        'as': 'joinedResult'
                    }
                }
            ]
            cursor = dir_structure.aggregate(QUERY)
            for row in cursor:
                files.append(row['joinedResult'][0]['name'])
            return files

        else:
            return recursive(currentFileID, paths_list, idx + 1)

    return recursive(ROOT_ID, pathsList, 1)
    
# HAVE TO MAKE A CONSTRAINT SUCH THAT A USER CANNOT KEEP ADDING THE SAME NAMED FILE IN THE SAME PATH
def makeDir(FULL_PATH):


    cleansed_path = FULL_PATH.rstrip('/')
    files = cleansed_path.split('/')

    # First, check if the full path is valid or not
    flag = pathValidatorMakeDir(FULL_PATH=FULL_PATH)
    if flag == 1:
        # do checking only when there is actually something entered by the user
        if files:
            FOLDER_TO_ADD = files[-1]

            PATH_OF_PARENT = "/".join(files[:-1])
            if not PATH_OF_PARENT: PATH_OF_PARENT = '/'
            parentID = getFolderID(PATH_OF_PARENT)

            # Now, we first persist the meta data of the folder to the meta_data table 
            # META_DATA_QUERY = f'INSERT INTO meta_data (name, type, partitions, ctime) VALUES ("{FOLDER_TO_ADD}", "FOLDER", NULL, "{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}");'
            META_DATA_QUERY = {'name': FOLDER_TO_ADD, 'partitions': "", 'ctime': datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 'folder': 'True'}
            cursor = metadata.insert_one(META_DATA_QUERY)
            # db.commit()
            
            # Now, we add information to the dir_structure table 
            # CHILD_ID_QUERY = "SELECT MAX(id) FROM meta_data;"
            childID = metadata.find({}).sort('_id',-1).limit(1).next()['_id']
            # DIR_STR_QUERY = f"INSERT INTO dir_structure VALUES ({parentID}, {childID});"
            DIR_STR_QUERY = {'parent' : ObjectId(parentID), 'child': ObjectId(childID)}
            cursor = dir_structure.insert_one(DIR_STR_QUERY)
            # db.commit()
            # print("All good. Folder created.")
            return {"flag":1,"desc":"Folder created successfully"}
            
            
    elif flag == -1:
        # print("Folder already exists.")
        return {"flag":-1,"desc":"Folder already exists"}
    
    elif flag == -2:
        # print("Invalid path entered.")
        return {"flag":-1,"desc":"Invalid path entered"}
    
    return


def cat(FULL_PATH):

    cleansed_path = FULL_PATH.rstrip('/')
    if not cleansed_path:
        # print('Root is a special directory.')
        return {"flag":-1,"desc":"Root is a directory"}
    
    id = getFolderID(cleansed_path)
    print('ID:', id)
    if id == False:
        # print("This file does not exist")
        return {"flag":-1,"desc":"File does not exist"}
    
    name = cleansed_path.split('/')[-1]

    folder = metadata.find({'_id' : ObjectId(id)},{'folder': 1}).next()
    # cursor.execute(f"SELECT type FROM meta_data WHERE id = {id};")
    # type = cursor.fetchone()[0]
    if folder['folder']:
        # print(f"{name} is a Directory.")
        return {"flag":-1,"desc":f"{name} is a directory"}
    
        return

    # cursor.execute(f"SELECT partitions FROM meta_data WHERE id = {id};")
    # partitions = cursor.fetchone()[0]
    partitions = metadata.find({'_id': ObjectId(id)},{'partitions': 1}).next()['partitions']
    partitions = partitions.split(',')

    parent_id = getFolderID('/'.join(cleansed_path.split('/')[0:-1]))
    collection_name = str(parent_id) +'_'+ str(folder['_id'])+'_'
    dataframes = []
    for partition in partitions:
        # df = pd.DataFrame(cursor2.execute(f"SELECT * FROM {partition};"))
        rows = []
        cursor = Datanode[collection_name+partition].find({})
        df = pd.DataFrame(list(cursor))

        # cursor2.execute(f"SELECT * FROM {partition};")
        # cols = list(cursor2.column_names)
        
        # for row in cursor2:
        #     r = list(row)
        #     rows.append(r)
        # df = pd.DataFrame(rows, columns=cols)
        dataframes.append(df)
    total = pd.concat(dataframes)
    # total["sort_index"] = pd.to_numeric(total["sort_index"])
    total.sort_values(by=['sort_index'], inplace=True, ignore_index=True)
    total.drop(['_id', 'sort_index'],axis=1, inplace=True)
    return total.to_json(orient='table')

def getPartitionLocations(FULL_PATH):

    cleansed_path = FULL_PATH.rstrip('/')
    if not cleansed_path:
        # print('Root is a special directory. No partitions exist for a Directory.')
        return {"flag":-1,"desc":f"Root is a directory"}

    id = getFolderID(cleansed_path)
    if id == False:
        # print("This file does not exist")
        return {"flag":-1,"desc":f"File does not exist"}

    name = cleansed_path.split('/')[-1]
    # cursor.execute(f"SELECT type FROM meta_data WHERE id = {id};")
    # type = cursor.fetchone()[0]
    folder = metadata.find({'_id' : ObjectId(id)},{'folder': 1}).next()
    if folder['folder']:
        # print(f"{name} is a Directory. No partitions exist for a Directory.")
        return {"flag":-1,"desc":f"{name} is a directory"}

    # cursor.execute(f"SELECT partitions FROM meta_data WHERE id = {id};")
    # partitions = cursor.fetchone()[0]
    partitions = metadata.find({'_id': ObjectId(id)},{'partitions': 1}).next()['partitions']
    partitions = partitions.split(',')
    
    # print(f"Partitions for the file {FULL_PATH} are: {partitions}")
    # return partitions
    return {"partitions":partitions}
    

def rm(FULL_PATH):

    cleansed_path = FULL_PATH.rstrip('/')
    id = getFolderID(cleansed_path)
    if id == False:
        # print('Invalid path entered.')
        return {"flag":-1,"desc":f"Invalid path entered"}
    partitions = getPartitionLocations(FULL_PATH)
    if partitions in [-1, -2, -3]:
        return partitionErrorOutput(partitions, FULL_PATH)
        
    
    # Delete from dir_structure table  
    # cursor.execute(f"DELETE FROM dir_structure WHERE child = {id};")
    # db.commit()

    # Next, delete from meta_data table 
    # cursor.execute(f"DELETE FROM meta_data WHERE id = {id};")
    # db.commit()
    # Then, delete all the relevant partition tables
    parent_id = getFolderID('/'.join(cleansed_path.split('/')[:-1]))
    collection_name = str(parent_id) + '_' + str(id) + '_'
    for partition in partitions['partitions']:
        # cursor2.execute(f"DROP TABLE {partition};")
        
        cursor = Datanode[collection_name+partition].drop()
        # db2.commit()
    
    cursor = dir_structure.delete_one({'child': ObjectId(id)})
    cursor = metadata.delete_one({'_id': ObjectId(id)})

    # print("File removed. Refresh to see changes.")
    return {"flag":1,"desc":f"File removed successfully"}


def readPartition(FULL_PATH, partition_num):

    # Make sure that the partition number entered is actually the correct one for the file 
    partitions = getPartitionLocations(FULL_PATH=FULL_PATH)['partitions']
    if partitions in [-1, -2, -3]:
        partitionErrorOutput(partitions, FULL_PATH)
        return 
    if partition_num not in partitions:
        # print("Partition location and file path do not match.")
        return {"flag":-1,"desc":f"Partition location and file path do not match"}
    
    # print(f"Retreiving data for {FULL_PATH}:")
    # cursor2.execute(f"SELECT * FROM {partition_num};")
    # cols = list(cursor2.column_names)
    # rows = []
    # for row in cursor2: 
    #     r = list(row)
    #     rows.append(r)
    # df = pd.DataFrame(rows, columns=cols)
    cleansed_path = FULL_PATH.rstrip('/')
    id = getFolderID(cleansed_path)
    parent_id = getFolderID('/'.join(cleansed_path.split('/')[:-1]))
    collection_name = str(parent_id) + '_' + str(id) + '_'
    cursor = Datanode[collection_name+partition_num].find({})
    df = pd.DataFrame(list(cursor))
    df.drop(columns=['_id', 'sort_index'], inplace=True)
    # print(df.head())  
    return df.to_json(orient = 'table') 

def put(target, file_path, no_of_partitions = 5, field = 'sort_index'):

    no_of_partitions = int(no_of_partitions)
    flag = pathValidatorMakeDir(FULL_PATH=target)
    if flag == -1:
        cleansed_path = target.rstrip('/')


        id = getFolderID(cleansed_path+'/'+file_path)
        # print('ID:', id)
        if id != False:
            # print("A file with same name already exists")
            return {"flag":-1,"desc":f"File already exists"}
        # print("put the file")
        data = pd.read_csv(file_path)
        data['sort_index'] = [x for x in range(len(data))]
        data['partition'] = data[field].map(lambda x: x%no_of_partitions)
        partitions = ['p'+str(x) for x in range(no_of_partitions)]

        META_DATA_QUERY = {'name': file_path, 'partitions': ','.join(partitions), 'ctime': datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 'folder': False}
        child_id = metadata.insert_one(META_DATA_QUERY)
        child_id = child_id.inserted_id
        # print("Inserted file ID:", child_id)
        parent_id = getFolderID(target)
        DIR_STR_QUERY = {'parent' : ObjectId(parent_id), 'child': ObjectId(child_id)}
        cursor = dir_structure.insert_one(DIR_STR_QUERY)
        # print('Dir Structure record inserted:', cursor.inserted_id)

        for x in range(no_of_partitions):
            partition_data = data[data['partition'] == x]
            partition_data = partition_data.drop(['partition'], axis=1)
            collection_name = str(parent_id) + '_' + str(child_id) + '_' + partitions[x]
            cursor = Datanode.create_collection(collection_name)
            if(len(partition_data)>0):
                cursor = Datanode[collection_name].insert_many(partition_data.to_dict(orient='records'))
                # print('Partition data inserted:', collection_name)
        return {"flag":1,"desc":f"File uploaded successfully"}

    elif flag == 1:
        # print("Path does not exists")
        return {"flag":-1,"desc":f"Path does not exist"}

    elif flag == -2:
        # print("Invalid path")
        return {"flag":-1,"desc":f"Invalid path"}
    
    # data = pd.read_csv(file_path)







# print(ls("/tejas"))
# print(makeDir('/qasim/work'))
# print(cat('/tejas/hello.csv'))
# print(readPartition('/tejas/a/hello.csv', '111'))
# print(rm('/tejas/a/hello.csv'))
# print(put("/tejas/Book1.csv",'Book1.csv', 3, 'aaa'))

def mongo_main(cmd, path, local=None, partitionNum=None, hashCol = 'sort_index'):

    if cmd == "readPartition":
        return readPartition(path, partitionNum)
        # print(df)
        

    elif cmd == "rm":
        return rm(path)

    elif cmd == "getPartitionLocations":
        return getPartitionLocations(path)
        
        #if error, returning error json
        # if nameTable == None:
        #     return partitions

        # else:
        #     json = {"tableName": nameTable,"partitions":partitions}
        #     return json
            
    
    elif cmd == "cat":
        df = cat(path)
        # print(df)
        return df
        # print("View your file below:")
        
    
    elif cmd == "put":
        print('in mongo_main calling put')
        return put(path,local,partitionNum, hashCol)

    elif cmd == "mkdir":
        return makeDir(path)

    elif cmd == "ls":
        json = {"files":ls(path)}
        return json

    return 
















# cursor = metadata.find({}).sort('_id',-1).limit(1)
# root_id = metadata.find({}).sort('_id', 1).limit(1)
# print("Root ",root_id.next()['_id'])
# cursor = metadata.find({'_id': ObjectId("636461b1e9ce64d1dd1e427a")})
# cursor = dir_structure.insert_one({'parent' : ObjectId('63646763e9ce64d1dd1e4284'), 'child' : ObjectId('636461b1e9ce64d1dd1e427e')})
# for x in cursor:
#     print(x)

# id = []
# QUERY = [
    
#     {
#         '$lookup': {
#             'from': 'metadata',
#             'localField': 'child',
#             'foreignField': '_id',
#             'as': 'joinedResult'
#         }
#     },
#     {
#         '$match': {
#             '$and':[
#                 {
#                     'parent' : 2
#                 },
#                 {
#                     'joinedResult.name': 'a'
#                 }
#             ]
#         }
            
#     }
# ]
# cursor = dir_structure.aggregate(QUERY)
# print(cursor)
# for x in cursor:
#     print("X",x)

# print(ROOT_ID)

# QUERY = [
#             {
#                 '$match': {
#                     'parent': ObjectId('636461b1e9ce64d1dd1e4279')
#                 }
#             },
#             {
#                 '$lookup': {
#                     'from': 'metadata',
#                     'localField': 'child',
#                     'foreignField': '_id',
#                     'as': 'joinedResult'
#                 }
#             }
#         ]
# cursor = dir_structure.aggregate(QUERY)
# for x in cursor:
#     print(x['joinedResult'][0]['_id'])