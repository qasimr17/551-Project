import mysql.connector
import pandas as pd
# import streamlit as st 
from datetime import datetime
import sys


# creating a view for easier implementation of commands
DROP_VIEW_QUERY = "DROP VIEW IF EXISTS simplified;"
CREATE_VIEW_QUERY = "CREATE VIEW simplified AS (SELECT m.id, m.type, m.name, d.parent FROM meta_data m LEFT OUTER JOIN dir_structure d ON m.id = d.child);"

############# 
# Need to create a function for PATH VALIDATION.
# First, we look to implement the ls command 
# example, in our case, we can try: ls /A to get c.csv

#### UTILS
def partitionErrorOutput(flag, path):

    if flag == -1:
        print('Root is a special directory. No partitions exist for a Directory.') 
    elif flag == -2:
        print('This file does not exist.') 
    elif flag == -3:
        name = path.split('/')[-1] 
        print(f"{name} is a Directory. No partitions exist for a Directory.")
    return

def pathValidatorMakeDir(FULL_PATH, cursor):

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
        QUERY = f"SELECT name FROM (SELECT m.id, m.type, m.name, d.parent FROM meta_data m LEFT OUTER JOIN dir_structure d ON m.id = d.child) t WHERE parent = {parent}"
        cursor.execute(QUERY)
        for r in cursor:
            children.append(r[0])
        return fname in children

    def getFileID(parent, fname):
        QUERY = f"SELECT id FROM (SELECT m.id, m.type, m.name, d.parent FROM meta_data m LEFT OUTER JOIN dir_structure d ON m.id = d.child) t WHERE parent = {parent} and name = '{fname}'"
        cursor.execute(QUERY)
        return cursor.fetchone()[0]

    def alreadyExists(parent, fname):
        QUERY = f"SELECT name FROM (SELECT m.id, m.type, m.name, d.parent FROM meta_data m LEFT OUTER JOIN dir_structure d ON m.id = d.child) t WHERE parent = {parent}"
        files = []
        cursor.execute(QUERY)
        for row in cursor:
            files.append(row[0])
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
            # print("Invalid Path")
            return -2 

        # else, get the id of this child 
        currentFileID = getFileID(parent, currentFile)

        return recursive(currentFileID, paths_list, idx + 1)
        
    return recursive(1, pathsList, 1)

    
# mainly used for the mkdir command
def getFolderID(PATH, cursor):

    if PATH == '/':
        return 1 

    pathsList = PATH.split('/')
    def validator_helper(parent, fname):
        children = []
        QUERY = f"SELECT name FROM (SELECT m.id, m.type, m.name, d.parent FROM meta_data m LEFT OUTER JOIN dir_structure d ON m.id = d.child) t WHERE parent = {parent}"
        cursor.execute(QUERY)
        for r in cursor:
            children.append(r[0])
        return fname in children

    def getFileID(parent, fname):
        QUERY = f"SELECT id FROM (SELECT m.id, m.type, m.name, d.parent FROM meta_data m LEFT OUTER JOIN dir_structure d ON m.id = d.child) t WHERE parent = {parent} and name = '{fname}'"
        cursor.execute(QUERY)
        return cursor.fetchone()[0] 

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

    return recursive(1, pathsList, 1)

## END OF UTILS



def ListFiles(FULL_PATH, cursor=None):

    """ Takes as input a full path (string) to a location, and returns all the files/folders
    that exist in that location."""
    if cursor is None:
        db = mysql.connector.connect(host="localhost", user="root", passwd="Garden@eden97", database="namenode")
        cursor = db.cursor(buffered=True)

    cleansed_path = FULL_PATH.rstrip('/')

    # First, check if the FULL_PATH was just the root directory -> if user entered 0 or any number of '/'
    if not cleansed_path:
        cursor.execute(DROP_VIEW_QUERY)
        cursor.execute(CREATE_VIEW_QUERY)
        cursor.execute("SELECT name FROM (SELECT m.id, m.type, m.name, d.parent FROM meta_data m LEFT OUTER JOIN dir_structure d ON m.id = d.child) t  WHERE parent = 1;")
        files = []
        for row in cursor:
            fname = row[0]
            files.append(fname)
        return files

    # now, the other case considers all other paths
    pathsList = cleansed_path.split('/')
    def validator_helper(parent, fname):
        children = []
        QUERY = f"SELECT name FROM (SELECT m.id, m.type, m.name, d.parent FROM meta_data m LEFT OUTER JOIN dir_structure d ON m.id = d.child) t WHERE parent = {parent}"
        cursor.execute(QUERY)
        for r in cursor:
            children.append(r[0])
        return fname in children

    def getFileID(parent, fname):
        QUERY = f"SELECT id FROM (SELECT m.id, m.type, m.name, d.parent FROM meta_data m LEFT OUTER JOIN dir_structure d ON m.id = d.child) t  WHERE parent = {parent} and name = '{fname}'"
        cursor.execute(QUERY)
        return cursor.fetchone()[0]

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
            cursor.execute(f"SELECT name FROM (SELECT m.id, m.type, m.name, d.parent FROM meta_data m LEFT OUTER JOIN dir_structure d ON m.id = d.child) t WHERE parent = {currentFileID}")
            for row in cursor:
                files.append(row[0])
            return files

        else:
            return recursive(currentFileID, paths_list, idx + 1)

    return recursive(1, pathsList, 1)


# HAVE TO MAKE A CONSTRAINT SUCH THAT A USER CANNOT KEEP ADDING THE SAME NAMED FILE IN THE SAME PATH
def makeDir(FULL_PATH, cursor, db):
    cleansed_path = FULL_PATH.rstrip('/')
    files = cleansed_path.split('/')

    # First, check if the full path is valid or not
    flag = pathValidatorMakeDir(FULL_PATH=FULL_PATH, cursor=cursor)
    if flag == 1:
        # do checking only when there is actually something entered by the user
        if files:
            FOLDER_TO_ADD = files[-1]

            PATH_OF_PARENT = "/".join(files[:-1])
            if not PATH_OF_PARENT: PATH_OF_PARENT = '/'
            parentID = getFolderID(PATH_OF_PARENT, cursor=cursor)

            # Now, we first persist the meta data of the folder to the meta_data table 
            META_DATA_QUERY = f'INSERT INTO meta_data (name, type, partitions, ctime) VALUES ("{FOLDER_TO_ADD}", "FOLDER", NULL, "{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}");'
            cursor.execute(META_DATA_QUERY)
            db.commit()
            
            # Now, we add information to the dir_structure table 
            CHILD_ID_QUERY = "SELECT MAX(id) FROM meta_data;"
            cursor.execute(CHILD_ID_QUERY)
            childID = cursor.fetchone()[0]
            DIR_STR_QUERY = f"INSERT INTO dir_structure VALUES ({parentID}, {childID});"
            cursor.execute(DIR_STR_QUERY)
            db.commit()
            # print("All good. Folder created.")
            return {"flag":1,"desc":"Folder created successfully"}
            
    elif flag == -1:
        # print("Folder already exists.")
        return {"flag":-1,"desc":"Folder already exists"}
    
    elif flag == -2:
        # print("Invalid path entered.")
        return {"flag":-1,"desc":"Invalid path entered"}
    return flag


def cat(FULL_PATH, cursor, cursor2):

    cleansed_path = FULL_PATH.rstrip('/')
    if not cleansed_path:
        # print('Root is a special directory.')
        
        return {"flag":-1,"desc":"Root is a special directory"}

    id = getFolderID(cleansed_path, cursor)
    if id == False:
        # print("This file does not exist")
        return {"flag":-1,"desc":"File does not exist"}

    name = cleansed_path.split('/')[-1]
    cursor.execute(f"SELECT type FROM meta_data WHERE id = {id};")
    type = cursor.fetchone()[0]
    if type == "FOLDER":
        # print(f"{name} is a Directory.")
        # return
        return {"flag":-1,"desc":f"{name} is a Directory"}

    cursor.execute(f"SELECT partition_table FROM meta_data WHERE id = {id};")
    nameTable = cursor.fetchone()[0]

    cursor2.execute(f"SELECT * FROM {nameTable};")
    cols = list(cursor2.column_names)
    rows = []
    for row in cursor2:
        r = list(row)
        rows.append(r)
    df = pd.DataFrame(rows, columns=cols)
    df["sort_index"] = pd.to_numeric(df["sort_index"])
    df.sort_values(by=['sort_index'], inplace=True, ignore_index=True)
    df.drop(['sort_index'],axis=1, inplace=True)
    
    return df.to_json(orient = 'table') 

    

def getPartitionLocations(FULL_PATH, cursor):

    cleansed_path = FULL_PATH.rstrip('/')
    if not cleansed_path:
        # print('Root is a special directory. No partitions exist for a Directory.')
        # return -1 

        return (None,{"flag":-1,"desc":"Root is a special directory"})
    id = getFolderID(cleansed_path, cursor=cursor)
    if id == False:
        # print("This file does not exist")
        # return -2 
        return (None,{"flag":-1,"desc":"File does not exist"})

    name = cleansed_path.split('/')[-1]
    cursor.execute(f"SELECT type FROM meta_data WHERE id = {id};")
    type = cursor.fetchone()[0]
    if type == "FOLDER":
        # print(f"{name} is a Directory. No partitions exist for a Directory.")
        return (None,{"flag":-1,"desc":f"{name} is a directory"})

    cursor.execute(f"SELECT partitions FROM meta_data WHERE id = {id};")
    partitions = cursor.fetchone()[0]
    # print(partitions)
    if partitions:
        partitions = partitions.split(',')

    cursor.execute(f"SELECT partition_table FROM meta_data WHERE id = {id};")
    nameTable = cursor.fetchone()[0]
    # print(f"Partitions for the file {FULL_PATH} are: {partitions}")
    # return partitions
    return (nameTable,partitions)
    

def rm(FULL_PATH, cursor, cursor2, db, db2):

    id = getFolderID(FULL_PATH, cursor=cursor)
    if id == False:
        # print('Invalid path entered.')
        return {"flag":-1,"desc":"File does not exist"}
    nameTable, partitions = getPartitionLocations(FULL_PATH, cursor=cursor)

    if nameTable == None:
        return partitions

    # Delete from dir_structure table  
    cursor.execute(f"DELETE FROM dir_structure WHERE child = {id};")
    db.commit()

    # Next, delete from meta_data table 
    cursor.execute(f"DELETE FROM meta_data WHERE id = {id};")
    db.commit()


    # Then, delete the table from datanode
    cursor2.execute(f"DROP TABLE {nameTable};")
    db2.commit()

    return {"flag":1,"desc":"File removed"}


def readPartition(FULL_PATH, partition_num, cursor, cursor2):

    # Make sure that the partition number entered is actually the correct one for the file 
    nameTable,partitions = getPartitionLocations(FULL_PATH=FULL_PATH, cursor=cursor)

    #if table name doesnt exists, return the error json
    if nameTable == None:
        return partitions


    if partition_num not in partitions:
        return {"flag":-1,"desc":"Partition does not exist"}

    cursor2.execute(f"SELECT * FROM {nameTable} PARTITION ({partition_num});")
    cols = list(cursor2.column_names)
    rows = []
    for row in cursor2: 
        r = list(row)
        rows.append(r)
    df = pd.DataFrame(rows, columns=cols)
    df.drop(columns=['sort_index'], inplace=True)
    return df.to_json(orient = 'table')   

def put(LOCAL_PATH, FULL_PATH, cursor, cursor2,db, db2, partitionNum = 5 ,hashCol = 'sort_index'):
    
    #full_path has path till parent
    cleansed_path = FULL_PATH.rstrip('/')
    files = cleansed_path.split('/')
    FILE_TO_ADD = LOCAL_PATH.split('/')[-1]


    # First, check if the full path is valid or not
    if files == ['']: #for root directory 
        flag = 1
    else:
        flag = pathValidatorMakeDir(FULL_PATH=f'{FULL_PATH}/{FILE_TO_ADD}', cursor=cursor)
    
    if flag == 1:
        
        #check if file sent is csv
        if LOCAL_PATH.split('.')[-1] != 'csv':
            return {"flag":-1,"desc":"File type not accomodated. Please try again with csv"}

        #reading csv and check if local path is correct   
        try:
            df = pd.read_csv(LOCAL_PATH)
        except FileNotFoundError:
            # print("Local file not found")
            return {"flag":-1,"desc":"Local file not found"}
        except pd.errors.EmptyDataError:
            print("No data in local file")
            return {"flag":-1,"desc":"No data in local file"}
        except pd.errors.ParserError:
            # print("Parse error, please check your file")
            return {"flag":-1,"desc":"Parse error, please check your file"}
        except Exception as e:
            # print(f"Error processing your file. \nThe error: {e}")
            return {"flag":-1,"desc":f"Error processing your file. \nThe error: {e}"}

        #adding an index to allow sorting when reading later
        df['sort_index'] = range(0,len(df))
        if df[hashCol].dtype != int:
            # print("Hashing column can only be int. Please try again")
            
            return {"flag":-1,"desc":"Hashing column can only be int"}
        
        #insert into namenode metadata
        PATH_OF_PARENT = "/".join(files)
        if not PATH_OF_PARENT: PATH_OF_PARENT = '/'
        parentID = getFolderID(PATH_OF_PARENT, cursor=cursor)

        # Now, we first persist the meta data of the file to the meta_data table 
        META_DATA_QUERY = f'INSERT INTO meta_data (name, type, partition_table, partitions, ctime) VALUES ("{FILE_TO_ADD}", "FILE", NULL, NULL, "{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}");'
        cursor.execute(META_DATA_QUERY)
        db.commit()
            
        # Now, we add information to the dir_structure table 
        CHILD_ID_QUERY = "SELECT MAX(id) FROM meta_data;"
        cursor.execute(CHILD_ID_QUERY)
        childID = cursor.fetchone()[0]
        DIR_STR_QUERY = f"INSERT INTO dir_structure VALUES ({parentID}, {childID});"
        cursor.execute(DIR_STR_QUERY)
        db.commit()

        #create table
        #creating sql command for create table
        createTable = ''
        for col in df.columns:
            dtype = ''
            if df[col].dtype == int:
                dtype = 'int'
            elif df[col].dtype == float:
                dtype = 'float'
            elif df[col].dtype == bool:
                dtype = 'bool'
            else:
                dtype = 'text'
            createTable += f'{col} {dtype}, '

        if hashCol == 'sort_index':
            createTable += f'PRIMARY KEY (sort_index)'
        else:
            createTable += f'PRIMARY KEY (sort_index,{hashCol})'
    
        #table name: parentname_childname
        nameTable = f't_{parentID}_{childID}'

        #create in database
        DROP_TABLE = f'DROP TABLE IF EXISTS {nameTable}'
        cursor2.execute(DROP_TABLE)

        CREATE_TABLE = f'''CREATE TABLE {nameTable} ({createTable}) 
                            PARTITION BY HASH({hashCol})
                            PARTITIONS {partitionNum};'''
        cursor2.execute(CREATE_TABLE)
        db2.commit()


        # insert data to table
        cols = "`,`".join([str(i) for i in df.columns.tolist()])

        # Insert DataFrame recrds one by one.
        for i,row in df.iterrows():
            sql = f"INSERT INTO {nameTable} (`" +cols + "`) VALUES (" + "%s,"*(len(row)-1) + "%s)"
            cursor2.execute(sql, tuple(row))

        db2.commit()
        
        #Adding partition details to meta_data
        cursor2.execute(f'EXPLAIN SELECT * FROM {nameTable}')
        result = cursor2.fetchone()
        partitions = result[3]

        UPDATE_META_DATA = f"UPDATE meta_data SET partition_table = '{nameTable}', partitions = '{partitions}' WHERE id = {childID}"
        cursor.execute(UPDATE_META_DATA)
        db.commit()

        return {"flag":1,"desc":"File uploaded successfully"}
            
    elif flag == -1:
        # print("Folder already exists.")
        return {"flag":-1,"desc":"Folder already exists"}
    
    elif flag == -2:
        # print("Invalid path entered.")
        return {"flag":-1,"desc":"Invalid path entered"}
    return flag

# will need to change this to incorporate put command as well 
# E.g may have to add a fourth argument to this function i.e numOfPartitions
def commands_main(cmd, path, partitionNum=None, local=None, hashCol = 'sort_index'):

    db = mysql.connector.connect(host="localhost", user="root", passwd="hazard97", database="namenode")
    db2 = mysql.connector.connect(host="localhost", user="root", passwd="hazard97", database="datanode")

    cursor = db.cursor(buffered=True)
    cursor2 = db2.cursor(buffered=True)

    if cmd == "readPartition":
        df = readPartition(path, partitionNum, cursor, cursor2)
        # print(df)
        return df

    elif cmd == "rm":
        return rm(path, cursor, cursor2, db, db2)

    elif cmd == "getPartitionLocations":
        nameTable,partitions = getPartitionLocations(path, cursor)
        
        #if error, returning error json
        if nameTable == None:
            return partitions

        else:
            json = {"partitions":partitions}
            return json
            
    
    elif cmd == "cat":
        df = cat(path, cursor, cursor2)
        # print(df)
        return df
        # print("View your file below:")
        
    
    elif cmd == "put":
        return put(local,path,cursor,cursor2,db,db2,partitionNum, hashCol)

    elif cmd == "mkdir":
        return makeDir(path, cursor, db)

    elif cmd == "ls":
        json = {"files":ListFiles(path, cursor)}
        return json

    elif cmd == "getFolderID":
        return getFolderID(path, cursor)

    elif cmd == "pathValidatorMakeDir":
        return pathValidatorMakeDir(path, cursor)

    cursor.close()
    cursor2.close()
    db.close()
    db2.close()
    return 



