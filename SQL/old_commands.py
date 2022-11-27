import mysql.connector
import pandas as pd
import streamlit as st 
from datetime import datetime
from sqlalchemy import create_engine
import pymysql
import sys


# creating a view for easier implementation of command
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
            print("Invalid Path")
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
            print("All good. Folder created.")
            
    elif flag == -1:
        print("Folder already exists.")
    
    elif flag == -2:
        print("Invalid path entered.")
    return flag


def cat(FULL_PATH, cursor, cursor2):

    cleansed_path = FULL_PATH.rstrip('/')
    if not cleansed_path:
        print('Root is a special directory.')
        return

    id = getFolderID(cleansed_path, cursor)
    if id == False:
        print("This file does not exist")
        return 

    name = cleansed_path.split('/')[-1]
    cursor.execute(f"SELECT type FROM meta_data WHERE id = {id};")
    type = cursor.fetchone()[0]
    if type == "FOLDER":
        print(f"{name} is a Directory.")
        return

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
    
    return df    
    
    

def getPartitionLocations(FULL_PATH, cursor):

    cleansed_path = FULL_PATH.rstrip('/')
    if not cleansed_path:
        # print('Root is a special directory. No partitions exist for a Directory.')
        return -1 

    id = getFolderID(cleansed_path, cursor=cursor)
    if id == False:
        # print("This file does not exist")
        return -2 

    name = cleansed_path.split('/')[-1]
    cursor.execute(f"SELECT type FROM meta_data WHERE id = {id};")
    type = cursor.fetchone()[0]
    if type == "FOLDER":
        # print(f"{name} is a Directory. No partitions exist for a Directory.")
        return -3 

    cursor.execute(f"SELECT partitions FROM meta_data WHERE id = {id};")
    partitions = cursor.fetchone()[0]
    partitions = partitions.split(',')

    cursor.execute(f"SELECT partition_table FROM meta_data WHERE id = {id};")
    nameTable = cursor.fetchone()[0]
    
    # print(f"Partitions for the file {FULL_PATH} are: {partitions}")
    # return partitions
    return (nameTable,partitions)
    

def rm(FULL_PATH, cursor, cursor2, db, db2):

    id = getFolderID(FULL_PATH, cursor=cursor)
    if id == False:
        print('Invalid path entered.')
        return
    # partitions = getPartitionLocations(FULL_PATH, cursor=cursor)
    # if partitions in [-1, -2, -3]:
    #     partitionErrorOutput(partitions, FULL_PATH)
    #     return 
    
    # Delete from dir_structure table  
    cursor.execute(f"DELETE FROM dir_structure WHERE child = {id};")
    db.commit()

    # Next, delete from meta_data table 
    cursor.execute(f"DELETE FROM meta_data WHERE id = {id};")
    db.commit()
    # Then, delete all the relevant partition tables
    # for partition in partitions:
    #     cursor2.execute(f"DROP TABLE {partition};")
    #     db2.commit()
    print("File removed. Refresh to see changes.")
    return 


def readPartition(FULL_PATH, partition_num, cursor, cursor2):

    # Make sure that the partition number entered is actually the correct one for the file 
    nameTable,partitions = getPartitionLocations(FULL_PATH=FULL_PATH, cursor=cursor)
    if partitions in [-1, -2, -3]:
        partitionErrorOutput(partitions, FULL_PATH)
        return 
    if partition_num not in partitions:
        print("Partition location and file path do not match.")
        return 
    
    print(f"Retreiving data for {FULL_PATH}:")
    cursor2.execute(f"SELECT * FROM {nameTable} PARTITION ({partition_num});")
    cols = list(cursor2.column_names)
    rows = []
    for row in cursor2: 
        r = list(row)
        rows.append(r)
    df = pd.DataFrame(rows, columns=cols)
    df.drop(columns=['sort_index'], inplace=True)
    # print(df.head())  
    return df 


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
            print("File type is not accomodated. Please try again with a csv file")
            return 

        #reading csv and check if local path is correct   
        try:
            df = pd.read_csv(LOCAL_PATH)
        except FileNotFoundError:
            print("Local file not found")
            return 
        except pd.errors.EmptyDataError:
            print("No data in local file")
            return
        except pd.errors.ParserError:
            print("Parse error, please check your file")
            return
        except Exception as e:
            print(f"Error processing your file. \nThe error: {e}")
            return

        #adding an index to allow sorting when reading later
        df['sort_index'] = range(0,len(df))
        print(df.columns)
        print(df['sort_index'])
        print(hashCol)
        if df[hashCol].dtype != int:
            print("Hashing column can only be int. Please try again")
            return

        
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
        engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}"
                               .format(user="root", 
                                     pw="hazard97", 
                                     db="datanode"))

        df.to_sql(nameTable, con = engine, if_exists = 'append', chunksize = 1000,index = False)
        engine.dispose()

        #Adding partition details to meta_data
        cursor2.execute(f'EXPLAIN SELECT * FROM {nameTable}')
        result = cursor2.fetchone()
        partitions = result[3]

        UPDATE_META_DATA = f"UPDATE meta_data SET partition_table = '{nameTable}', partitions = '{partitions}' WHERE id = {childID}"
        cursor.execute(UPDATE_META_DATA)
        db.commit()

        print("All good. File uploaded.")
        
            
    elif flag == -1:
        print("Folder already exists.")
    
    elif flag == -2:
        print("Invalid path entered.")
    return flag

# will need to change this to incorporate put command as well 
# E.g may have to add a fourth argument to this function i.e numOfPartitions
def main(cmd, path, partitionNum=None, local=None, hashCol = None, search=False):


    db = mysql.connector.connect(host="localhost", user="root", passwd="hazard97", database="namenode")
    db2 = mysql.connector.connect(host="localhost", user="root", passwd="hazard97", database="datanode")

    cursor = db.cursor(buffered=True)
    cursor2 = db2.cursor(buffered=True)

    if search == False:
        if cmd == "readPartition":
            df = readPartition(path, partitionNum, cursor, cursor2)
            print(df)

        elif cmd == "rm":
            rm(path, cursor, cursor2, db, db2)

        elif cmd == "getPartitionLocations":
            flag = getPartitionLocations(path, cursor)
            if flag in [-1,-2,-3]:
                partitionErrorOutput(flag, path)
                return
            else:
                nameTable,partitions = flag 
                print(f"File {path} is located in table {nameTable} with partitions:")
                print(f"{partitions}") 

        
        elif cmd == "cat":
            df = cat(path, cursor, cursor2)
            print("View your file below:")
            print(df)
        
        elif cmd == "put":
            # before this if statement, since hashCol was always None, that was what was getting supplied to the put function
            if not hashCol and partitionNum:
                put(local,path,cursor,cursor2,db,db2,partitionNum=partitionNum)   
            elif not hashCol and not partitionNum:
                put(local,path,cursor,cursor2,db,db2) 
            elif not partitionNum and hashCol:
                put(local,path,cursor,cursor2,db,db2,hashCol=hashCol) 
            else:
                put(local,path,cursor,cursor2,db,db2,partitionNum, hashCol)

        elif cmd == "mkdir":
            makeDir(path, cursor, db)

        elif cmd == "ls":
            print(ListFiles(path, cursor))

        elif cmd == "getFolderID":
            return getFolderID(path, cursor)

        elif cmd == "pathValidatorMakeDir":
            return pathValidatorMakeDir(path, cursor)


    elif search == True:

        if cmd == 'getPartitionLocations':
            flag = getPartitionLocations(path, cursor)
            if flag in [-1,-2,-3]:
                partitionErrorOutput(flag, path)
                return
            else:
                nameTable,partitions = flag 
                return nameTable, partitions 

        if cmd == "readPartition":
            df = readPartition(path, partitionNum, cursor, cursor2)
            return df 

                
    cursor.close()
    cursor2.close()
    db.close()
    db2.close()
    return 