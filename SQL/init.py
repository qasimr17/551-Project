import mysql.connector
import pandas as pd
import streamlit as st 
from datetime import datetime


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
        st.subheader('Root is a special directory. No partitions exist for a Directory.') 
    elif flag == -2:
        st.subheader('This file does not exist.') 
    elif flag == -3:
        name = path.split('/')[-1] 
        st.subheader(f"{name} is a Directory. No partitions exist for a Directory.")
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
        db = mysql.connector.connect(host="localhost", user="root", passwd="HarryPotter7", database="namenode")
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
            st.text("All good. Folder created.")
            
    elif flag == -1:
        st.text("Folder already exists.")
    
    elif flag == -2:
        st.text("Invalid path entered.")
    return


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

    cursor.execute(f"SELECT partitions FROM meta_data WHERE id = {id};")
    partitions = cursor.fetchone()[0]
    partitions = partitions.split(',')

    dataframes = []
    for partition in partitions:
        # df = pd.DataFrame(cursor2.execute(f"SELECT * FROM {partition};"))
        cursor2.execute(f"SELECT * FROM {partition};")
        cols = list(cursor2.column_names)
        rows = []
        for row in cursor2:
            r = list(row)
            rows.append(r)
        df = pd.DataFrame(rows, columns=cols)
        dataframes.append(df)
    total = pd.concat(dataframes)
    total["sort_index"] = pd.to_numeric(total["sort_index"])
    total.sort_values(by=['sort_index'], inplace=True, ignore_index=True)
    total.drop(['sort_index'],axis=1, inplace=True)
    return total 
    

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
    
    # st.text(f"Partitions for the file {FULL_PATH} are: {partitions}")
    # return partitions
    return partitions
    

def rm(FULL_PATH, cursor, cursor2, db, db2):

    id = getFolderID(FULL_PATH, cursor=cursor)
    if id == False:
        st.text('Invalid path entered.')
        return
    partitions = getPartitionLocations(FULL_PATH, cursor=cursor)
    if partitions in [-1, -2, -3]:
        partitionErrorOutput(partitions, FULL_PATH)
        return 
    
    # Delete from dir_structure table  
    cursor.execute(f"DELETE FROM dir_structure WHERE child = {id};")
    db.commit()

    # Next, delete from meta_data table 
    cursor.execute(f"DELETE FROM meta_data WHERE id = {id};")
    db.commit()
    # Then, delete all the relevant partition tables
    for partition in partitions:
        cursor2.execute(f"DROP TABLE {partition};")
        db2.commit()
    st.text("File removed. Refresh to see changes.")
    return 


def readPartition(FULL_PATH, partition_num, cursor, cursor2):

    # Make sure that the partition number entered is actually the correct one for the file 
    partitions = getPartitionLocations(FULL_PATH=FULL_PATH, cursor=cursor)
    if partitions in [-1, -2, -3]:
        partitionErrorOutput(partitions, FULL_PATH)
        return 
    if partition_num not in partitions:
        print("Parition location and file path do not match.")
        return 
    
    print(f"Retreiving data for {FULL_PATH}:")
    cursor2.execute(f"SELECT * FROM {partition_num};")
    cols = list(cursor2.column_names)
    rows = []
    for row in cursor2: 
        r = list(row)
        rows.append(r)
    df = pd.DataFrame(rows, columns=cols)
    df.drop(columns=['sort_index'], inplace=True)
    # print(df.head())  
    return df 

# will need to change this to incorporate put command as well 
# E.g may have to add a fourth argument to this function i.e numOfPartitions
def main(cmd, path, partitionNum=None):

    db = mysql.connector.connect(host="localhost", user="root", passwd="HarryPotter7", database="namenode")
    db2 = mysql.connector.connect(host="localhost", user="root", passwd="HarryPotter7", database="datanode")

    cursor = db.cursor(buffered=True)
    cursor2 = db2.cursor(buffered=True)

    if cmd == "readPartition":
        df = readPartition(path, partitionNum, cursor, cursor2)
        st.dataframe(df)

    elif cmd == "rm":
        rm(path, cursor, cursor2, db, db2)

    elif cmd == "getPartitionLocations":
        flag = getPartitionLocations(path, cursor)
        if flag in [-1,-2,-3]:
            partitionErrorOutput(flag, path)
            return
        else:
            partitions = flag 
            st.subheader(f"Partitions for the file {path} are:")
            st.text(f"{partitions}") 

    
    elif cmd == "cat":
        df = cat(path, cursor, cursor2)
        st.subheader("View your file below:")
        st.dataframe(df)
    
    elif cmd == "mkdir":
        makeDir(path, cursor, db)

    elif cmd == "ls":
        ListFiles(path, cursor)

    elif cmd == "getFolderID":
        return getFolderID(path, cursor)

    elif cmd == "pathValidatorMakeDir":
        return pathValidatorMakeDir(path, cursor)

    cursor.close()
    cursor2.close()
    db.close()
    db2.close()
    return 



# main('mkdir', '/wow')


