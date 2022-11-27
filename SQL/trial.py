import mysql.connector
import pandas as pd
from datetime import datetime



db = mysql.connector.connect(host="localhost", user="root", passwd="HarryPotter7", database="trial")

cursor = db.cursor(buffered=True)

def put(FULL_PATH,db,cursor):

	#create dataframe
	df = pd.read_csv(FULL_PATH)
	df['sort_index'] = range(0,len(df))

	for dtype in df.dtypes:
		print(dtype)


	# print(df)
	return df


df = put('/Users/zainabsoomro/Documents/2022 - fall/551 - data management/project/project-git/data/cars.csv',db,cursor)


# META_DATA_QUERY = f'INSERT INTO meta_data (name, type, partitions, ctime) VALUES ("{FILE_TO_ADD}", "FILE", NULL, "{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}");'
# cursor.execute(META_DATA_QUERY)
# db.commit()
            
