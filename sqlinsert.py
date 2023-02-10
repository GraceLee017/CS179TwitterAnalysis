import csv
import datetime
import json
import os
import time

import mysql.connector

BATCH_SIZE = 1000

db_connection = mysql.connector.connect(user="grace", password="password08")
cursor = db_connection.cursor()
cursor.execute("USE awa;")

cursor.execute("DROP TABLE IF EXISTS ArtistEngagement;")
cursor.execute(
   "CREATE TABLE ArtistEngagement("
   "userid BIGINT,"
   "screen_name VARCHAR(64)," 
   "created_at_year VARCHAR(64),"
   "created_at_month VARCHAR(64),"
   "total_count BIGINT,"
   "engagement DOUBLE"
   ");")

cursor.execute("DROP TABLE IF EXISTS ArtistInfo;")
cursor.execute(
   "CREATE TABLE ArtistInfo("
   "screenName VARCHAR(64)," 
   "handle VARCHAR(64),"
   "URL VARCHAR(128)"
   ");")


def create_indexes():
	for col in ("screen_name", "created_at_year", "created_at_month", "total_count", "engagement"):
		cursor.execute(
			f"CREATE INDEX idx_{col} ON ArtistEngagement({col}) USING BTREE;")

	for col in ("handle", "URL"):
		cursor.execute(
			f"CREATE INDEX idx_{col} ON ArtistInfo({col}) USING BTREE;")
	db_connection.commit()

insert_stmt = "INSERT INTO ArtistEngagement (userid,screen_name,created_at_year,created_at_month,total_count,engagement" \
		") VALUES (%s,%s,%s,%s,%s,%s)"

insert_stmt1 = "INSERT INTO ArtistInfo (screenName,handle,URL" \
		") VALUES (%s,%s,%s)"


def row_to_vals(row: dict) -> tuple:
   return (row["userid"],  
          row["screen_name"],  
           row["created_at_year"], row["created_at_month"],  
           int(row["total_count"]), float(row["engagement"])) 

def row_to_vals1(row: dict) -> tuple:
   return (row["screenName"],  
          row["handle"],  
           row["URL"]) 

with open("engagement.csv", "r") as f:
	csv_rdr = csv.DictReader(f, delimiter="\t")
	if BATCH_SIZE == 1:
		for row in csv_rdr:
			cursor.execute(insert_stmt, row_to_vals(row))
	else:
		batch = []
		for row in csv_rdr:
			batch.append(row_to_vals(row))
			if len(batch) == BATCH_SIZE:
				cursor.executemany(insert_stmt, batch)
				batch.clear()
		if len(batch) > 0:
			cursor.executemany(insert_stmt, batch)
			batch.clear()

with open("artistInfo.csv", "r") as f:
	csv_rdr = csv.DictReader(f, delimiter="\t")
	if BATCH_SIZE == 1:
		for row in csv_rdr:
			cursor.execute(insert_stmt1, row_to_vals1(row))
	else:
		batch = []
		for row in csv_rdr:
			batch.append(row_to_vals1(row))
			if len(batch) == BATCH_SIZE:
				cursor.executemany(insert_stmt1, batch)
				batch.clear()
		if len(batch) > 0:
			cursor.executemany(insert_stmt1, batch)
			batch.clear()

db_connection.commit()

if False:
    create_indexes()

cursor.close()
db_connection.close()
