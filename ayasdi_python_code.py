#!/usr/bin/python

import random
import string
import time
import logging
import sqlite3
import csv
import traceback

# Constants
FILE_NAME = "ayasdi_assignment.csv"
DB_NAME = "ayasdi_assignment.db"
TABLE_NAME = "ayasdi_assignment"


# set up logger

format = "%(asctime)s %(levelname)s => %(message)s "
logging.basicConfig(format=format, level=logging.INFO)

logger = logging.getLogger("ayasdi_challenge")


# utility functions


def mean():
    ''' returns the mean.
    assumptions : we will use mean from 11 to 19 for col2 to col10 respectively.'''
    while True:
        for i in xrange(11, 20):
            yield i


def standard_deviation():
    ''' returns the standard deviation.
    assumptions: we will use std deviation from 1 to 9 for col2 to col10 respectively'''
    while True:
        for i in xrange(1, 10):
            yield i


def random_string():
    ''' returns a random string of size give in args.
    assumption: size of the string is hardcoded to 4. '''
    letters = string.ascii_letters
    str = ""
    for i in xrange(4):
        str += random.choice(letters)
    return str


def unix_timestamp_date(date, date_format):
    ts = time.mktime(time.strptime(date, date_format))
    return int(ts)


def random_date(start_date, end_date):
    ''' returns a random date between 2 intervals.
    assumption: date_format = %d/%m/%Y'''
    date_format = "%m/%d/%Y"
    start_date_timestatmp = unix_timestamp_date(start_date, date_format)
    end_date_timestatmp = unix_timestamp_date(end_date, date_format)
    # add seconds + start_date_ts
    seconds_elapsed = end_date_timestatmp - start_date_timestatmp
    rand_number = random.randrange(0, seconds_elapsed)
    new_timestamp = start_date_timestatmp + rand_number
    random_date = time.localtime(new_timestamp)
    return time.strftime(date_format, random_date)


# table headers


def col_2_to_10_header():
    ''' returns a concatenated string of headers for col2 to col10. Each header is seperated bt tab char'''
    data = ""
    mean_generator = mean()
    for i in xrange(2, 11):
        mean_value = next(mean_generator)
        header = "col{i}_{m}".format(i=i, m=mean_value)
        data += header + "\t"
    return data


def col_11_to_19_header():
    ''' returns a concatenated string of headers for col11 to col19. Each header is seperated by tab char.'''
    data = ""
    for i in xrange(11, 20):
        header = "col{i}".format(i=i)
        data += header + "\t"
    return data


# column values


def column_2_to_10():
    ''' returns a concatenated string of row value for col 2 to col 10. TODO : achieve 10 percent nulls '''
    data = ""
    mean_generator = mean()
    std_dev_generator = standard_deviation()
    for i in xrange(2, 11):
        mean_value = next(mean_generator)
        std_dev_value = next(std_dev_generator)
        value = random.gauss(mean_value, std_dev_value)
        data += str(value) + "\t"
    return data


def column_11_to_19():
    ''' returns a concatenated string value for col 11 to col 19. '''
    data = ""
    for i in xrange(11, 20):
        value = random_string()
        data += value + "\t"
    return data


def column_20():
    ''' returns a string representing random date between jan 1,2014 and dec 31,2014'''
    start_date = "01/01/2014"
    end_date = "12/31/2014"
    return random_date(start_date, end_date)


# creating the dataset


def create_dataset(file_name):
    logger.info("Creating dataset file : " + str(file_name))
    with open(file_name, "w+") as f:
        # add table headers
        f.write("col1\t")
        f.write(col_2_to_10_header())
        f.write(col_11_to_19_header())
        f.write("col20")
        f.write("\n")
        # add table rows
        for i in range(1, 1000001):
            f.write(str(i) + "\t")
            f.write(column_2_to_10())
            f.write(column_11_to_19())
            f.write(column_20())
            f.write("\n")


# sql lite operations


def create_table(cursor, table_name):
    ''' creates a table in sql-lite. '''
    logger.info("Creating Table Name : " + table_name)
    COL1 = "CREATE TABLE {t} (col1 INT PRIMARY KEY NOT NULL,".format(
        t=table_name)

    # # add col 2_to_10 headers
    COL_2_10 = ""
    entries = filter(None, col_2_to_10_header().split("\t"))
    for i in entries:
        COL_2_10 += i + " REAL,"

    # # add col 11_to_19 headers
    COL_11_19 = ""
    text_entries = filter(None, col_11_to_19_header().split("\t"))
    for j in text_entries:
        COL_11_19 += j + " TEXT,"

    # SQL query
    SQL = "{c1}{c2}{c3}col20 TEXT NOT NULL);".format(
        c1=COL1, c2=COL_2_10, c3=COL_11_19)

    logger.info("create table sql:\n{s}".format(s=SQL))
    cursor.execute(SQL)
    logger.info("Table was Successfully Created.")


def read_chunks(reader, chunk_size=10):
    ''' generator function that returns a chunk of 10000 records from csv file.'''
    l = []
    for x in reader:
        if(chunk_size == 0):
            yield l
        l.append(x)
        chunk_size -= 1


def insert_chunk(cursor, query, data):
    ''' each chunk is denoted as one transaction.'''
    logging.info("inside insert_chunk")
    for row in data:
        logging.info("cursor execute"+str(row))
        cursor.execute(query, tuple(row))

# inserts data in table

def insert_entries_table(cursor, connection,file_name, table_name):
    ''' inserts entries in table,chunk-wise.'''
    logger.info("Insering entries from {f} in {t}".format(
        f=file_name, t=table_name))

    with open(file_name, "r+") as file:
        reader = csv.reader(file, delimiter="\t")
        # read the first line for columns.
        columns = next(reader)
        query = "INSERT into {0}({1}) values({2})"
        format_query = query.format(table_name, ",".join(
            columns), ",".join("?" * len(columns)))
        logger.info("Query:"+format_query)
        # insert the entries in table chunk-wise.
        for chunk in read_chunks(reader):
            cursor.execute("BEGIN TRANSACTION")
            insert_chunk(cursor, format_query, chunk)
            connection.commit()


def main():
    create_dataset(FILE_NAME)
    logger.info("Dataset File was Successfully Created.")
    # insert the dataset in sqllite
    try:
        logger.info("Creating Database Name : " + DB_NAME)
        connection = sqlite3.connect(DB_NAME)
        logger.info("Database was Successfully Created.")
        cursor = connection.cursor()
        create_table(cursor, TABLE_NAME)
        insert_entries_table(cursor, connection, "ayasdi_assignment.csv", "ayasdi_assignment")
    except Exception as e:
        logger.error(
            "Found an Exception when trying to insert data in sqllite. "+e.message)
    finally:
        connection.close()


if __name__ == "__main__":
    logger.info("Running Main ..")
    main()
