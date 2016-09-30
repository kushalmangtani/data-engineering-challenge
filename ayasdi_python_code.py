#!/usr/bin/python

import random
import string
import time
import logging
import sqlite3
import csv
import traceback
import requests

# Constants
FILE_NAME = "ayasdi_assignment.csv"
DB_NAME = "ayasdi_assignment.db"
TABLE_NAME = "ayasdi_assignment_table_30"
DEFAULT_CHUNK_SIZE = 10000
TOTAL_ENTRIES = 1000000
PERCENT_OF_NULLS = 10
WORDS_LIST_URL = "https://svnweb.freebsd.org/csrg/share/dict/words?view=co&content-type=text/plain"

# set up logger

format = "%(asctime)s %(levelname)s => %(message)s "
logging.basicConfig(format=format, level=logging.INFO)
logger = logging.getLogger("ayasdi_challenge")


# utility functions

def no_of_nulls(total_entries=TOTAL_ENTRIES, percent_nulls=PERCENT_OF_NULLS):
    ''' returns the number of null entries allowed in col 2 to 19. '''
    nulls_allowed = total_entries / percent_nulls
    return nulls_allowed


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

def dictionary():
    ''' returns a dictionary with keys being words and values being meanings.
    assumptions : in this application, the meanings are going to be empty strings.'''
    response = requests.get(WORDS_LIST_URL)
    words = response.content.split("\n")
    my_dictionary = {}
    for word in words:
        my_dictionary[word] = ""
    return my_dictionary 
       

def random_string(my_dictionary_keys):
    ''' returns a random string from the dictionary.'''
    while True:
        yield random.choice(my_dictionary_keys)


def unix_timestamp_date(date, date_format):
    ''' unix_timestamp of a date passed in argument.'''
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


def column_2_to_10(iteration_no, no_of_nulls_entries):
    ''' returns a concatenated string of row value for col 2 to col 10. Achieves 10 percent nulls.
    assumptions : empty strings are assumed to be nulls. '''
    data = ""
    mean_generator = mean()
    std_dev_generator = standard_deviation()
    for i in xrange(2, 11):
        # null entry
        if iteration_no % no_of_nulls_entries is 0:
            value = ""
        # gaussian dist.
        else:
            mean_value = next(mean_generator)
            std_dev_value = next(std_dev_generator)
            value = str(random.gauss(mean_value, std_dev_value))
        data += value + "\t"
    return data


def column_11_to_19(my_dictionary_keys,iteration_no, no_of_nulls_entries):
    ''' returns a concatenated string value for col 11 to col 19. Achieves 10 percent nulls.
    assumptions : empty strings are assumed to be nulls. '''
    data = ""
    random_string_generator = random_string(my_dictionary_keys)
    for i in xrange(11, 20):
        # null entry.
        if iteration_no % no_of_nulls_entries is 0:
            value = ""
        # random string.
        else:    
            value = next(random_string_generator)
        data += value + "\t"
    return data


def column_20():
    ''' returns a string representing random date between jan 1,2014 and dec 31,2014'''
    start_date = "01/01/2014"
    end_date = "12/31/2014"
    return random_date(start_date, end_date)


# creating the dataset


def create_dataset(file_name):
    ''' creates the dataset file.'''
    logger.info("Creating dataset file : " + str(file_name))
    logger.info("Creating a dictionary of words.")
    my_dictionary = dictionary()
    logger.info("Dictionary was Successfully Created.")
    with open(file_name, "w+") as f:
        # add table headers
        f.write("col1\t")
        f.write(col_2_to_10_header())
        f.write(col_11_to_19_header())
        f.write("col20")
        f.write("\n")
        # add table rows
        nulls_entries = no_of_nulls()
        for i in range(1, TOTAL_ENTRIES+1):
            f.write(str(i) + "\t")
            f.write(column_2_to_10(i, nulls_entries))
            f.write(column_11_to_19(my_dictionary.keys(),i, nulls_entries))
            f.write(column_20())
            f.write("\n")


# database operations


def create_table(cursor, table_name):
    ''' creates a table.'''
    logger.info("Creating Table Name : " + table_name)
    COL1="CREATE TABLE {t} (col1 INT PRIMARY KEY NOT NULL,".format(
        t=table_name)

    # # add col 2_to_10 headers
    COL_2_10=""
    entries=filter(None, col_2_to_10_header().split("\t"))
    for i in entries:
        COL_2_10 += i + " REAL,"

    # # add col 11_to_19 headers
    COL_11_19=""
    text_entries=filter(None, col_11_to_19_header().split("\t"))
    for j in text_entries:
        COL_11_19 += j + " TEXT,"

    # SQL query
    SQL="{c1}{c2}{c3}col20 TEXT NOT NULL);".format(
        c1=COL1, c2=COL_2_10, c3=COL_11_19)

    logger.info("create table sql:\n{s}".format(s=SQL))
    cursor.execute(SQL)


def read_chunks(reader, chunk_size=DEFAULT_CHUNK_SIZE):
    ''' generator function that returns a chunk of 10000 records from csv file.'''
    l=[]
    logger.info("Chunk Size : " + str(chunk_size))
    for index,line in enumerate(reader):
        if(index % chunk_size == 0 and index != 0):
            yield l
            l=[]
        l.append(line)
    yield l


def insert_chunk(cursor, query, data):
    ''' inserts each chunk in table. '''
    for row in data:
        l = [None if x is '' else x for x in row]
        cursor.execute(query, tuple(l))

# inserts data in table

def insert_entries_table(cursor, connection, file_name, table_name):
    ''' inserts entries in a table. Note: entries are inserted chunk-wise in table.'''
    logger.info("Insering entries from {f} in {t}".format(
        f=file_name, t=table_name))

    with open(file_name, "r+") as file:
        reader=csv.reader(file, delimiter="\t")
        # read the first line for columns.
        columns=next(reader)
        query="INSERT into {0}({1}) values({2})"
        format_query=query.format(table_name, ",".join(
            columns), ",".join("?" * len(columns)))
        logger.info("Query:" + format_query)
        i=1
        # insert the entries in table chunk-wise.
        for chunk in read_chunks(reader):
            logger.info("Inserting chunk no : {i}.".format(i=i))
            cursor.execute("BEGIN TRANSACTION")
            insert_chunk(cursor, format_query, chunk)
            connection.commit()
            i += 1
        connection.commit()
        
def database_connect(database_name):
    ''' returns a database connection.
    database connection is broken in a seperate method for abstraction purposes.
    Using sqlite3 database in this application.'''
    connection = sqlite3.connect(DB_NAME)
    return connection

# main    

def main():
    create_dataset(FILE_NAME)
    logger.info("Dataset File was Successfully Created.")
    # insert the dataset in table.
    try:
        logger.info("Creating Database Name : " + DB_NAME)
        connection = database_connect(DB_NAME)
        logger.info("Database was Successfully Created.")
        cursor=connection.cursor()
        create_table(cursor, TABLE_NAME)
        logger.info("Table was Successfully Created.")
        insert_entries_table(cursor, connection, FILE_NAME, TABLE_NAME)
        connection.commit()
        logger.info("Success => Inserted {n} entries in  {t} ".format(
            n=TOTAL_ENTRIES, t=TABLE_NAME))

    except Exception as e:
        logger.error(
            "Found an Exception when trying to insert data in database. " + e.message)
        traceback.print_exc()
    finally:
        connection.close()


if __name__ == "__main__":
    logger.info("Running Main ..")
    main()
