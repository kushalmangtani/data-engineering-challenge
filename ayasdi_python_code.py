#!/usr/bin/python

import random
import string
import time
import logging

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
    for i in xrange(2, 11):
        mean_value = next(mean())
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
    for i in xrange(2, 11):
        mean_value = next(mean())
        std_dev_value = next(standard_deviation())
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
        for i in range(1, 1000000):
            f.write(str(i) + "\t")
            f.write(column_2_to_10())
            f.write(column_11_to_19())
            f.write(column_20())
            f.write("\n")


# inserting the dataset

def main():
    logger.info("Running Main..")
    create_dataset("temp.csv")
    logger.info("Dataset File was Successfully Created.")
    # insert the dataset in sqllite

if __name__ == "__main__":
    print "running main"
    main()
