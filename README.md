Objective
---------

The objective behind doing this coding challenge (problem defination below) and opensourcing it:  
1.  To demonstrate my problem solving skills.  
2.  To demonstrate my python coding skills.  
3.  To demonstrate my data engineering skills. This program initially took 15+ mins to generate. I was quickly able to optimize it using generators.  

Currently, this program takes approx 2 mins to generate the dataset file and insert it in the database. 7x performance improvement.  

problem-defination
------------------

Produce a python module  which does the following:  

Create a tab-delimited file (data.csv) containing 20 columns and a million rows  with the following characteristics:  

1. Column 1 (labeled as col1 is the index column where the values are 1 to 1 million)  

2. The next 9 columns (2 to 10) are labelled col2_x ... col10_x where each contains random values and 'x' is the mean mentioned in the next sentence. Each column has random data generated from a gaussian distribution at different means and variances. 
Additionally, each of these columns have 10% nulls.  

3. Columns 11 to 19 are labelled as col11...col19, where each column has random strings selected from the English Dictionary. 10% nulls in this column as well.  

4. Column 20 has random dates selected between January 1, 2014 to December 31, 2014. 
No nulls in this column.  

Once this dataset has been created, load it into a single table in a sqlite database (dataset.db).  


Requirements
------------

python >= 2.7  
sqllite  
requests

Run
---

```python main.py```


Output
-------

It should generate a "data.csv" in current dir, create a "gaussian_dist_table" in "dataset.db" in sqllite. The table will have all the entries of the csv file.  
Tested only with sqlite.

Verify
-----

1.  Use ```wc -l data.csv``` to ensure all the lines were successfully written in the file.
2.  Go to sqlite prompt and ensure table has all the entries and all requirements are satisfied.

Note: The py script has some rudimentary tests where it runs some sql queries on the table generated. They verify no of entries in data, no of nulls in columns and dataset values. 

TODO
----

Some of the things that I would like to add in the future:  
1.  Creating file,inserting in database should be seperated in 2 different files.  
2.  I wanted to do this using python generic operations and without using any additional libs.We can also use pandas package.  
3.  Creating multiple files in parallel using multithreading and then reducing them to one "data.csv" file.




