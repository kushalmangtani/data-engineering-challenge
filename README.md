problem-defination
==================

Produce a python module (ayasdi_python_code.py) which does the following:  

Create a tab-delimited file (ayasdi_assignment.csv) containing 20 columns and a million rows  with the following characteristics:  

1. Column 1 (labeled as col1 is the index column where the values are 1 to 1 million)  

2. The next 9 columns (2 to 10) are labelled col2_x ... col10_x where each contains random values and 'x' is the mean mentioned in the next sentence. Each column has random data generated from a gaussian distribution at different means and variances. 
Additionally, each of these columns have 10% nulls.  

3. Columns 11 to 19 are labelled as col11...col19, where each column has random strings selected from the English Dictionary. 10% nulls in this column as well.  

4. Column 20 has random dates selected between January 1, 2014 to December 31, 2014. 
No nulls in this column.  

Once this dataset has been created, load it into a single table in a sqlite database (ayasdi_assignment.db).  


Requirements
------------

python >= 2.7  
sqllite
requests
pytest

Run
---

```python ayasdi_python_code.py```


Output
-------

It should generate a ayasdi_assignment.csv in current dir and add it to ayasdi_assignment_table in ayasdi_assignment.db.

Tested only with sqlite.

Verify
-----

The ayasdi_assignment.csv file takes around 2 mins to generate. I have used generators and inserted rows in table chunk-wise to improve performance.  
Also, I have used logger in the python module to indicate any INFO,ERROR messages in module.

Use ```wc -l ayasdi_assignment.csv``` to ensure all the lines were successfully written in the file.

TODO
----

Some of the things that I would like to add:  
1.  Creating file,Inserting in database should be seperated in 2 different files.  
2.  I wanted to do this using python generic operation and without using any additional libs.We can also use pandas package.   
3.  Reduce the creating file operation time by removing string concatenations or using multithreading.  






