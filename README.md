# MiniSQLEngine
The objective of the project is to replicate the functioning of a SQL engine.
The types of queries it can process are:
Select all records :
1. Select * from table_name;
2. Aggregate functions: Simple aggregate functions on a single column.
Sum, average, max and min. They will be very trivial given that the data is only
numbers:
Select max(col1) from table1;
3.
Project Columns(could be any number of columns) from one or more tables : Selectcol1, col2 from table_name;
4.
Select/project with distinct from one table : Select distinct(col1), distinct(col2) from
table_name;
5. Select with where from one or more tables : Select col1,col2 from table1,table2 where
col1 = 10 AND col2 = 20;
a. In the where queries, there would be a maximum of one AND/OR operator
with no NOT operators.
b. Relational operators that are to be handled in the assignment, the operators
include "< , >, <=, >=, =".
6. Projection of one or more(including all the columns) from two tables with one join
condition :
a. Select * from table1, table2 where table1.col1=table2.col2;
b. Select col1,col2 from table1,table2 where table1.col1=table2.col2;
c. NO REPETITION OF COLUMNS – THE JOINING COLUMN SHOULD BE
PRINTED ONLY ONCE.

I first read the metadata of the relation and then store the headers of all the columns and their values in a dict.
Then, I parse the query input from the command line classifying the query into different categories of queries. While parsing, I perform error handling as well.
If the query contains multiple tables, I perform joins of the tables.
Then I run the selection condition on the result which could be a single or joined table.
Finally, I project the required columns and the resultant table is output.
