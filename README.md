# iknn
incomplete k nearest neighbor query in postgresql

## Algorithm Discription
### LP algorithm:
  Please see Mr. Gao's paper: <i><b>IkNN-TFS-Yunjun Gao-20150115</b></i>
### Initialization:
  1. Set an extra table to record lattice-bucket relations. Each Lattice is identified by ncomplete, the number of completed fields of all the objects in it. Each bucket is identified by a bitmap, representing the incomplete state of objects in it. e.g. '1010' represents the 2nd and 4th field of the object is incomplete.
  2. Categorize all objects into buckets. Build a table for each bucket, storing the whole tuple and an extra column for alphavalue. A BTREE index is built on each bucket at the column alphavalue.

### Query
  1. Fetch all bitmaps from lattice-bucket table, order by latticeid (ncomplete);
  2. For each bitmap, fetch all tuples in the corresponding buckets, order by alphavalue. Notice that a BTREE index has been built at the column alphavalue, postgres fetches tuple directly from Btree and doesn't need to sort them. The order by clause thus does not cause extra complexity;
  3. Calculate the qAlpha according to the bitmap, and binary search its position in the fetched tuples;
  4. Search forward and backward with alpha value pruning and partial distance pruning (mentioned in the paper), while maintain a max-heap as the candidate set;
  5. return all the tuples remained in the candidate set.

## How to use?
### 1. Clone and enter my repo (in terminal)
~~~terminal
    git clone git@github.com:Travelinglight/iknn.git
    cd iknn
~~~

### 2. Import LPinit.sql (in postgresql)

~~~sql
    \i pgsql/LAinit.sql
~~~

### 3. Initialize a target table to support iknn query
You may want to use the sample table "test". Import the table before you initialize it:

~~~sql
	CREATE DATABASE iknn;
	\i iknn.sql
~~~

Now initialize the "test" table

~~~sql
    select lpinit('test');
~~~

The lpinit function automatically does these things:

  1. create a tmp table as lattices. The name of tmp table is [table name]_latmp;
  2. add four columns to the original table: lp_id, alphavalue, nincomplete and ibitmap, recording the unique id for lp algorithm, alpha value of the entry, the number of incomplete values and the bitmap of completeness respectively;
  3. build up hash index on ibitmap, that is for buckets deletion;
  4. build up hash index on latticeid of the tmp table, to speed up query for buckets;
  5. create table for each bitmap, representing buckets, with the name lp\_[table name]\_[bitmap].
  6. build up b-tree index on lp\_[table name]\_[bitmap] at column alphavalue, to auto-sort the tuples with alphavalue
  7. set triggers to maintain the three columns and the extra tables on insert, update and delete.

### 4. Make and install iknnLP function (in terminal)
~~~terminal
	cd c
	make
	sudo make install
	cd ..
~~~

### 5. Import iknnLP function (in postgresql)
~~~sql
	\i c/iknnLP.sql
~~~

### 6. Performing iknn query with LP althrothm
~~~sql
	select a, b, c, d, distance from iknnLP('find 3 nearest neighbour of (a0,a1,a2,a3)(31,32,33,34) from test') AS (a int, b int, c int, d int, distance float);
~~~
* a0,a1,a2,a3 are columns in the table _test_.
* 31,32,33,34 are values of the columns respectively.
* the query object must have values for all columns of the query table
* The tuples returned are those considered nearest with the query object.

### 7. Here's the result
~~~sql
 a | b  | c | d  | distance ---+----+---+----+----------   |    |   | 46 |      576   |    |   | 23 |      484   | 17 |   | 35 |      452(3 rows)
~~~

### 8. Inport LPwithdraw.sql

~~~
    \i LPwithdraw.sql
~~~

### 9. Withdraw iknn query support

~~~
    select lpwithdraw([table name]);
~~~

This function automatically does these things:
  1. drop the extra three columns of the original table;
  2. drop all tmp tables;
  3. drop the two triggers that maintains the three columns and the tmp table;
  4. drop the lpinit function

## Q&A
### I cannot create hstore extension when importing LPinit.sql.
  In ubuntu, you need to install the contrib before you create them.

  ~~~
  sudo apt-get install postgresql-contrib-9.4
  ~~~

  or you can install postgresql with those contribs

  ~~~
  sudo apt-get install postgresql postgresql-contrib
  ~~~

### lpinit function doesn't exist after withdraw
  In the withdraw function I dropped the lpinit function. this is convenient for me because this project is not yet finished and I need to update lpinit function frequently. All you have to do is to re-import the LPinit module.

## To-do list
1. improve input, to allow spaces

## Contact us
1. You can get the paper from Mr. Gao: gaoyj@zju.edu.cn
2. The projet is coded by Kingston Chen, feel free to ask any questions: holaelmundokingston@gmail.com