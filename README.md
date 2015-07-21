# iknn
incomplete k nearest neighbor query in postgresql

## How to use?
### 1. Import LPinit.sql

~~~
    \i LAinit.sql
~~~

### 2. Initialize a target table to support iknn query

~~~
    select lpinit([table name]);
~~~
This function automatically does these things:
  1. create a tmp table as lattices. The name of tmp table is [table name]_latmp;
  2. add three columns to the original table: alphavalue, nincomplete and ibitmap, recording the alpha value of the entry, the number of incomplete values and the bitmap of completeness respectively;
  3. build up hash index on ibitmap, that is for buckets;
  4. build up hash index on latticeid of the tmp table, to speed up query for buckets;
  5. set triggers to maintain the three columns and the extra tmp table on insert, update and delete.

### 3. Inport LPwithdraw.sql

~~~
    \i LPwithdraw.sql
~~~

### 4. Withdraw iknn query support

~~~
    select lpwithdraw([table name]);
~~~

This function automatically does these things:
  1. drop the extra three columns of the original table;
  2. drop the tmp table;
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
