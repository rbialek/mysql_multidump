# mysql_multidump
Created by Robert Bialek rbialek@gmail.com, 2017-11-17

A utility to dump multiple database tables, in parallel, to distinct files without a global lock 
using mysql and mysqldump tools.

The goal is to solve the challenge of dumping large databases from Amazon RDS/Aurora databases. 
The main applications are Ruby On Rails databases, where tables usually have a primary key :id.

The tool runs in 3 steps:

1. Take a list of all databases.
2. Fetch last primary key for each table.
3. Dump tables one by one using the last primary key as a limiter.


Installation
---
multidump requires functional mysql and mysqldump, as well as:

gem install parallel 

To run the dump call:
ruby multidump.rb db_name [options]

Then you can restore the files 1-by one. 
#  TODO - we will add a multirestore.rb script
ruby multirestore.rb db_name [options]

Limitations
---

The DB limit is applied only to tables with a single primary key.
Other tables are copied without limit   

TODO
---
* add DB locking when taking indexes
* add multirestore.rb

