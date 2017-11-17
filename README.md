# mysql_multidump
A utility to dump multiple database tables, in parallel, to distinct files without a global lock.

The goal is to solve the challenge of dumping large databases from Amazon RDS/Aurora databases. 

The main applications are Ruby On Rails databases, where tables usually have a primary key :id.

The tool runs in 3 steps:

1. Take a list of all databases.
2. Fetch last primary key for each table.
3. Dump tables one by one using the last primary key as a limiter.

Design 
---

We assume that 


Installation
---
gem install parallel optparse

ruby multidump.rb db_name [options] 
ruby multirestore.rb db_name [options]

Limitations
---

The DB limit is applied only to tables with a single primary key.
Other tables are copied without limit   

TODO
---
* add DB locking when taking indexes
* add multirestore.rb

