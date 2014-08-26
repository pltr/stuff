#Tracing Partial Dumper for MySQL
=================================

A script for partially dumping data from mysql database
It starts at a particular table/row and then traces all rows
referring to it (through Foreign Key) and dumps them too.

    python setup.py install
    mysql_tpdump -h