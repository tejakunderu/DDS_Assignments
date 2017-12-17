#!/usr/bin/python2.7
#
# Assignment3 Interface
#

import psycopg2
import os
import sys
from threading import Thread
import time

##################### This needs to changed based on what kind of table we want to sort. ##################
##################### To know how to change this, see Assignment 3 Instructions carefully #################
FIRST_TABLE_NAME = 'ratings'
SECOND_TABLE_NAME = 'movies'
SORT_COLUMN_NAME_FIRST_TABLE = 'rating'
SORT_COLUMN_NAME_SECOND_TABLE = 'MovieId1'
JOIN_COLUMN_NAME_FIRST_TABLE = 'MovieId'
JOIN_COLUMN_NAME_SECOND_TABLE = 'MovieId1'
##########################################################################################################

SORT_PARTITION_NAME = 'sortPartition_'
JOIN_PARTITION_NAME = 'joinPartition_'

# Donot close the connection inside this file i.e. do not perform openconnection.close()
def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):
    numPartitions = 5
    cur = openconnection.cursor()

    def CreateAndSortFirstPartition(partitionName, start, end):
        print "Started sort on " + partitionName

        con = getOpenConnection()
        cur = con.cursor()

        cur.execute("DROP TABLE IF EXISTS " + partitionName)

        cur.execute(
            """CREATE TABLE %s AS
                SELECT *
                FROM %s
                WHERE %s >= %f AND %s <= %f
                ORDER BY %s ASC;
            """ %
            (partitionName,
             InputTable,
             SortingColumnName,
             start,
             SortingColumnName,
             end,
             SortingColumnName)
        )

        cur.close()
        con.commit()
        con.close()

        print "Finished sort on " + partitionName

    def CreateAndSortPartition(partitionName, start, end):
        print "Started sort on " + partitionName

        con = getOpenConnection()
        cur = con.cursor()

        cur.execute("DROP TABLE IF EXISTS " + partitionName)

        cur.execute(
            """CREATE TABLE %s AS
                SELECT *
                FROM %s
                WHERE %s > %f AND %s <= %f
                ORDER BY %s ASC;
            """ %
            (partitionName,
             InputTable,
             SortingColumnName,
             start,
             SortingColumnName,
             end,
             SortingColumnName)
        )

        cur.close()
        con.commit()
        con.close()

        print "Finished sort on " + partitionName

    threads = []

    cur.execute("DROP TABLE IF EXISTS " + OutputTable)

    cur.execute("SELECT MIN(%s), MAX(%s) FROM %s;" % (SortingColumnName, SortingColumnName, InputTable))
    minValue, maxValue = cur.fetchall()[0]

    start = minValue
    r = float(maxValue - minValue) / numPartitions
    end = start + r

    print "Creating partitions"

    partitionName = SORT_PARTITION_NAME + str(0)
    threads.append(Thread(target=CreateAndSortFirstPartition, args=(partitionName, start, end)))

    start = end
    for i in range(1, numPartitions):
        end = start + r
        partitionName = SORT_PARTITION_NAME + str(i)
        threads.append(Thread(target=CreateAndSortPartition, args=(partitionName, start, end)))
        start = end

    map(lambda x: x.start(), threads)
    map(lambda x: x.join(), threads)

    print "Joining partitions"

    cur.execute("CREATE TABLE %s AS SELECT * FROM %s" % (OutputTable, SORT_PARTITION_NAME + str(0)))
    for i in range(1, numPartitions):
        cur.execute("INSERT INTO %s SELECT * FROM %s" % (OutputTable, SORT_PARTITION_NAME + str(i)))

    print "Finished joining"
    print "Removing partitions"

    for i in range(numPartitions):
        cur.execute("DROP TABLE IF EXISTS %s" % SORT_PARTITION_NAME + str(i))

    cur.close()
    openconnection.commit()

    print "Done"


def ParallelJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    numPartitions = 5
    cur = openconnection.cursor()

    def CreateAndJoinFirstPartition(partitionName, start, end):
        print "Started join on " + partitionName

        con = getOpenConnection()
        cur = con.cursor()

        cur.execute("DROP TABLE IF EXISTS " + partitionName)

        cur.execute(
            """CREATE TABLE %s AS 
                SELECT *
                FROM (
                    SELECT *
                    FROM %s 
                    WHERE %s >= %f AND %s <= %f
                ) AS T1 INNER JOIN (
                    SELECT *
                    FROM %s 
                    WHERE %s >= %f AND %s <= %f
                ) AS T2
                ON T1.%s = T2.%s;
            """ %
            (partitionName,
             InputTable1,
             Table1JoinColumn,
             start,
             Table1JoinColumn,
             end,
             InputTable2,
             Table2JoinColumn,
             start,
             Table2JoinColumn,
             end,
             Table1JoinColumn,
             Table2JoinColumn)
        )

        cur.close()
        con.commit()
        con.close()

        print "Finished join on " + partitionName

    def CreateAndJoinPartition(partitionName, start, end):
        print "Started join on " + partitionName

        con = getOpenConnection()
        cur = con.cursor()

        cur.execute("DROP TABLE IF EXISTS " + partitionName)

        cur.execute(
            """CREATE TABLE %s AS 
                SELECT *
                FROM (
                    SELECT *
                    FROM %s 
                    WHERE %s > %f AND %s <= %f
                ) AS T1 INNER JOIN (
                    SELECT *
                    FROM %s 
                    WHERE %s > %f AND %s <= %f
                ) AS T2
                ON T1.%s = T2.%s;
            """ %
            (partitionName,
             InputTable1,
             Table1JoinColumn,
             start,
             Table1JoinColumn,
             end,
             InputTable2,
             Table2JoinColumn,
             start,
             Table2JoinColumn,
             end,
             Table1JoinColumn,
             Table2JoinColumn)
        )

        cur.close()
        con.commit()
        con.close()

        print "Finished join on " + partitionName

    threads = []

    cur.execute("DROP TABLE IF EXISTS " + OutputTable)

    cur.execute("SELECT MIN(%s), MAX(%s) FROM %s;" % (Table1JoinColumn, Table1JoinColumn, InputTable1))
    minValue1, maxValue1 = cur.fetchall()[0]

    cur.execute("SELECT MIN(%s), MAX(%s) FROM %s;" % (Table2JoinColumn, Table2JoinColumn, InputTable2))
    minValue2, maxValue2 = cur.fetchall()[0]

    minValue = max(minValue1, minValue2)
    maxValue = min(maxValue1, maxValue2)

    start = min(minValue1, minValue2)
    r = float(maxValue - minValue) / numPartitions
    end = minValue + r

    print "Creating partitions"

    partitionName = JOIN_PARTITION_NAME + str(0)
    threads.append(Thread(target=CreateAndJoinFirstPartition, args=(partitionName, start, end)))

    start = end
    for i in range(1, numPartitions - 1):
        end = start + r
        partitionName = JOIN_PARTITION_NAME + str(i)
        threads.append(Thread(target=CreateAndJoinPartition, args=(partitionName, start, end)))
        start = end
    end = max(maxValue1, maxValue2)

    partitionName = JOIN_PARTITION_NAME + str(numPartitions - 1)
    threads.append(Thread(target=CreateAndJoinFirstPartition, args=(partitionName, start, end)))

    map(lambda x: x.start(), threads)
    map(lambda x: x.join(), threads)

    print "Joining partitions"

    cur.execute("CREATE TABLE %s AS SELECT * FROM %s" % (OutputTable, JOIN_PARTITION_NAME + str(0)))
    for i in range(1, numPartitions):
        cur.execute("INSERT INTO %s SELECT * FROM %s" % (OutputTable, JOIN_PARTITION_NAME + str(i)))

    print "Finished joining"
    print "Removing partitions"

    for i in range(numPartitions):
        cur.execute("DROP TABLE IF EXISTS %s" % JOIN_PARTITION_NAME + str(i))

    cur.close()
    openconnection.commit()

    print "Done"


################### DO NOT CHANGE ANYTHING BELOW THIS #############################


# Donot change this function
def getOpenConnection(user='postgres', password='1234', dbname='ddsassignment3'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

# Donot change this function
def createDB(dbname='ddsassignment3'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.commit()
    con.close()

# Donot change this function
def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()

# Donot change this function
def saveTable(ratingstablename, fileName, openconnection):
    try:
        cursor = openconnection.cursor()
        cursor.execute("Select * from %s" %(ratingstablename))
        data = cursor.fetchall()
        openFile = open(fileName, "w")
        for row in data:
            for d in row:
                openFile.write(`d`+",")
            openFile.write('\n')
        openFile.close()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()

if __name__ == '__main__':
    try:
	# Creating Database ddsassignment3
	print "Creating Database named as ddsassignment3"
	createDB();
	
	# Getting connection to the database
	print "Getting connection from the ddsassignment3 database"
	con = getOpenConnection();

	# Calling ParallelSort
	print "Performing Parallel Sort"
	ParallelSort(FIRST_TABLE_NAME, SORT_COLUMN_NAME_FIRST_TABLE, 'parallelSortOutputTable', con);

	# Calling ParallelJoin
	print "Performing Parallel Join"
	ParallelJoin(FIRST_TABLE_NAME, SECOND_TABLE_NAME, JOIN_COLUMN_NAME_FIRST_TABLE, JOIN_COLUMN_NAME_SECOND_TABLE, 'parallelJoinOutputTable', con);
	
	# Saving parallelSortOutputTable and parallelJoinOutputTable on two files
	saveTable('parallelSortOutputTable', 'parallelSortOutputTable.txt', con);
	saveTable('parallelJoinOutputTable', 'parallelJoinOutputTable.txt', con);

	# Deleting parallelSortOutputTable and parallelJoinOutputTable
	deleteTables('parallelSortOutputTable', con);
        deleteTables('parallelJoinOutputTable', con);

        if con:
            con.close()

    except Exception as detail:
        print "Something bad has happened!!! This is the error ==> ", detail
