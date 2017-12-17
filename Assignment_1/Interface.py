#!/usr/bin/python2.7
#
# Interface for the assignement
#

import psycopg2

DATABASE_NAME = 'dds_assgn1'


def getopenconnection(user='postgres', password='1234', dbname='dds_assgn1'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadratings(ratingstablename, ratingsfilepath, openconnection):
    openconnection.autocommit = True
    cur = openconnection.cursor()
    cur.execute(
        """CREATE TABLE %s (
            userid INTEGER,
            space1 CHAR,
            movieid INTEGER,
            space2 CHAR,
            rating REAL,
            space3 CHAR,
            timestamp INTEGER)
        """ % ratingstablename
    )
    dataset = open(r'%s' % ratingsfilepath, 'r')
    cur.copy_from(dataset, ratingstablename, sep=':')
    dataset.close()
    cur.execute(
        """ALTER TABLE %s 
            DROP COLUMN space1, 
            DROP COLUMN space2, 
            DROP COLUMN space3, 
            DROP COLUMN timestamp
        """ % ratingstablename
    )
    cur.close()


def rangepartition(ratingstablename, numberofpartitions, openconnection):
    if numberofpartitions == 0 or not isValidNumber(numberofpartitions):
        return
    cur = openconnection.cursor()
    start = 0
    r = 5.0 / numberofpartitions
    end = start + r
    table_name = 'range_part' + str(0)
    cur.execute(
        """CREATE TABLE %s AS 
            SELECT * 
            FROM %s 
            WHERE rating >= %f AND rating <= %f
        """ % (table_name, ratingstablename, start, end)
    )
    start = end
    for i in range(1, numberofpartitions):
        end = start + r
        table_name = 'range_part' + str(i)
        cur.execute(
            """CREATE TABLE %s AS 
                SELECT * 
                FROM %s 
                WHERE rating > %f AND rating <= %f
            """ % (table_name, ratingstablename, start, end)
        )
        start = end
    cur.close()


def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    if numberofpartitions == 0 or not isValidNumber(numberofpartitions):
        return
    cur = openconnection.cursor()
    cur.execute("""ALTER TABLE %s ADD COLUMN id SERIAL""" % ratingstablename)
    for i in range(numberofpartitions):
        table_name = 'rrobin_part' + str(i)
        command = 'CREATE TABLE ' + table_name
        command += ' AS SELECT * FROM ' + ratingstablename + ' where (id - 1) % ' + str(numberofpartitions) + ' = ' + str(i)
        cur.execute(command)
        cur.execute("""ALTER TABLE %s DROP COLUMN id""" % table_name)
    cur.execute("""ALTER TABLE %s DROP COLUMN id""" % ratingstablename)
    cur.close()


def isValidNumber(num):
    if num < 0 or (not isinstance(num, int) and not num.is_integer()):
        return False
    return True


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    if not isValidNumber(userid) or not isValidNumber(itemid) or rating < 0:
        return
    cur = openconnection.cursor()
    cur.execute(
        """INSERT INTO %s (userid, movieid, rating) VALUES (%d, %d, %f)""" % (ratingstablename, userid, itemid, rating))
    cur.execute(
        """SELECT table_name
           FROM information_schema.tables
           WHERE table_schema = 'public' and table_name like 'rrobin_part_'
        """
    )
    count = []
    for row in cur.fetchall():
        table_name = row[0]
        cur.execute("""SELECT COUNT(*) FROM %s""" % table_name)
        count.append((table_name, int(cur.fetchone()[0])))
    count.sort(key=lambda x: x[1])
    cur.execute(
        """INSERT INTO %s (userid, movieid, rating) VALUES (%d, %d, %f)""" % (count[0][0], userid, itemid, rating))
    cur.close()


def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    if not isValidNumber(userid) or not isValidNumber(itemid) or rating < 0:
        return
    cur = openconnection.cursor()
    cur.execute(
        """INSERT INTO %s (userid, movieid, rating) VALUES (%d, %d, %f)""" % (ratingstablename, userid, itemid, rating))
    cur.execute(
         """SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = 'public' and table_name like 'range_part_'
         """
    )
    numberofpartitions = cur.fetchone()[0]
    start = 0
    r = 5.0 / numberofpartitions
    end = start + r
    if start <= rating <= end:
        table_num = 0
    else:
        start = end
        for i in range(1, numberofpartitions):
            end = start + r
            if start < rating <= end:
                table_num = i
                break
            start = end
    table_name = 'range_part' + str(table_num)
    cur.execute(
        """INSERT INTO %s (userid, movieid, rating) VALUES (%d, %d, %f)""" % (table_name, userid, itemid, rating))
    cur.close()


def create_db(dbname):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getopenconnection(dbname='postgres')
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
    con.close()


# Middleware
def before_db_creation_middleware():
    # Use it if you want to
    pass


def after_db_creation_middleware(databasename):
    # Use it if you want to
    pass


def before_test_script_starts_middleware(openconnection, databasename):
    # Use it if you want to
    pass


def after_test_script_ends_middleware(openconnection, databasename):
    # Use it if you want to
    pass


def deletepartitionsandexit(openconnection):
    cur = openconnection.cursor()
    cur.execute(
         """SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
         """
    )
    for row in cur.fetchall():
        cur.execute("""DROP TABLE %s""" % row[0])
    cur.close()


if __name__ == '__main__':
    try:

        # Use this function to do any set up before creating the DB, if any
        before_db_creation_middleware()

        create_db(DATABASE_NAME)

        # Use this function to do any set up after creating the DB, if any
        after_db_creation_middleware(DATABASE_NAME)

        with getopenconnection('postgres', '1234', DATABASE_NAME) as con:
            # Use this function to do any set up before I starting calling your functions to test, if you want to
            before_test_script_starts_middleware(con, DATABASE_NAME)

            # Here is where I will start calling your functions to test them. For example,
            loadratings('ratings', 'ratings.dat', con)

            rangepartition('ratings', 5, con)

            roundrobinpartition('ratings', 5, con)

            roundrobininsert('ratings', 21, 1, 0.995, con)

            rangeinsert('ratings', 21, 1, 2.995, con)

            deletepartitionsandexit(con)
            # ###################################################################################
            # Anything in this area will not be executed as I will call your functions directly
            # so please add whatever code you want to add in main, in the middleware functions provided "only"
            # ###################################################################################

            # Use this function to do any set up after I finish testing, if you want to
            after_test_script_ends_middleware(con, DATABASE_NAME)

    except Exception as detail:
        print "OOPS! This is the error ==> ", detail
