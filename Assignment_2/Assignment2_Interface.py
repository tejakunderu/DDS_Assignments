#!/usr/bin/python2.7
#
# Assignment2 Interface
#

import psycopg2
import os
import sys

# Do not close the connection inside this file i.e. do not perform openconnection.close()
def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):
    cur = openconnection.cursor()

    def getTables(partitionType):
        metadataName = partitionType + ratingsTableName.title() + 'Metadata'
        partitionName = partitionType + ratingsTableName.title() + 'Part'

        cur.execute("SELECT * from %s;" % metadataName)
        rows = cur.fetchall()

        if partitionType == 'Range':
            tableNames = []
            for partitionNum, partitionMin, partitionMax in rows:
                if (ratingMinValue <= partitionMin and ratingMaxValue >= partitionMax) or (
                        partitionMin <= ratingMinValue <= partitionMax) or (
                        partitionMin <= ratingMaxValue <= partitionMax):
                    tableNames.append(partitionName + str(partitionNum))
            return tableNames

        elif partitionType == 'RoundRobin':
            numPartitions = rows[0][0]
            return [(partitionName + str(i)) for i in range(numPartitions)]

    rangeNames = getTables('Range')
    roundRobinNames = getTables('RoundRobin')

    f = open("RangeQueryOut.txt", 'w')

    for partitionName in rangeNames + roundRobinNames:
        cur.execute(("SELECT * FROM %s WHERE Rating BETWEEN %f AND %f;") % (partitionName, ratingMinValue, ratingMaxValue))
        for userId, movieId, rating in cur.fetchall():
            f.write(','.join([partitionName, str(userId), str(movieId), str(rating)]) + "\n")

    f.close()
    cur.close()

def PointQuery(ratingsTableName, ratingValue, openconnection):
    cur = openconnection.cursor()

    def getTables(partitionType):
        metadataName = partitionType + ratingsTableName.title() + 'Metadata'
        partitionName = partitionType + ratingsTableName.title() + 'Part'

        cur.execute("SELECT * from %s;" % metadataName)
        rows = cur.fetchall()

        if partitionType == 'Range':
            for partitionNum, partitionMin, partitionMax in rows:
                if partitionMin <= ratingValue <= partitionMax:
                    return [partitionName + str(partitionNum)]
        elif partitionType == 'RoundRobin':
            numPartitions = rows[0][0]
            return [(partitionName + str(i)) for i in range(numPartitions)]

    rangeNames = getTables('Range')
    roundRobinNames = getTables('RoundRobin')

    f = open("PointQueryOut.txt", 'w')

    for partitionName in rangeNames + roundRobinNames:
        cur.execute(
            ("SELECT * FROM %s WHERE Rating = %f;") % (partitionName, ratingValue))
        for userId, movieId, rating in cur.fetchall():
            f.write(','.join([partitionName, str(userId), str(movieId), str(rating)]) + "\n")

    f.close()
    cur.close()
