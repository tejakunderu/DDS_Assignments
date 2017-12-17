#!/usr/bin/python2.7
#
# Assignment3 Interface
# Name: Satyam Rohit
#

from pymongo import MongoClient
import os
import sys
import json
import Assignment5_Interface as Assignment5

DATABASE_NAME = "ddsassignment5"
COLLECTION_NAME = "businessCollection"
CITY_TO_SEARCH = "TeMpe"
MAX_DISTANCE = 10
CATEGORIES_TO_SEARCH = ["Restaurants"]
MY_LOCATION = ['33.832099900000003', '-111.94094200000001'] #[LATITUDE, LONGITUDE]
SAVE_LOCATION_1 = "findBusinessBasedOnCity.txt"
SAVE_LOCATION_2 = "findBusinessBasedOnLocation.txt"

def loadBusinessTable(fileName, collection):
    try:
        page = open(fileName, "r")
        parsedJson = json.loads(page.read())
        for oneItem in parsedJson["BusinessRecords"]:
            collection.insert(oneItem)
    except Exception as e:
            print "Error: "+str(e)

if __name__ == '__main__':
    try:
        #Getting Connection from MongoDB
        conn = MongoClient('mongodb://localhost:27017/')

        #Creating a New DB in MongoDB
        print "Creating database in MongoDB named as " + DATABASE_NAME
        database   = conn[DATABASE_NAME]

        #Creating a collection named businessCollection in MongoDB
        print "Creating a collection in " + DATABASE_NAME + " named as " + COLLECTION_NAME
        collection = database[COLLECTION_NAME]

        #Loading BusinessCollection from a json file to MongoDB
        print "Loading testData.json file in the " + COLLECTION_NAME + " present inside " + DATABASE_NAME
        loadBusinessTable("testData.json", collection)
    
        #Finding All Business name and address(full_address, city and state) present in CITY_TO_SEARCH
        print "Executing FindBusinessBasedOnCity function"
        # Assignment5.FindBusinessBasedOnCity(CITY_TO_SEARCH, SAVE_LOCATION_1, collection)
        Assignment5.FindBusinessBasedOnCity("Tempe", SAVE_LOCATION_1, collection)
        Assignment5.FindBusinessBasedOnCity("Mesa", SAVE_LOCATION_1, collection)
        Assignment5.FindBusinessBasedOnCity("Tolleson", SAVE_LOCATION_1, collection)
        Assignment5.FindBusinessBasedOnCity("Wickenburg", SAVE_LOCATION_1, collection)
        Assignment5.FindBusinessBasedOnCity("Peoria", SAVE_LOCATION_1, collection)

        #Finding All Business name and address(full_address, city and state) present in radius of MY_LOCATION for CATEGORIES_TO_SEARCH
        print "Executing FindBusinessBasedOnLocation function"
        #Assignment5.FindBusinessBasedOnLocation(CATEGORIES_TO_SEARCH, MY_LOCATION, MAX_DISTANCE, SAVE_LOCATION_2, collection)
        Assignment5.FindBusinessBasedOnLocation(["Shopping"], ["33.42315", "-111.549409"], 10, SAVE_LOCATION_2, collection)
        # Assignment5.FindBusinessBasedOnLocation(["Restaurants"], ["33.832099900000003", "-111.94094200000001"], 10, SAVE_LOCATION_2,
        #                             collection)
        # Assignment5.FindBusinessBasedOnLocation(["Bars"], ["33.416263999999998", "-111.78111199999999"], 10, SAVE_LOCATION_2,
        #                             collection)
        # Assignment5.FindBusinessBasedOnLocation(["Food"], ["33.571163200000001", "-112.1065486"], 10, SAVE_LOCATION_2, collection)
        # Assignment5.FindBusinessBasedOnLocation(["Shopping"], ["33.5823587", "-111.927037"], 10, SAVE_LOCATION_2, collection)

        collection.remove({})

    except Exception as detail:
        print "Something bad has happened!!! This is the error ==> ", detail
