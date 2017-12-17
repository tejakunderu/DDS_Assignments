#!/usr/bin/python2.7
#
# Assignment3 Interface
# Name: Teja Kunderu
#

from pymongo import MongoClient
import os
import sys
import json
import re
import math

def FindBusinessBasedOnCity(cityToSearch, saveLocation1, collection):
    f = open(saveLocation1, 'w')
    cursor = collection.find({"city": re.compile(cityToSearch, re.IGNORECASE)})
    for item in cursor:
        try:
            name = ", ".join(item["name"].upper().split("\n"))
            address = ", ".join(item["full_address"].upper().split("\n"))
            city = item["city"].upper()
            state = item["state"].upper()
            f.write(name + "$" + address + "$" + city + "$" + state + "\n")
        except:
            continue
    f.close()

def FindBusinessBasedOnLocation(categoriesToSearch, myLocation, maxDistance, saveLocation2, collection):
    f = open(saveLocation2, 'w')
    cursor = collection.find({"$and": [{"categories": category} for category in categoriesToSearch]})
    for item in cursor:
        try:
            name = ", ".join(item["name"].upper().split("\n"))
            latitude = item["latitude"]
            longitude = item["longitude"]
            if actualDistance(float(myLocation[0]), float(myLocation[1]), latitude, longitude) <= maxDistance:
                f.write(name + "\n")
        except:
            continue
    # collection.remove({})
    f.close()

def actualDistance(lat2, lon2, lat1, lon1):
    r = 3959
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delPhi = math.radians(lat2 - lat1)
    delLambda = math.radians(lon2 - lon1)
    a = math.sin(delPhi / 2)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(delLambda / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    d = r * c
    return d

