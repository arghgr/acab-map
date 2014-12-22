import csv
from pymongo import MongoClient
from datetime import datetime
import json
import collections

client = MongoClient()

incidents = client.acab_db.incidents

"INCIDENTS: FIND"

# Lists incidents from all years matching provided description
def search_by_description(description):
    print("description")
    thing = incidents.find({
        'Descript':description
    })
    return thing

# Lists incidents in a given year with description "Danger of leading immoral life" and resolution "Juvenile booked"
def danger(year):
    print("danger")
    thing = incidents.find({
        'Date': {'$gte':datetime(year,1,1),'$lte':datetime(year,12,31)},
        'Descript':'DANGER OF LEADING IMMORAL LIFE',
        'Resolution':'JUVENILE BOOKED'
    })
    return thing

# Lists incident types in a given year, grouped by resolution
def incident_types(year):
    print("incident types")
    thing = incidents.aggregate([
        { '$match' : {'Date': {'$gte':datetime(year,1,1),'$lte':datetime(year,12,31)} } },
        { '$group' : {"_id":"$Resolution", "count":{"$sum":1} } },
        { '$sort' : {'count':-1} }
    ])
    return thing

"INCIDENTS: AGGREGATE"

# Counts incidents in a given year with resolution "Juvenile booked", grouped by description
def juveniles(year):
    print("juveniles")
    thing = incidents.aggregate([
        { '$match' : {'Date': {'$gte':datetime(year,1,1),'$lte':datetime(year,12,31)}, 'Resolution':'JUVENILE BOOKED' } },
        { '$group' : {"_id":"$Descript", "count":{"$sum":1} } },
        { '$sort' : {'count':-1} }
    ])
    return thing

# Counts incidents in a given year, grouped by incident number
def incidentnums(year):
    print("incidentnums")
    thing = incidents.aggregate([
        { '$match' : {'Date': {'$gte':datetime(year,1,1),'$lte':datetime(year,12,31)} } },
        { '$group' : {"_id":"$IncidntNum", "count":{"$sum":1} } },
        { '$sort' : {'count':-1} }
    ])
    return len(thing['result'])

# Counts incidents in a given year, grouped by resolution
def resolution(year):
    print("resolution")
    thing = incidents.aggregate([
        { '$match' : {'Date': {'$gte':datetime(year,1,1),'$lte':datetime(year,12,31)} } },
        { '$group' : {"_id":"$Resolution", "count":{"$sum":1} } },
        { '$sort' : {'count':-1} }
    ])
    return thing

"UTILITY FUNCTIONS"

# Converts dictionary from unicode to string using recursion
def convert(data):
    if isinstance(data, basestring):
        return str(data)
    elif isinstance(data, collections.Mapping):
        return dict(map(convert, data.iteritems()))
    elif isinstance(data, collections.Iterable):
        return type(data)(map(convert, data))
    else:
        return data

# Writes data to text file (if query uses aggregate)
def write_to_file(filename,data):
    print("write_to_file")
    outfile = open('analysis/%s.json' % filename, 'w+')
    outjson = json.dumps(data)
    outfile.write(outjson)
    outfile.close()

# Writes data to text file (if query uses find)
def write_to_file_find(filename,data):
    print("write_to_file_find")
    outfile = open('analysis/%s.json' % filename, 'w+')
    outfile.write(str(data))
    outfile.close()

"RUN FUNCTIONS"

def run_analysis(func, filename, functype):
    print("run_analysis")
    print(filename)
    outdata = {}
    if functype == "find":
        for year in range(2003,2015):
            rawdata = list(globals()[func](year))
            data = convert(rawdata)
            outdata[year] = data
        write_to_file_find(filename, outdata)
    elif functype == "aggregate":
        for year in range(2003,2015):
            outdata[year] = globals()[func](year)['result']
        write_to_file(filename, outdata)

def search_analysis(description, filename):
    print("search_analysis")
    print(filename)
    outdata = []
    rawdata = search_by_description(description)
    for item in rawdata:
        converted_item = convert(item)
        outdata.append(converted_item)
    write_to_file_find(filename, outdata)

# run_analysis("incident_types", "incidents", "aggregate")
# run_analysis("juveniles", "juveniles", "aggregate")
# run_analysis("resolution", "resolution", "aggregate")
# run_analysis("danger", "danger", "find")
# search_analysis("WEARING THE APPAREL OF OPPOSITE SEX TO DECEIVE", "apparel")
search_analysis("FORTUNE TELLING", "fortune")
