import csv
import re
import os
import time
import pymongo
from pymongo import MongoClient

part_size = 500

while True:
    try:
        client = MongoClient("mongodb://localhost:27017")
        database = client.result_db
        collecction = database.result_coll

        start = time.time()
        for name in os.listdir('ZNODdata'):
            year = re.findall(r'Odata(\d{4})File.csv', name)
            if year:
                with open(os.path.join('ZNOData', name), encoding='cp1251') as csvfile:
                    pointer = csv.reader(csvfile, delimiter=';')
                    next(pointer)
                    number = 0
                    part = list()
                    res = collecction.find_one({'year' : year[0]}, sort=[('data', -1)])
                    if res:
                        if 'data' not in res:
                            continue
                        for i in range(res['data'] + 1):
                            next(pointer)
                            number += 1

                    for row in pointer:
                        part.append(dict(zip(['data', 'year', '_id', 'Birth', 'SEXTYPENAME', 'REGNAME', 'AREANAME', 'TERNAME', 'REGTYPENAME', 'TerTypeName', 'ClassProfileNAME', 'ClassLangName', 'EONAME', 'EOTYPENAME', 'EORegName', 'EOAreaName', 'EOTerName', 'EOParent', 'UkrTest', 'UkrTestStatus', 'UkrBall100', 'UkrBall12', 'UkrBall', 'UkrAdaptScale', 'UkrPTName', 'UkrPTRegName', 'UkrPTAreaName', 'UkrPTTerName', 'histTest', 'HistLang', 'histTestStatus', 'histBall100', 'histBall12', 'histBall', 'histPTName', 'histPTRegName', 'histPTAreaName', 'histPTTerName', 'mathTest', 'mathLang', 'mathTestStatus', 'mathBall100', 'mathBall12', 'mathBall', 'mathPTName', 'mathPTRegName', 'mathPTAreaName', 'mathPTTerName', 'physTest', 'physLang', 'physTestStatus', 'physBall100', 'physBall12', 'physBall', 'physPTName', 'physPTRegName', 'physPTAreaName', 'physPTTerName', 'chemTest', 'chemLang', 'chemTestStatus', 'chemBall100', 'chemBall12', 'chemBall', 'chemPTName', 'chemPTRegName', 'chemPTAreaName', 'chemPTTerName', 'bioTest', 'bioLang', 'bioTestStatus', 'bioBall100', 'bioBall12', 'bioBall', 'bioPTName', 'bioPTRegName', 'bioPTAreaName', 'bioPTTerName', 'geoTest', 'geoLang', 'geoTestStatus', 'geoBall100', 'geoBall12', 'geoBall', 'geoPTName', 'geoPTRegName', 'geoPTAreaName', 'geoPTTerName', 'engTest', 'engTestStatus', 'engBall100', 'engBall12', 'engDPALevel', 'engBall', 'engPTName', 'engPTRegName', 'engPTAreaName', 'engPTTerName', 'fraTest', 'fraTestStatus', 'fraBall100', 'fraBall12', 'fraDPALevel', 'fraBall', 'fraPTName', 'fraPTRegName', 'fraPTAreaName', 'fraPTTerName', 'deuTest', 'deuTestStatus', 'deuBall100', 'deuBall12', 'deuDPALevel', 'deuBall', 'deuPTName', 'deuPTRegName', 'deuPTAreaName', 'deuPTTerName', 'spaTest', 'spaTestStatus', 'spaBall100', 'spaBall12', 'spaDPALevel', 'spaBall', 'spaPTName', 'spaPTRegName', 'spaPTAreaName', 'spaPTTerName']
, [number] + year + row)))
                        number += 1

                        if not number % part_size:
                            collecction.insert_many(part)
                            part = list()
                    if part:
                        collecction.insert_many(part)
                        collecction.update_many({}, {'$unset': {'data': 1}})
                        part = list()
                        
        result_time = time.time() - start
        with open('result_time.txt', 'w') as final_time:
            final_time.write(f'Result time: {result_time}')

        select = [{"$match": {"physTestStatus": 'Зараховано'}},{"$group": {"_id": {"region": "$REGNAME", "zno_year": "$year"}, "min": {"$min": "$physBall"}}}]
        res = list(collecction.aggregate(select))
        with open("data.txt", "w") as data:
            for item in res:
                data.write("%s\n" % item)
        break
