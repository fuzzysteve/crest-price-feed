from sqlalchemy import create_engine
import redis
import time
import requests
import datetime
import sys


import logging

if __name__ == "__main__":
    logging.basicConfig(filename='amarrload.log',level=logging.WARN)
    engine = create_engine('mysql://eve:eve@localhost:3306/eve', echo=False)
    result = engine.execute("select typeid from invTypes join invGroups on invTypes.groupid=invGroups.groupid where marketgroupid is not null and categoryid != 350001 and invTypes.published=1 order by typeid asc")
    # build the basic list
    baseitemids=[]
    for row in result:
        baseitemids.append(row[0])
    # Two redis connections. r for pushing in, sub for the pubsub.
    rC = redis.StrictRedis(host='localhost', port=6379, db=0)
    sub = redis.StrictRedis(host='localhost', port=6379, db=0)
    feed = sub.pubsub(ignore_subscribe_messages=True)
    feed.subscribe("buyprices")
    session = requests.Session()
    session.headers.update({'UserAgent':'Fuzzwork Price Getter'});
    itemids=list(baseitemids)
    sleeptimer=0
    while True:
        if len(itemids):
            typeid=itemids.pop(0)
            region=10000043
            what="{}|{}".format(typeid,region)
            logging.debug(what)
            sub.publish("sellprices",what)
            sub.publish("buyprices",what)
            sleeptimer+=1
        else:
            break
        if sleeptimer>20:
            sleeptimer=0
            time.sleep(10)
