from sqlalchemy import create_engine
import redis
import time
import requests
import datetime
import memcache
import sys

import logging
logging.basicConfig(filename='getsell.log',level=logging.INFO,format='%(asctime)s %(levelname)s %(message)s')


def RateLimited(maxPerSecond):
    minInterval = 1.0 / float(maxPerSecond)
    def decorate(func):
        lastTimeCalled = [0.0]
        def rateLimitedFunction(*args,**kargs):
            elapsed = time.clock() - lastTimeCalled[0]
            leftToWait = minInterval - elapsed
            if leftToWait>0:
                time.sleep(leftToWait)
            ret = func(*args,**kargs)
            lastTimeCalled[0] = time.clock()
            return ret
        return rateLimitedFunction
    return decorate




@RateLimited(20)
def getData(redisConnection,requestsConnection,memcacheConnection,typeid,regionid=None):
    if regionid is None:
        regionid=10000002

    if regionid == 10000002:
        regionstr='forge'
    else:
        regionstr=str(regionid)
    url="https://public-crest.eveonline.com/market/{}/orders/sell/?type=https://public-crest.eveonline.com/types/{}/".format(regionid,typeid)
    exceptionCounter=0
    
    while True:
        if exceptionCounter>20:
            return False
        try:
            selldata=requestsConnection.get(url)
        except:
            logging.warn(str(typeid)+" exception :"+str(exceptionCounter))
            exceptionCounter+=1
            time.sleep(exceptionCounter)
            continue
        if selldata.status_code == 200:
            break
        # wait to retry
        logging.warn(str(typeid)+" is failing with "+str(selldata.status_code))
        exceptionCounter+=1
        time.sleep(exceptionCounter)

    data=selldata.json()
    count=data['totalCount']
    numberOfSellItems=0
    sellPrice=dict()
    for order in data['items']:
        sellPrice[order['price']]=sellPrice.get(order['price'],0)+order['volume']
        numberOfSellItems+=order['volume']

    # generate statistics
    if numberOfSellItems:
        prices=sorted(sellPrice.keys())
        fivePercent=max(numberOfSellItems/20,1)
        bought=0
        boughtPrice=0
        while bought<fivePercent:
            fivePercentPrice=prices.pop(0)
            if fivePercent > bought+sellPrice[fivePercentPrice]:
                boughtPrice+=sellPrice[fivePercentPrice]*fivePercentPrice;
                bought+=sellPrice[fivePercentPrice]
            else:
                diff=fivePercent-bought
                boughtPrice+=fivePercentPrice*diff
                bought=fivePercent
        averageSellPrice=boughtPrice/bought
        now=datetime.datetime.utcnow()
        timezone="+00:00"
        timestring=now.strftime("%Y-%m-%dT%H:%M:%S")+timezone
        value="{:0.2f}|{}|{}|{}".format(averageSellPrice,numberOfSellItems,fivePercent,timestring)
        key="{}sell-{}".format(regionstr,typeid)
        redisConnection.set(key, value)
        memcacheConnection.set(key, value)
        logging.info(key)







if __name__ == "__main__":
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
    feed.subscribe("sellprices")
    session = requests.Session()
    session.headers.update({'UserAgent':'Fuzzwork Price Getter'});
    mC=memcache.Client(["127.0.0.1:11211"])
    itemids=list(baseitemids)
    sleeptimer=0
    while True:
        # check for override
        message = feed.get_message()
        if message:
            data=message['data'].split('|')
            typeid=data[0]
            region=int(data[1])
        else:
            if len(itemids):
                typeid=itemids.pop(0)
                region=10000002
            else:
                time.sleep(1)
                sleeptimer+=1
                if sleeptimer==3600:
                    itemids=list(baseitemids)
                    sleeptimer=0
                continue
        getData(rC,session,mC,typeid,region)
