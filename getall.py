from sqlalchemy import create_engine, Column, MetaData, Table, Index
from sqlalchemy import Integer, String, Text, Float, Boolean, BigInteger, Numeric, SmallInteger
import time
import requests
from requests_futures.sessions import FuturesSession
import requests_futures
from concurrent.futures import as_completed
import datetime
import csv
import time
import sys
from math import floor,log10,fabs

import logging
logging.basicConfig(filename='getall.log',level=logging.INFO,format='%(asctime)s %(levelname)s %(message)s')



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


    
    
def processData(result,connection,orderTable):
    
    try:
        resp=result.result()
        logging.warn('Process {} {} {}'.format(resp.status_code,result.url,result.retry))
        if resp.status_code==200:
            orders=resp.json()
            for order in orders['items']:
                connection.execute(orderTable.insert(),
                                    orderID=order['id'],
                                    typeID=order['type'],
                                    buy=order['buy'],
                                    volume=order['volume'],
                                    minVolume=order['minVolume'],
                                    price=order['price'],
                                    roundedPrice=round(order['price'],2-int(floor(log10(fabs(order['price']))))),
                                    location=order['stationID'],
                                )
            logging.warn('{}  another page. {}'.format(result.url,orders.get('next',{}).get('href',None)))
            return {'retry':0,'url':orders.get('next',{}).get('href',None)}
        else:
            logging.warn("Non 200 status. {} returned {}".format(resp.url,resp.status_code))
            return {'retry':result.retry+1,'url':result.url}
    except requests.exceptions.ConnectionError as e:
        logging.warn(e)
        return {'retry':result.retry+1,'url':result.url}
    
    
    
import pprint
    


@RateLimited(150)
def getData(requestsConnection,url,retry):
    future=requestsConnection.get(url)
    future.url=url
    future.retry=retry
    return future


if __name__ == "__main__":
    engine = create_engine('sqlite+pysqlite:///market.db', echo=False)
    metadata = MetaData()
    connection = engine.connect()
    

    reqs_num_workers = 10
    session = FuturesSession(max_workers=reqs_num_workers)
    session.headers.update({'UserAgent':'Fuzzwork Market grabber'});
    orderTable = Table('orders',metadata,
                            Column('id',Integer,primary_key=True, autoincrement=True),
                            Column('orderID',BigInteger, primary_key=False,autoincrement=False),
                            Column('typeID',Integer),
                            Column('buy',Integer),
                            Column('volume',BigInteger),
                            Column('minVolume',BigInteger),
                            Column('price',Numeric(scale=4,precision=19)),
                            Column('roundedPrice',Numeric(scale=4,precision=19)),
                            Column('location',Integer),
                            Column('locationName',String)
                            )
                            
    Index("orders_1",orderTable.c.typeID)
    Index("orders_2",orderTable.c.typeID,orderTable.c.buy)
    Index("orders_4",orderTable.c.typeID,orderTable.c.buy,orderTable.c.location)
    Index("orders_3",orderTable.c.typeID,orderTable.c.buy,orderTable.c.price)
    Index("orders_5",orderTable.c.typeID,orderTable.c.buy,orderTable.c.roundedPrice)
 
    metadata.drop_all(engine,checkfirst=True)
    metadata.create_all(engine,checkfirst=True)
    connection.execute(orderTable.delete())
    
    urls=[]

    urls.append({'url':"https://crest-tq.eveonline.com/market/10000001/orders/all/",'retry':0})
    urls.append({'url':"https://crest-tq.eveonline.com/market/10000002/orders/all/",'retry':0})
    urls.append({'url':"https://crest-tq.eveonline.com/market/10000003/orders/all/",'retry':0})
    urls.append({'url':"https://crest-tq.eveonline.com/market/10000004/orders/all/",'retry':0})
    urls.append({'url':"https://crest-tq.eveonline.com/market/10000005/orders/all/",'retry':0})
    urls.append({'url':"https://crest-tq.eveonline.com/market/10000006/orders/all/",'retry':0})
    urls.append({'url':"https://crest-tq.eveonline.com/market/10000007/orders/all/",'retry':0})
    urls.append({'url':"https://crest-tq.eveonline.com/market/10000008/orders/all/",'retry':0})
    urls.append({'url':"https://crest-tq.eveonline.com/market/10000009/orders/all/",'retry':0})
    urls.append({'url':"https://crest-tq.eveonline.com/market/10000010/orders/all/",'retry':0})
    urls.append({'url':"https://crest-tq.eveonline.com/market/10000011/orders/all/",'retry':0})
    urls.append({'url':"https://crest-tq.eveonline.com/market/10000012/orders/all/",'retry':0})
    urls.append({'url':"https://crest-tq.eveonline.com/market/10000013/orders/all/",'retry':0})
    urls.append({'url':"https://crest-tq.eveonline.com/market/10000014/orders/all/",'retry':0})
    urls.append({'url':"https://crest-tq.eveonline.com/market/10000015/orders/all/",'retry':0})
    urls.append({'url':"https://crest-tq.eveonline.com/market/10000016/orders/all/",'retry':0})
    urls.append({'url':"https://crest-tq.eveonline.com/market/10000017/orders/all/",'retry':0})
    urls.append({'url':"https://crest-tq.eveonline.com/market/10000018/orders/all/",'retry':0})
    urls.append({'url':"https://crest-tq.eveonline.com/market/10000019/orders/all/",'retry':0})
    urls.append({'url':"https://crest-tq.eveonline.com/market/10000020/orders/all/",'retry':0})
    urls.append({'url':"https://crest-tq.eveonline.com/market/10000021/orders/all/",'retry':0})
    urls.append({'url':"https://crest-tq.eveonline.com/market/10000022/orders/all/",'retry':0})
    urls.append({'url':"https://crest-tq.eveonline.com/market/10000023/orders/all/",'retry':0})
    urls.append({'url':"https://crest-tq.eveonline.com/market/10000024/orders/all/",'retry':0})
    urls.append({'url':"https://crest-tq.eveonline.com/market/10000025/orders/all/",'retry':0})
    urls.append({'url':"https://crest-tq.eveonline.com/market/10000026/orders/all/",'retry':0})
    urls.append({'url':"https://crest-tq.eveonline.com/market/10000027/orders/all/",'retry':0})
    urls.append({'url':"https://crest-tq.eveonline.com/market/10000028/orders/all/",'retry':0})
    urls.append({'url':"https://crest-tq.eveonline.com/market/10000029/orders/all/",'retry':0})
    urls.append({'url':"https://crest-tq.eveonline.com/market/10000030/orders/all/",'retry':0})
    urls.append({'url':"https://crest-tq.eveonline.com/market/10000031/orders/all/",'retry':0})
    urls.append({'url':"https://crest-tq.eveonline.com/market/10000032/orders/all/",'retry':0})
    urls.append({'url':"https://crest-tq.eveonline.com/market/10000033/orders/all/",'retry':0})
    urls.append({'url':"https://crest-tq.eveonline.com/market/10000034/orders/all/",'retry':0})
    urls.append({'url':"https://crest-tq.eveonline.com/market/10000035/orders/all/",'retry':0})
    urls.append({'url':"https://crest-tq.eveonline.com/market/10000036/orders/all/",'retry':0})
    urls.append({'url':"https://crest-tq.eveonline.com/market/10000037/orders/all/",'retry':0})
    urls.append({'url':"https://crest-tq.eveonline.com/market/10000038/orders/all/",'retry':0})
    urls.append({'url':"https://crest-tq.eveonline.com/market/10000039/orders/all/",'retry':0})
    urls.append({'url':"https://crest-tq.eveonline.com/market/10000040/orders/all/",'retry':0})
    urls.append({'url':"https://crest-tq.eveonline.com/market/10000041/orders/all/",'retry':0})
    urls.append({'url':"https://crest-tq.eveonline.com/market/10000042/orders/all/",'retry':0})
    urls.append({'url':"https://crest-tq.eveonline.com/market/10000043/orders/all/",'retry':0})
    urls.append({'url':"https://crest-tq.eveonline.com/market/10000044/orders/all/",'retry':0})
    urls.append({'url':"https://crest-tq.eveonline.com/market/10000045/orders/all/",'retry':0})
    urls.append({'url':"https://crest-tq.eveonline.com/market/10000046/orders/all/",'retry':0})
    urls.append({'url':"https://crest-tq.eveonline.com/market/10000047/orders/all/",'retry':0})
    urls.append({'url':"https://crest-tq.eveonline.com/market/10000048/orders/all/",'retry':0})
    urls.append({'url':"https://crest-tq.eveonline.com/market/10000049/orders/all/",'retry':0})
    urls.append({'url':"https://crest-tq.eveonline.com/market/10000050/orders/all/",'retry':0})
    urls.append({'url':"https://crest-tq.eveonline.com/market/10000051/orders/all/",'retry':0})
    urls.append({'url':"https://crest-tq.eveonline.com/market/10000052/orders/all/",'retry':0})
    urls.append({'url':"https://crest-tq.eveonline.com/market/10000053/orders/all/",'retry':0})
    urls.append({'url':"https://crest-tq.eveonline.com/market/10000054/orders/all/",'retry':0})
    urls.append({'url':"https://crest-tq.eveonline.com/market/10000055/orders/all/",'retry':0})
    urls.append({'url':"https://crest-tq.eveonline.com/market/10000056/orders/all/",'retry':0})
    urls.append({'url':"https://crest-tq.eveonline.com/market/10000057/orders/all/",'retry':0})
    urls.append({'url':"https://crest-tq.eveonline.com/market/10000058/orders/all/",'retry':0})
    urls.append({'url':"https://crest-tq.eveonline.com/market/10000059/orders/all/",'retry':0})
    urls.append({'url':"https://crest-tq.eveonline.com/market/10000060/orders/all/",'retry':0})
    urls.append({'url':"https://crest-tq.eveonline.com/market/10000061/orders/all/",'retry':0})
    urls.append({'url':"https://crest-tq.eveonline.com/market/10000062/orders/all/",'retry':0})
    urls.append({'url':"https://crest-tq.eveonline.com/market/10000063/orders/all/",'retry':0})
    urls.append({'url':"https://crest-tq.eveonline.com/market/10000064/orders/all/",'retry':0})
    urls.append({'url':"https://crest-tq.eveonline.com/market/10000065/orders/all/",'retry':0})
    urls.append({'url':"https://crest-tq.eveonline.com/market/10000066/orders/all/",'retry':0})
    urls.append({'url':"https://crest-tq.eveonline.com/market/10000067/orders/all/",'retry':0})
    urls.append({'url':"https://crest-tq.eveonline.com/market/10000068/orders/all/",'retry':0})
    urls.append({'url':"https://crest-tq.eveonline.com/market/10000069/orders/all/",'retry':0})


    while len(urls)>0:
        futures=[]
        logging.warn('Looping');
        for url in urls:
            logging.warn('URL:{}  Retry:{}'.format(url['url'],url['retry']));
            futures.append(getData(session,url['url'],url['retry']))
        urls=[]
        for result in as_completed(futures):
            logging.warn("as completed")
            trans = connection.begin()
            presult=processData(result,connection,orderTable)
            if presult['retry']==1:
                urls.append(presult)
                logging.warn("adding {} to retry {}".format(presult.url,presult.retry))
            if presult['retry'] == 0 and presult['url'] is not None:
                logging.warn('{} has more pages. {}'.format(result.url,presult['retry']))
                urls.append(presult)
            trans.commit()
