import redis


if __name__ == "__main__":
    # build the basic list
    baseitemids=[34,35,36,37,38,39,40,11399]
    sub = redis.StrictRedis(host='localhost', port=6379, db=0)
    for itemid in baseitemids:
        region=10000002
        what="{}|{}".format(itemid,region)
        sub.publish("sellprices",what)
        sub.publish("buyprices",what)

