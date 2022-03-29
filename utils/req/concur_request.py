import time

import grequests_throttle as gt


def checkResend(resp):
    errreqs = []
    goodresp = []
    for i in resp:
        errcode = i.json()["errcode"]
        if errcode == 10021:
            print(i.text)
            errreqs.append(i.request)
        if errcode == 10000:
            goodresp.append(i)
        else:
            print(i.text)
    return goodresp, errreqs


def concurQ(reqs):
    res = []
    while len(reqs) != 0:
        resp = gt.map(reqs, rate=50)
        goodresp, errreqs = checkResend(resp)
        res += goodresp
        reqs = errreqs
        if len(reqs) != 0:
            time.sleep(0.3)
            print("retry")
    return res
