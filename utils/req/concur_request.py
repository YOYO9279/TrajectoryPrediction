import time

import grequests
import grequests_throttle as gt

from conf.config import s


def checkResend(resp):
    errurls = []
    goodresp = []
    method = resp[0].request.method
    for i in resp:
        errcode = i.json()["errcode"]
        if errcode == 10021:
            print(i.text)
            errurls.append(i.url)
        if errcode == 10000:
            goodresp.append(i)
        else:
            print(i.text)
    if method == "POST":
        errreqs = [grequests.post(url, session=s) for url in errurls]
    else:
        errreqs = [grequests.get(url, session=s) for url in errurls]
    return goodresp, errreqs


def concurQ(reqs):
    res = []
    while len(reqs) != 0:
        resp = gt.map(reqs, rate=40)
        goodresp, errreqs = checkResend(resp)
        res += goodresp
        reqs = errreqs
        if len(reqs) != 0:
            time.sleep(0.3)
            print("retry")
    return res
