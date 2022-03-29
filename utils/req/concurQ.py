import grequests_throttle as gt

def concurQ(reqs):
    resp = gt.map(reqs, rate=50)
    for i in resp:
        try:
            print(i.text)
        except Exception as e:
            print(e)
    return resp