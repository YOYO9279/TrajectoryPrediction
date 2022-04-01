import requests


def wx_reminder(content='done'):
    url = f'http://wxpusher.zjiecode.com/api/send/message/?appToken=AT_u2Leo6K3qZ9bImPOZRbaNQVql7fN97SN&content={content}&uid=UID_kqgL0NeS7Jc2oaupxkaIn9D9o4Wb'

    requests.get(url)
