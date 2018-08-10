import urllib2
import json
from pprint import pprint


def add_uri(rpc_url, dl_urls):
    res = None
    jsonreq = json.dumps({
        'jsonrpc':'2.0', 
        'id':'qwer',
        'method':'aria2.addUri',
        'params':[dl_urls]
        })
    try:
        r = urllib2.urlopen(rpc_url, jsonreq)
        res = r.read()
        gid = json.loads(res).get('result')
        print(gid)
    except Exception as e:
        print('failed to get add {}, error: {}'.format(dl_urls, e))
    return res


def get_state(rpc_url, gid):
    res = None
    jsonreq = json.dumps({
        'jsonrpc':'2.0', 
        'id':'qwer',
        'method':'aria2.tellStatus',
        'params':[gid]
        })
    try:
        r = urllib2.urlopen(rpc_url, jsonreq)
        res = r.read()
    except Exception as e:
        print('failed to get state of gid {}, error: {}'.format(gid, e))
    return res


def main():
    rpc_host = '192.168.0.10'
    rpc_port = '6800'
    rpc_url = 'http://{}:{}/jsonrpc'.format(rpc_host, rpc_port)

    # state = add_uri(rpc_url, ['https://cd.phncdn.com/videos/201807/09/173761321/720P_1500K_173761321.mp4?a5dcae8e1adc0bdaed975f0d66fb5e0568d9f5b553250a40db604034853fa0906616b9adfa700a2022514ad1a6f32dbb7d2e67785b042d64d9b84aaf9e9455bb1d2a701df85c881e02da8a8df9980d73a487d004077ba78fa19ef60c9140731f8eae405b29bbd8c74ebe4dfe2755d6aa5315b1ec51bc22c9f61b13b2ba'])
    # print state
    gid = '15c42b4d1849e82c'
    state = get_state(rpc_url, gid)
    pprint(json.loads(state))

main()