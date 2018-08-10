import redis
import requests
import json
import random
import time
from pprint import pprint
from bs4 import BeautifulSoup


def build_session():
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Host": "www.pornhub.com",
        "Origin": "https://www.pornhub.com",
        "Referer": "https://www.pornhub.com/",
    }
    s = requests.Session()
    s.headers.update(headers)
    return s


def random_sleep():
    sleep_time = random.randint(25, 35)
    time.sleep(sleep_time)
    print('sleep for {} seconds'.format(sleep_time))


def get_movie_info(s, movie_url, proxies, token_cookie):
    dl_url = None
    try:
        r = s.get(movie_url, proxies=proxies, cookies=token_cookie)
        soup = BeautifulSoup(r.content, 'lxml')
        dl_list = soup.find_all('a', class_='downloadBtn greyButton')
        if len(dl_list) > 0:
            dl_url = dl_list[0].get('href')
    except Exception as e:
        print('failed to get movie detail {}'.format(movie_url))
    return dl_url


def get_need_download(conn, download_count, already_run_id_list):
    count = 0
    need_download_list = dict()
    r_index = random.randint(0, len(conn.keys()) - 100)
    for key in conn.keys()[r_index: r_index + 100]:
        key = key.decode('utf-8')
        if key in already_run_id_list:
            continue
        d = dict()
        d['url'] = conn.hget(key, 'url').decode('utf-8')
        d['title'] = conn.hget(key, 'title').decode('utf-8')
        if conn.hget(key, 'status') is not None:
            continue
        need_download_list[key] = d
        count += 1
        if count == download_count:
            break
    return need_download_list


def aria2_request(rpc_url, c_req):
    res = None
    req = {
        'jsonrpc': '2.0',
        'id': 'qwer',
        }
    req.update(c_req)
    try:
        r = requests.post(rpc_url, json=req)
        if r.status_code == 200:
            res = r.content.decode('utf-8')
            res = json.loads(res)
        else:
            print('aria2 rpc request return code: {}'.format(r.status_code))
        res = r.content.decode('utf-8')
        res = json.loads(res)
        res = res.get('result')
    except Exception as e:
        print('failed to request arai2 rpc, error: {}'.format(e))
    return res


def aria2_add_uri(rpc_url, dl_urls, download_path):
    opts = {
        'dir': download_path,
    }
    req = {
        'method': 'aria2.addUri',
        'params': [dl_urls, opts]
        }
    res = aria2_request(rpc_url, req)
    return res


def aria2_get_state(rpc_url, gid):
    req = {
        'method': 'aria2.tellStatus',
        'params': [gid]
        }
    res = aria2_request(rpc_url, req)
    return res


def aria2_remove(rpc_url, gid):
    req = {
        'method': 'aria2.remove',
        'params': [gid]
        }
    res = aria2_request(rpc_url, req)
    return res


def aria2_all_active(rpc_url):
    req = {
        'method': 'aria2.tellActive',
        }
    running_list = aria2_request(rpc_url, req)
    return running_list


def aria2_clean_mem(rpc_url):
    req = {
        'method': 'aria2.purgeDownloadResult',
        }
    stop_list = aria2_request(rpc_url, req)
    return stop_list


def aria2_global_state(rpc_url):
    req = {
        'method': 'aria2.getGlobalStat',
        }
    res = aria2_request(rpc_url, req)
    return res


def aria2_all_stop(rpc_url):
    req = {
        'method': 'aria2.tellStopped',
        'params': [0, 4]
        }
    res = aria2_request(rpc_url, req)
    return res


def check_complete_job(conn, rpc_url):
    complete_job_list = aria2_all_stop(rpc_url)
    complete_job_gid_list = [i.get('gid') for i in complete_job_list] if len(complete_job_list) > 0 else list()
    print(complete_job_gid_list)
    return complete_job_gid_list


def check_running(rpc_url):
    running_list = aria2_all_active(rpc_url)
    return running_list


def download_movie(dl_list, s, proxies, token_cookie, rpc_url, download_path):
    run_list = dict()
    fail_list = dict()
    for movie_id, movie_info in dl_list.items():
        print('try to download {}'.format(movie_id))
        movie_url = movie_info.get('url')
        dl_url = get_movie_info(s, movie_url, proxies, token_cookie)
        random_sleep()
        if dl_url is None:
            fail_list[movie_id] = movie_info
            continue
        state = aria2_add_uri(rpc_url, [dl_url, ], download_path)
        gid = state.get('result')
        if gid is not None:
            run_list[movie_id] = movie_info.update({'gid': gid})
            print('job {} running'.format(gid))
    return run_list, fail_list


def main_download_job(conn, proxies, token_cookie, s, rpc_url, download_path, total_download, already_run):
    print('download job starting, total job: {}, already running: {}'.format(total_download, already_run))
    need_download_num = total_download - already_run
    already_run_id_list = list()
    dl_list = get_need_download(conn, need_download_num, already_run_id_list)
    run_list, fail_list = download_movie(dl_list, s, proxies, token_cookie, rpc_url, download_path)

    # make sure running task number is total download
    already_run_id_list = run_list.keys()
    diff_num = total_download - len(run_list)
    while diff_num > 0:
        dl_list = get_need_download(conn, diff_num, already_run_id_list)
        tmp_run_list, fail_list = download_movie(dl_list, s, proxies, token_cookie, rpc_url, download_path)
        run_list.update(tmp_run_list)
        diff_num = total_download - len(run_list)
    print('{} job running\n'.format(total_download))


def main():
    # aria2c --enable-rpc --rpc-listen-all -D

    # init parameters
    download_path = '/Users/wpei/Downloads/aria2_download'
    total_download = 1
    rpc_host = '127.0.0.1'
    rpc_port = '6800'
    rpc_url = 'http://{}:{}/jsonrpc'.format(rpc_host, rpc_port)

    token_cookie = {
        'il': 'v1GHsWxKmUcnmsqeaZ6tJk6qWe3LV4fOuZu66tLvllapQxNTQxMzkxMzA5VGdmOWFwUzBueU0yVnl1N1h6Q3BrdnNxZUY4STZWWlJxcmg3cjV3Rg..'
    }
    proxies = {
        'http': 'socks5://127.0.0.1:1080',
        'https': 'socks5://127.0.0.1:1080'
    }

    # init process
    conn = redis.StrictRedis(host='127.0.0.1', port='6379', password='foobared', db=5)
    s = build_session()

    # check stop jobs
    check_complete_job(conn, rpc_url)
    exit(1)

    # removie finish jobs
    running_list = check_running(rpc_url)
    already_run = len(running_list)
    print(running_list)
    exit(1)

    # download process
    main_download_job(conn, proxies, token_cookie, s, rpc_url, download_path, total_download, already_run)

    # already_run_id_list = list()
    # dl_list = get_need_download(conn, total_download, already_run_id_list)
    # run_list, fail_list = download_movie(dl_list, s, proxies, token_cookie, rpc_url, download_path)

    # # make sure running task number is total download
    # already_run_id_list = run_list.keys()
    # diff_num = total_download - len(run_list)
    # while diff_num > 0:
    #     dl_list = get_need_download(conn, diff_num, already_run_id_list)
    #     tmp_run_list, fail_list = download_movie(dl_list, s, proxies, token_cookie, rpc_url, download_path)
    #     run_list.update(tmp_run_list)
    #     diff_num = total_download - len(run_list)

main()