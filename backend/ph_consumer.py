import redis
import requests
import json
import random
import time
import re
from pprint import pprint
import math
import os
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
    sleep_time = random.randint(25, 45)
    print('sleep for {} seconds'.format(sleep_time))
    time.sleep(sleep_time)


def get_n(n):
    if n == 0:
        return 0
    if n % 1 != 0 or n * n < 2:
        return 1
    if n % 2 == 0:
        return 2
    if n % 3 == 0:
        return 3
    if n % 5 == 0:
        return 5
    m = int(math.sqrt(n))
    for i in range(7, m + 1, 30):
        if n % i == 0:
            return i
        if n % (i + 4) == 0:
            return i + 4
        if n % (i + 6) == 0:
            return i + 6
        if n % (i + 10) == 0:
            return i + 10
        if n % (i + 12) == 0:
            return i + 12
        if n % (i + 16) == 0:
            return i + 16
        if n % (i + 22) == 0:
            return i + 22
        if n % (i + 24) == 0:
            return i + 24


def break_js(content, token_cookie):
    content = content.decode('utf-8')
    content_no_comment = re.sub('/\*[a-zA-Z\+\s=\-\d;\*]+\*/', '', content)
    else_condition_value_list = re.findall('else[\s]+p[\s\-=]+(\d+)[\*\s]+(\d+)', content_no_comment)
    last_p_minuse_value = re.findall('p([\+\-])[=\s]+(\d+)', content_no_comment)[-1]
    if_condition_value_list = re.findall('s >> (\d+)\) \& (\d+)\)[p\s\+\-=]+(\d+)[\*\s]+(\d+)', content_no_comment)
    condition_value_list = list()
    for a in if_condition_value_list:
        index_a = if_condition_value_list.index(a)
        b = else_condition_value_list[index_a]
        condition_value_list.append(a + b)

    p = int(re.findall('var p=(\d+);', content)[0])
    s = int(re.findall('var s=(\d+);', content)[0])

    for condition_value in condition_value_list:
        if s >> int(condition_value[0]) & int(condition_value[1]):
            p += int(condition_value[2]) * int(condition_value[3])
        else:
            p -= int(condition_value[4]) * int(condition_value[5])
    if last_p_minuse_value[0] == '+':
        p += int(last_p_minuse_value[1])
    else:
        p -= int(last_p_minuse_value[1])
    n = get_n(p)
    a, b = re.findall('\"\+s\+\":(\d+):(\d+)', content)[0]
    random_key = {
        "RNKEY": "{}*{}:{}:{}:{}".format(n, p//n, s, a, b)
    }
    token_cookie.update(random_key)
    return token_cookie


def get_movie_info(s, movie_url, proxies, token_cookie):
    dl_url = None
    retry_time = 0
    try:
        while retry_time < 4:
            r = s.get(movie_url, proxies=proxies, cookies=token_cookie)
            soup = BeautifulSoup(r.content, 'lxml')
            dl_list = soup.find_all('a', class_='downloadBtn greyButton')
            if len(dl_list) > 0:
                dl_url = dl_list[0].get('href')
                break
            else:
                retry_time += 1
                if isinstance(r.content, bytes):
                    content = r.content.decode('utf-8')
                else:
                    content = r.content
                if len(re.findall('leastFactor', content)) > 0 and len(re.findall('document\.location\.reload', content)) > 0:
                    print('load js cookie value, will retry: {}, length of class downloadBtn greyButton != 1, length is {}, url: {}'.format(retry_time, len(dl_list), movie_url))
                    break_js(r.content, token_cookie)
                elif len(soup.find_all('div', class_='recaptchaContent')) == 1:
                    print('need to break robot check')
                    time.sleep(900)
                else:
                    print('something wrong when get {}'.format(movie_url))
            random_sleep()
    except Exception as e:
        print('failed to get movie detail {}'.format(movie_url))
    return dl_url


def get_need_download(conn, download_count):
    count = 0
    need_download_list = dict()
    for key in conn.keys():
        key = key.decode('utf-8')
        if conn.hget(key, 'download') is not None:
            continue
        d = dict()
        d['url'] = conn.hget(key, 'url').decode('utf-8')
        d['title'] = conn.hget(key, 'title').decode('utf-8')
        d['category'] = conn.hget(key, 'category').decode('utf-8')
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


def aria2_add_uri(rpc_url, dl_urls, download_path, dl_filename):
    opts = {
        'dir': download_path,
        'out': '{}.mp4'.format(dl_filename)
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
    complete_list = aria2_request(rpc_url, req)
    return complete_list


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


def update_movie_download_state(conn, gid_list, state):
    if state == 1:
        state_string = 'complete'
    elif state == 2:
        state_string = 'running'
    for key in conn.keys():
        key_gid = conn.hget(key, 'gid')
        if key_gid is None:
            continue
        if key_gid.decode('utf-8') in gid_list:
            conn.hset(key, 'download', state)
            print('{} {} download {}'.format(key, conn.hget(key, 'title').decode('utf-8'), state_string))


def check_complete_job(conn, rpc_url):
    complete_job_list = aria2_all_stop(rpc_url)
    complete_job_gid_list = [i.get('gid') for i in complete_job_list] if len(complete_job_list) > 0 else list()
    return complete_job_gid_list


def check_running(rpc_url):
    running_list = aria2_all_active(rpc_url)
    running_gid_list = [i.get('gid') for i in running_list] if len(running_list) > 0 else list()
    return running_gid_list


def download_movie(conn, dl_list, s, proxies, token_cookie, rpc_url, download_path):
    dl_path = ''
    run_list = dict()
    fail_list = dict()
    for movie_id, movie_info in dl_list.items():
        print('try to download {}'.format(movie_id))
        movie_url = movie_info.get('url')
        movie_title = movie_info.get('title')
        movie_category = movie_info.get('category')
        dl_path = os.path.join(download_path, movie_category)
        if os.path.isdir(dl_path) is None:
            os.mkdir(dl_path)
        dl_url = get_movie_info(s, movie_url, proxies, token_cookie)
        if dl_url is None:
            fail_list[movie_id] = movie_info
            continue
        state = aria2_add_uri(rpc_url, [dl_url, ], dl_path, movie_title)
        gid = state
        if gid is not None:
            run_list[movie_id] = movie_info.update({'gid': gid})
            print('job {} running'.format(gid))
            conn.hset(movie_id, 'download', 2)
            conn.hset(movie_id, 'gid', gid)
        else:
            fail_list[movie_id] = movie_info
        random_sleep()
    return run_list, fail_list


def main_download_job(conn, proxies, token_cookie, s, rpc_url, download_path, total_download):
    # download status
    # 0: not run
    # 1: done
    # 2: running

    # check complete jobs
    print('checking complete download and update redis...')
    complete_job_list = check_complete_job(conn, rpc_url)

    if len(complete_job_list) > 0:
        update_movie_download_state(conn, complete_job_list, 1)
    print('{} jobs download complete'.format(len(complete_job_list)))

    # remove complete jobs
    print('clean complete job in aria2 daemon...')
    clean_list = aria2_clean_mem(rpc_url)
    print('{} complete jobs cleaned in aria2 daemon'.format(len(clean_list)))

    # checking running jobs
    print('checking running jobs in aira2 and update redis...')
    running_list = check_running(rpc_url)
    if len(running_list) > 0:
        update_movie_download_state(conn, running_list, 2)
    already_run = len(running_list)
    print('{} running jobs cleaned in aria2 daemon'.format(already_run))

    # start to download movies
    need_download_num = total_download - already_run
    print('total job: {}, already running: {}, need to run {}'.format(total_download, already_run, need_download_num))

    if need_download_num > 0:
        if len(conn.keys()) < 10:
            random_sleep()
        else:
            dl_list = get_need_download(conn, need_download_num)
            run_list, fail_list = download_movie(conn, dl_list, s, proxies, token_cookie, rpc_url, download_path)

    # make sure running task number is total download
    # already_run_id_list = run_list.keys()
    # diff_num = total_download - len(run_list)
    # while diff_num > 0:
    #     dl_list = get_need_download(conn, diff_num, already_run_id_list)
    #     tmp_run_list, fail_list = download_movie(conn, dl_list, s, proxies, token_cookie, rpc_url, download_path)
    #     run_list.update(tmp_run_list)
    #     diff_num = total_download - len(run_list)
    # print('{} job running\n'.format(total_download))


def main():
    # aria2c --enable-rpc --rpc-listen-all -D

    # init parameters
    download_path = '/Users/wpei/Downloads/aria2_download'
    total_download = 8
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

    # download process
    while True:
        main_download_job(conn, proxies, token_cookie, s, rpc_url, download_path, total_download)
        random_sleep()
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


if __name__ == '__main__':
    main()
