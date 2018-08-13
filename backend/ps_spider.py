import redis
import re
import math
import time
import json
import random
from urllib.parse import urljoin
import requests
from bs4 import BeautifulSoup


def get_token(s, auth_url, proxies):
    token = ''
    post_data = {
        "username": "haha0903",
        "password": "peter0903",
        "from": "pc_login_modal_:index",
        "token": "MTUzMzYxNTI4OFW5rcyWnLIc0DDa1PsGfp8fHW_sKpYahSjGVsuV14kZWq-OXAaj3bYWxxHT3Z-Ht1ofEEu--O2epqSvHos55HU."
    }
    r = s.post(auth_url, proxies=proxies, data=post_data)
    print(r.status_code)
    print(r.headers)
    return token


def random_sleep():
    sleep_time = random.randint(35, 55)
    print('sleep for {} seconds'.format(sleep_time))
    time.sleep(sleep_time)


def build_session():
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Host": "www.pornhub.com",
        "Origin": "https://www.pornhub.com",
        "Referer": "https://www.pornhub.com/",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1"
    }
    s = requests.Session()
    s.headers.update(headers)
    return s


def get_movie_info(s, movie_url, proxies, token_cookie):
    r = s.get(movie_url, proxies=proxies, cookies=token_cookie)
    soup = BeautifulSoup(r.content, 'lxml')
    dl_list = soup.find_all('a', class_='downloadBtn greyButton')
    if len(dl_list) > 0:
        dl_url = dl_list[0].get('href')
        print(dl_url)


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


def get_movie_list(s, host_url, url, channel_title, proxies, token_cookie, ps_redis):
    # random_key = {
    #     "RNKEY": "1001219*1131181:1408596114:3845177341:1"
    # }
    # token_cookie.update(random_key)
    print('fetch movie list from {}'.format(url))
    next_url = None
    movie_url_list = dict()
    retry_time = 0
    tmp_movie_list = list()

    while len(tmp_movie_list) != 1 and retry_time < 4:
        r = s.get(url, proxies=proxies, cookies=token_cookie)
        soup = BeautifulSoup(r.content, 'lxml')
        next_url = soup.find_all('li', class_='page_next')
        if len(next_url) == 1:
            next_url = next_url[0].a.get('href')
            next_url = urljoin(host_url, next_url)

        tmp_movie_list = soup.find_all('ul', class_='row-5-thumbs')
        if len(tmp_movie_list) == 1:
            movie_list = tmp_movie_list[0]
            movie_list = movie_list.find_all('span', class_='title')
            for movie in movie_list:
                d = dict()
                movie = movie.a
                d['category'] = channel_title
                d['title'] = movie.get('title').strip()
                d['url'] = urljoin(host_url, movie.get('href').strip())
                if 'view_video.php' not in d['url']:
                    continue
                movie_id = d['url'].split('?viewkey=')[-1]
                movie_url_list[movie_id] = d
                if not ps_redis.exists(movie_id):
                    ps_redis.hmset(movie_id, d)
                else:
                    print('{} {} in redis, pass'.format(movie_id, d['title']))
        else:
            retry_time += 1
            print('load js cookie value, will retry: {}, length of class row-5-thumbs != 1, length is {}, url: {}'.format(retry_time, len(tmp_movie_list), url))
            break_js(r.content, token_cookie)
            random_sleep()

    return next_url


def get_channellist(s, host_url, channels_url, proxies, token_cookie):
    print('fetch channel list from {}'.format(channels_url))
    next_url = None
    channels_url_list = dict()
    r = s.get(channels_url, proxies=proxies, cookies=token_cookie)
    soup = BeautifulSoup(r.content, 'lxml')
    next_url = soup.find_all('li', class_='page_next')
    if len(next_url) == 1:
        next_url = next_url[0].a.get('href')
        next_url = urljoin(host_url, next_url)

    for channel in soup.find_all('div', class_='descriptionContainer'):
        try:
            channel_url = urljoin(host_url, channel.ul.li.a.get('href').strip())
            channel_title = channel.ul.li.a.get('href').split('/')[-1].strip()
            channels_url_list[channel_title] = channel_url
        except Exception as e:
            print('failed to parse channel info, error: {}'.format(e))
            continue
    return channels_url_list, next_url


def main():
    token_cookie = {
        'il': 'v1GHsWxKmUcnmsqeaZ6tJk6qWe3LV4fOuZu66tLvllapQxNTQxMzkxMzA5VGdmOWFwUzBueU0yVnl1N1h6Q3BrdnNxZUY4STZWWlJxcmg3cjV3Rg..'
    }
    proxies = {
        'http': 'socks5://127.0.0.1:1080',
        'https': 'socks5://127.0.0.1:1080'
    }

    redis_host = 'localhost'
    redis_port = 6379
    redis_pass = 'foobared'
    ps_movie = 5

    try:
        ps_redis = redis.StrictRedis(host=redis_host, port=redis_port, db=ps_movie, password=redis_pass)
    except Exception as e:
        error_message = 'build redis connection failed, error: {}, shutting down...'.format(e)
        print(error_message)
        exit(1)

    host_url = 'https://www.pornhub.com'

    # auth_url = 'https://www.pornhub.com/front/authenticate'
    s = build_session()

    # print('fetching channel list...')
    # channels_url = 'https://www.pornhub.com/channels?o=rk'
    # all_channels_url_list = dict()
    # channels_url_list, next_channel_url = get_channellist(s, host_url, channels_url, proxies, token_cookie)
    # all_channels_url_list.update(channels_url_list)
    # random_sleep()
    # while next_channel_url:
    #     channels_url_list, next_channel_url = get_channellist(s, host_url, next_channel_url, proxies, token_cookie)
    #     all_channels_url_list.update(channels_url_list)
    #     random_sleep()
    # with open('./channel.json', 'w') as f:
    #     json.dump(all_channels_url_list, f)
    # return False

    with open('./channel.json') as f:
        all_channels_url_list = json.load(f)

    for channel_title, channel_url in all_channels_url_list.items():
        if channel_title.lower() not in ['massagerooms', 'vixen', 'faketaxi', 'blacked', 'bartty-sis', 'property-sex', 'public-agent']:
            continue
        channel_url = '{}/videos'.format(channel_url)
        next_url = get_movie_list(s, host_url, channel_url, channel_title, proxies, token_cookie, ps_redis)
        random_sleep()
        while next_url:
            next_url = get_movie_list(s, host_url, next_url, channel_title, proxies, token_cookie, ps_redis)
            random_sleep()


if __name__ == '__main__':
    main()
