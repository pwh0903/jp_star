from pprint import pprint
import redis
import time
import json
import random
from urllib.parse import urljoin
import requests
from bs4 import BeautifulSoup


def get_token(s, auth_url, proxies):
    cookies = {
        "bs": "3fji3ahw4jw7i1d0zvj77kbh8ao75uc5",
        "ss": "549086987087850421",
        "RNLBSERVERID": "ded6856",
        "_ga": "GA1.2.669540838.1525010683",
        "performance_timing": "video",
        "ua": "4c68bf180b963757ccc259f98d2f08ae",
        "platform": "pc",
        "_gid": "GA1.2.1118720219.1533598249",
        "g36FastPopSessionRequestNumber": "20",
        "expiredEnterModalShown": "1",
        "FPSRN": "5",
        "_gat": "1"
    }
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
    time.sleep(random.randint(10, 15))


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


def get_movie_info(s, movie_url, proxies, token_cookie):
    r = s.get(movie_url, proxies=proxies, cookies=token_cookie)
    soup = BeautifulSoup(r.content, 'lxml')
    dl_list = soup.find_all('a', class_='downloadBtn greyButton')
    if len(dl_list) > 0:
        dl_url = dl_list[0].get('href')
        print(dl_url)


def get_movie_list(s, host_url, url, proxies, token_cookie, ps_redis):
    print('fetch movie list from {}'.format(url))
    next_url = None
    movie_url_list = dict()
    r = s.get(url, proxies=proxies, cookies=token_cookie)
    soup = BeautifulSoup(r.content, 'lxml')
    next_url = soup.find_all('li', class_='page_next')
    if len(next_url) == 1:
        next_url = next_url[0].a.get('href')
        next_url = urljoin(host_url, next_url)

    movie_list = soup.find_all('span', class_='title')
    for movie in movie_list:
        d = dict()
        movie = movie.a
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

    # url = 'https://www.pornhub.com/channels/vixen/videos?o=ra'
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
        channel_url = '{}/videos'.format(channel_url)
        next_url = get_movie_list(s, host_url, channel_url, proxies, token_cookie, ps_redis)
        random_sleep()
        while next_url:
            next_url = get_movie_list(s, host_url, next_url, proxies, token_cookie, ps_redis)
            random_sleep()


if __name__ == '__main__':
    main()
