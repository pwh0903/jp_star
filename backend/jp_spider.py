import json
import time
import os
import random
from urlparse import urljoin
from pprint import pprint
import redis
import requests
from bs4 import BeautifulSoup


def download_img(s, url, proxies, movie_path, img_name):
    status = False
    try:
        r = requests.get(url, proxies=proxies)
        if img_name == None:
            img_name = '{}.jpg'.format(str(count))
        with open(os.path.join(movie_path, img_name), 'wb') as f:
            f.write(r.content)
        status = True
    except Exception as e:
        print('failed to download {}, error: {}'.format(url, e))
    return status


def random_sleep():
    time.sleep(random.randint(1,5))


def redis_conn(hostname, port=6379, db=0, password='foobared'):
    conn = None
    conn = redis.StrictRedis(host=hostname, port=port, db=db, password=password)
    return conn


def build_re_session():
    s = requests.Session()
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36'
    }
    s.headers.update(headers)
    return s

def movie_in_redis(movie_id, redis_movie):
    in_redis = redis_movie.exists(movie_id)
    return in_redis


def detail_parser(movie_detail_page, movie_info, movie_img_path, s, proxies):
    movie_imgs = os.listdir(movie_img_path)
    movie_info['title'] = movie_detail_page.find_all('h3')[0].text
    movie_detail = movie_detail_page.find_all(class_='row movie')[0]

    frontcover_url = movie_detail_page.find_all(class_='bigImage')[0].get('href')
    sample_imgs = [i.get('href') for i in movie_detail_page.find_all('a', class_='sample-box')]
    fc_dl_status = download_img(s, frontcover_url, proxies, movie_img_path, 'frontcover.jpg')
    count = 1
    for url in sample_imgs:
        print movie_img_path, count
        s_dl_status = download_img(s, url, proxies, movie_img_path, '{}.jpg'.format(str(count)))
        count += 1
    print sample_imgs
    print frontcover_url
    exit(1)


def run_spider(url, proxies, redis_movie, download_path):
    print('fetching data from {}...'.format(url))
    next_url = None
    
    fail_dict = {}
    s = build_re_session()
    r = s.get(url, proxies=proxies)

    if r.status_code == 200:
        soup = BeautifulSoup(r.content, 'lxml')
    else:
        print('get content failed')
    movie_list = soup.find_all(class_='movie-box')
    next_url = urljoin(url, soup.find_all('a', id='next')[0].get('href'))

    for movie in movie_list:
        movie_info = dict()
        movie_url =  movie.get('href')
        movie_id = movie_url.strip('/').split('/')[-1]
        if movie_in_redis(movie_id, redis_movie):
            print('{} already in redis, pass'.format(movie_id))
            continue
        movie_img_path = os.path.join(download_path, movie_id)
        if not os.path.isdir(movie_img_path):
            os.mkdir(movie_img_path)
        movie_info['id'] = movie_id
        movie_info['url'] = movie_url
        r = s.get(movie_url, proxies=proxies)
        if r.status_code != 200:
            fail_dict[movie_id] = movie_url
            print '{} failed to get detail page, status code: {}'.format(movie_id, r.status_code)
            continue
        movie_detail_page = BeautifulSoup(r.content, 'lxml')
        try:
            detail_parser(movie_detail_page, movie_info, movie_img_path, s, proxies)
        except Exception as e:
            print('failed to parse detail for {}, error: {}'.format(movie_url, e))
            fail_dict[movie_id] = movie_url
            continue
        redis_movie.set(movie_id, movie_info)
        random_sleep()

    return next_url


def main():
    download_path = './download'
    if not os.path.isdir(download_path):
        os.mkdir(download_path)

    proxies = {
        'http': 'socks5://127.0.0.1:1080',
        'https': 'socks5://127.0.0.1:1080'
    }
    url_list = [
        'http://javbus.pw',
    ]
    redis_host = 'localhost'
    redis_port = 6379
    redis_pass = 'foobared'
    redis_db_movie = 0
    # redis_db_star = 1

    movie_detail_fail_list = {}
    try:
        redis_movie = redis_conn(redis_host, redis_port, redis_db_movie, redis_pass)
        # redis_star = redis_conn(redis_host, redis_port, redis_db_star)
    except Exception as e:
        error_message = 'build redis connection failed, error: {}, shutting down...'.format(e)
        print(error_message)
        exit(1)

    for url in url_list:
        next_url = run_spider(url, proxies, redis_movie, download_path)
        while next_url:
            next_url = run_spider(next_url, proxies, redis_movie, download_path)
            random_sleep()

if __name__ == "__main__":
    main()