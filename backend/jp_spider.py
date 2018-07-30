import json
import time
import os
import re
import logging
import random
from urlparse import urljoin
from pprint import pprint
import redis
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
from bs4 import BeautifulSoup


# def build_logger():
#     log = logging.getLogger()
#     sh = logging.StreamHandler()
#     fh = logging.FileHandler(logfile, mode='w+')


def download_img(s, url, proxies, movie_path, img_name):
    status = False
    try:
        r = s.get(url, proxies=proxies, verify=False)
        if img_name == None:
            img_name = '{}.jpg'.format(str(img_name))
        with open(os.path.join(movie_path, img_name), 'wb') as f:
            f.write(r.content)
        status = True
    except Exception as e:
        # print('failed to download {}, error: {}'.format(url, e))
        pass
    return status


def random_sleep():
    time.sleep(random.randint(1,5))


def build_request_session():
    s = requests.Session()
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36'
    }
    s.headers.update(headers)
    return s

def movie_in_redis(movie_id, redis_movie):
    in_redis = redis_movie.exists(movie_id)
    return in_redis


def get_magnet(magnet_info, base_url, movie_url, proxies):
    mangnet_list = list()
    url = '/ajax/uncledatoolsbyajax.php?'
    url = urljoin(base_url, url) + '?'
    for key, value in magnet_info.items():
        url = '{}{}={}&'.format(url, key, value)
    url = url.strip('&')
    headers = {
        "referer": movie_url,
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36",
        "x-requested-with": "XMLHttpRequest"
    }
    r = requests.get(url, headers=headers, proxies=proxies, verify=False)
    tmp_list = BeautifulSoup(r.content, 'lxml').find_all('tr')
    for magnet in tmp_list:
        mangnet_dict = dict()
        magnet_items = magnet.find_all('td')
        mangnet_dict['title'] = magnet_items[0].text.strip()
        mangnet_dict['url'] = magnet_items[0].a.get('href').strip()
        mangnet_dict['size'] = magnet_items[1].text.strip()
        mangnet_dict['date'] = magnet_items[2].text.strip()
        mangnet_list.append(mangnet_dict)
    return mangnet_list


def detail_parser(movie_detail_page, movie_info, movie_img_path, s, proxies, base_url, movie_url):
    movie_imgs = os.listdir(movie_img_path)
    movie_info['title'] = movie_detail_page.find_all('h3')[0].text

    # parse magnet url
    magnet_tmp = [i.text for i in movie_detail_page.find_all('script') if 'gid' in i.text][0]
    magnet_tmp = [i.strip() for i in magnet_tmp.split('\n') if i.strip() != '']
    magnet_info = dict()
    for i in magnet_tmp:
        i = i.strip(';')
        key = i.split()[1]
        value = i.split()[-1]
        magnet_info[key] = value
    movie_info['magnet_list'] = get_magnet(magnet_info, base_url, movie_url, proxies)

    # get movie detail info
    movie_detail_list = movie_detail_page.find_all(class_='col-md-3 info')[0].find_all('p')
    movie_star_list = movie_detail_page.find_all(class_='col-md-3 info')[0].find_all('li')
    len_movie_detail_list = len(movie_detail_list)
    if len(movie_star_list) > 0:
        len_movie_detail_list = len_movie_detail_list - 1
    if len_movie_detail_list == 10:
        movie_maker_index = 3
        movie_publish_index = 4
        movie_genre_index = 6
    elif len_movie_detail_list == 11:
        movie_maker_index = 3
        movie_publish_index = 4
        movie_series_index = 5
        movie_genre_index = 7
    elif len_movie_detail_list > 11:
        movie_director_index = 3
        movie_maker_index = 4
        movie_publish_index = 5
        movie_series_index = 6
        movie_genre_index = 8
    else:
        print('{} p element in movie detail sector'.format(len_movie_detail_list))
        pass

    try:
        movie_id = movie_detail_list[0].text.strip().split()[-1].strip()
    except Exception as e:
        print('faile to parse movie id, error: {}'.format(e))
    if movie_id != movie_info['id']:
        print('id on detail page:{}, id on overview page: {}, detail page: {}'.format(movie_id, movie_info['id'], movie_url))

    movie_info['movie_pub_data'] = movie_detail_list[1].text.strip().split()[-1]
    movie_info['movie_length'] = int(re.findall(r'\d+', movie_detail_list[2].text.strip().split()[-1])[0])

    try:
        if len_movie_detail_list > 11:
            movie_director = movie_detail_list[movie_director_index].a
            movie_info['movie_director_name'] = movie_director.text.strip()
            movie_info['movie_director_url'] = movie_director.get('href').strip()
        else:
            movie_info['movie_director_name'] = None
            movie_info['movie_director_url'] = None
            print('no director info for {}'.format(movie_url))
    except Exception as e:
        print('faile to parse movie director, error: {}'.format(e))


    try:
        movie_maker = movie_detail_list[movie_maker_index].a
        movie_info['movie_maker_name'] = movie_maker.text.strip()
        movie_info['movie_make_url'] = movie_maker.get('href').strip()
    except Exception as e:
        print('faile to parse movie maker, error: {}'.format(e))

    try:
        movie_publish = movie_detail_list[movie_publish_index].a
        movie_info['movie_publish_name'] = movie_publish.text.strip()
        movie_info['movie_publish_url'] = movie_publish.get('href').strip()
    except Exception as e:
        print('faile to parse movie publisher, error: {}'.format(e))

    try:
        if len_movie_detail_list > 10:
            movie_series = movie_detail_list[movie_series_index].a
            movie_info['movie_series_name'] = movie_series.text.strip()
            movie_info['movie_series_url'] = movie_series.get('href').strip()
        else:
            movie_info['movie_series_name'] = None
            movie_info['movie_series_url'] = None
            print('no series info for {}'.format(movie_url))
    except Exception as e:
        print('faile to parse movie series, error: {}'.format(e))

    movie_genre = list()
    try:
        movie_genre_list = movie_detail_list[movie_genre_index].find_all('a')
        if len(movie_genre_list) == 0:
            print('no genre info of {}'.format(movie_url))
        else:
            for i in movie_genre_list:
                d = dict()
                d['url'] = i.get('href').strip()
                d['title'] = i.text.strip()
                movie_genre.append(d)
    except Exception as e:
        print('faile to parse movie genre, error: {}'.format(e))
    movie_info['movie_genre'] = movie_genre

    movie_star = list()
    try:
        if len(movie_star_list) == 0:
            print('no movie star info of {}'.format(movie_url))
        else:
            for i in movie_star_list:
                d = dict()
                d['url'] = i.div.a.get('href').strip()
                d['name'] = i.div.a.text.strip()
                movie_star.append(d)
    except Exception as e:
        print('faile to parse movie star, error: {}'.format(e))
    movie_info['movie_star'] = movie_star

    # download images
    frontcover_url = movie_detail_page.find_all(class_='bigImage')[0].get('href').strip()
    sample_imgs = [i.get('href').strip() for i in movie_detail_page.find_all('a', class_='sample-box')]
    fc_dl_status = download_img(s, frontcover_url, proxies, movie_img_path, 'frontcover.jpg')
    count = 1
    for url in sample_imgs:
        s_dl_status = download_img(s, url, proxies, movie_img_path, '{}.jpg'.format(str(count)))
        if s_dl_status:
            count += 1


def run_spider(url, proxies, redis_movie, download_path):
    print('fetching data from {}...\n'.format(url))
    
    fail_dict = {}
    s = build_request_session()
    r = s.get(url, proxies=proxies, verify=False)

    if r.status_code == 200:
        soup = BeautifulSoup(r.content, 'lxml')
    else:
        print('get content failed')
    try:
        next_url = urljoin(url, soup.find_all('a', id='next')[0].get('href').strip())
    except IndexError as e:
        print('failed to get next page of {}, error: {}'.format(url, e))
        next_url = None

    movie_list = soup.find_all(class_='movie-box')
    if len(movie_list) == 0:
        print('no movie on {}'.format(url))
        return next_url
    for movie in movie_list:
        movie_info = dict()
        movie_url =  movie.get('href').strip()
        movie_id = movie_url.strip('/').split('/')[-1]
        if movie_in_redis(movie_id, redis_movie):
            print('{} already in redis, pass'.format(movie_id))
            continue
        movie_img_path = os.path.join(download_path, movie_id)
        if not os.path.isdir(movie_img_path):
            os.mkdir(movie_img_path)
        movie_info['id'] = movie_id
        movie_info['url'] = movie_url
        try:
            print('get detail info for {} {}'.format(movie_id, movie_url))
            r = s.get(movie_url, proxies=proxies, verify=False)
            if r.status_code != 200:
                fail_dict[movie_id] = movie_url
                print '{} failed to get detail page, status code: {}'.format(movie_id, r.status_code)
                fail_dict[movie_id] = movie_url
                continue
            movie_detail_page = BeautifulSoup(r.content, 'lxml')
            try:
                detail_parser(movie_detail_page, movie_info, movie_img_path, s, proxies, url, movie_url)
            except Exception as e:
                print('failed to parse detail for {}, error: {}'.format(movie_url, e))
                fail_dict[movie_id] = movie_url
                continue
        except Exception as e:
            print('failed to get detail page for {}, error: {}'.format(movie_url, e))
            fail_dict[movie_id] = movie_url
            continue
        redis_movie.set(movie_id, json.dumps(movie_info))
        random_sleep()
        print('set info in redis done, sleep...\n')

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
        'https://www.javbus.pw',
        # 'https://www.pornhub.com/view_video.php?viewkey=ph5b4a3a21b334c',
    ]
    redis_host = 'localhost'
    redis_port = 6379
    redis_pass = 'foobared'
    redis_db_movie = 0
    # redis_db_star = 1

    movie_detail_fail_list = {}
    try:
        redis_movie = redis.StrictRedis(host=redis_host, port=redis_port, db=redis_db_movie, password=redis_pass)
    except Exception as e:
        error_message = 'build redis connection failed, error: {}, shutting down...'.format(e)
        print(error_message)
        exit(1)

    for url in url_list:
        next_url = run_spider(url, proxies, redis_movie, download_path)
        while next_url:
            next_url = run_spider(next_url, proxies, redis_movie, download_path)
            random_sleep()

    url_list = list()
    movies = redis_movie.keys()
    for key in movies:
        try:
            movie = redis_movie.get(key)
            movie = json.loads(movie)
            movie_genre = [i.get('url') for i in movie.get('movie_genre')]
            url_list += movie_genre
            movie_star = [i.get('url') for i in movie.get('movie_star')]
            url_list += movie_star
            url_list.append(movie.get('movie_director_url'))
            url_list.append(movie.get('movie_make_url'))
            url_list.append(movie.get('movie_publish_url'))
            url_list.append(movie.get('movie_series_url'))
            url_list = [i for i in url_list if i is not None]
        except Exception as e:
            pass

    url_list = list(set(url_list))
    for url in url_list:
        next_url = run_spider(url, proxies, redis_movie, download_path)
        while next_url:
            next_url = run_spider(next_url, proxies, redis_movie, download_path)
            random_sleep()


if __name__ == "__main__":
    while True:
        main()
        time.sleep(600)