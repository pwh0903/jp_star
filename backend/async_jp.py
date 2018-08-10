import os
import re
from urllib.parse import urljoin
import json
import asyncio
from bs4 import BeautifulSoup
import redis
import aiohttp
from aiosocksy.connector import ProxyClientRequest, ProxyConnector


def movie_in_redis(movie_id, redis_movie):
    in_redis = redis_movie.exists(movie_id)
    return in_redis


def insert_cata_url_in_redis(redis_movie):
    redis_url = redis.StrictRedis(
                                    host='127.0.0.1',
                                    port=6379,
                                    db=3,
                                    password='foobared'
                                )
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
    for i in url_list:
        redis_url.set(i, 0)
    return url_list


async def get_url(url, headers):
    content = False
    conn = ProxyConnector()
    try:
        async with aiohttp.ClientSession(connector=conn, request_class=ProxyClientRequest) as session:
            async with session.get(url, proxy='socks5://127.0.0.1:1080', headers=headers) as resp:
                if resp.status == 200:
                    content = await resp.text()
                else:
                    print('failed to get {}, status {}'.format(url, resp.status))
    except Exception as e:
        print('failed to get {}, error {}'.format(url, e))
    return content


async def get_img_url(url, session, headers):
    content = False
    async with session.get(url, headers=headers) as resp:
        if resp.status == 200:
            content = await resp.read()
        else:
            print('failed to get {}, status {}'.format(url, resp.status))
    return content


async def download_img(img_name, movie_path, url, headers, session):
    try:
        content = await get_img_url(url, session, headers)
        if content is None:
            pass
        else:
            with open(os.path.join(movie_path, img_name), 'wb') as f:
                f.write(content)
    except Exception as e:
        print(e)


async def get_magnet(magnet_info, base_url, movie_url):
    magnet_list = list()
    try:
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
        content = await get_url(url, headers)
        if content is None:
            return magnet_list
        tmp_list = BeautifulSoup(content, 'lxml').find_all('tr')
        for magnet in tmp_list:
            magnet_dict = dict()
            magnet_items = magnet.find_all('td')
            magnet_dict['title'] = magnet_items[0].text.strip()
            magnet_dict['url'] = magnet_items[0].a.get('href').strip()
            magnet_dict['size'] = magnet_items[1].text.strip()
            magnet_dict['date'] = magnet_items[2].text.strip()
            magnet_list.append(magnet_dict)
    except Exception as e:
        print('failed to get magnet info of {}, error: {}'.format(movie_url, e))
    return magnet_list


async def detail_parser(movie_detail_page, movie_info, movie_img_path,
                        base_url, movie_url, headers, session):
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
    movie_info['magnet_list'] = await get_magnet(magnet_info, base_url, movie_url)

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
        movie_info['movie_director_name'] = None
        movie_info['movie_director_url'] = None

    try:
        movie_maker = movie_detail_list[movie_maker_index].a
        movie_info['movie_maker_name'] = movie_maker.text.strip()
        movie_info['movie_make_url'] = movie_maker.get('href').strip()
    except Exception as e:
        print('faile to parse movie maker, error: {}'.format(e))
        movie_info['movie_maker_name'] = None
        movie_info['movie_make_url'] = None

    try:
        movie_publish = movie_detail_list[movie_publish_index].a
        movie_info['movie_publish_name'] = movie_publish.text.strip()
        movie_info['movie_publish_url'] = movie_publish.get('href').strip()
    except Exception as e:
        print('faile to parse movie publisher, error: {}'.format(e))
        movie_info['movie_publish_name'] = None
        movie_info['movie_publish_url'] = None

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
        movie_info['movie_series_name'] = None
        movie_info['movie_series_url'] = None

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
    img_dict = dict()
    frontcover_url = movie_detail_page.find_all(class_='bigImage')[0].get('href').strip()
    img_dict['frontvocer.jpg'] = frontcover_url
    sample_imgs = [i.get('href').strip() for i in movie_detail_page.find_all('a', class_='sample-box')]
    count = 1
    for url in sample_imgs:
        img_dict['{}.jpg'.format(count)] = url
        count += 1
    await asyncio.wait([download_img(img_name, movie_img_path, url, headers, session) for img_name, url in img_dict.items()])


async def detail_spider(movie_id, movie_url, headers, download_path, base_url, session):
    movie_img_path = os.path.join(download_path, movie_id)
    if not os.path.isdir(movie_img_path):
        os.mkdir(movie_img_path)
    movie_info = dict()
    movie_info['id'] = movie_id
    movie_info['url'] = movie_url
    try:
        print('get detail info for {} {}'.format(movie_id, movie_url))
        content = await get_url(movie_url, headers)
        if content is None:
            return None
        movie_detail_page = BeautifulSoup(content, 'lxml')
        try:
            await detail_parser(movie_detail_page, movie_info, movie_img_path, base_url, movie_url, headers, session)
        except Exception as e:
            print('failed to parse detail for {}, error: {}'.format(movie_url, e))
    except Exception as e:
        print('failed to get detail page for {}, error: {}'.format(movie_url, e))
    return movie_id, movie_info


async def run_spider(url, redis_movie, download_path):
    print('fetching data from {}...\n'.format(url))
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36'
    }
    next_url = None
    content = await get_url(url, headers)

    if content:
        soup = BeautifulSoup(content, 'lxml')
    else:
        return next_url

    try:
        next_url = urljoin(url, soup.find_all('a', id='next')[0].get('href').strip())
    except IndexError as e:
        print('failed to get next page of {}, error: {}'.format(url, e))

    movie_list = soup.find_all(class_='movie-box')
    if len(movie_list) == 0:
        print('no movie on {}'.format(url))
        return next_url

    movies = dict()
    for movie in movie_list:
        movie_url = movie.get('href').strip()
        movie_id = movie_url.strip('/').split('/')[-1]
        if movie_in_redis(movie_id, redis_movie):
            print('{} already in redis, pass'.format(movie_id))
        else:
            movies[movie_id] = movie_url

    if len(movies) > 0:
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(verify_ssl=False)) as session:
            done, pending = await asyncio.wait([detail_spider(movie_id, movie_url, headers, download_path, url, session) for movie_id, movie_url in movies.items()])
        for i in done:
            redis_movie.set(i.result()[0], json.dumps(i.result()[1]))
    return next_url


async def main():
    download_path = './download_1'
    if not os.path.isdir(download_path):
        os.mkdir(download_path)

    url_list = [
        'https://www.javbus.pw',
    ]

    redis_host = 'localhost'
    redis_port = 6379
    redis_pass = 'foobared'
    redis_db_movie = 0

    try:
        redis_movie = redis.StrictRedis(host=redis_host,
                                        port=redis_port,
                                        db=redis_db_movie,
                                        password=redis_pass)
    except Exception as e:
        error_message = 'build redis connection failed, error: {}'.format(e)
        print(error_message)
        return False

    redis_url = redis.StrictRedis(
                                host=redis_host,
                                port=redis_port,
                                db=3,
                                password=redis_pass
                                )
    redis_url_list = [i.decode('utf-8') for i in redis_url.keys()]
    # redis_url_list = insert_cata_url_in_redis(redis_movie)
    url_list += redis_url_list
    print('{} start url in redis'.format(len(url_list)))
    url_set_list = list()
    for num in range(len(url_list)):
        if num % 5 == 0:
            if num + 5 > len(url_list):
                url_set_list.append(url_list[num: len(url_list)])
            else:
                url_set_list.append(url_list[num: num+5])

    for url_sub_list in url_set_list:
        try:
            done, pending = await asyncio.wait([run_spider(url, redis_movie, download_path) for url in url_sub_list])
            tmp_next_url = list()
            for i in done:
                if i.result():
                    tmp_next_url.append(i.result())

            while len(tmp_next_url) > 0:
                done, pending = await asyncio.wait([run_spider(url, redis_movie, download_path) for url in tmp_next_url])
                tmp_next_url = list()
                for i in done:
                    if i.result():
                        tmp_next_url.append(i.result())
            # print('sleep 100 seconds')
            # await asyncio.sleep(100)
        except Exception as e:
            pass


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
