from elasticsearch import Elasticsearch
import datetime
import redis
import re
import json
import uuid
from pprint import pprint


es_host = '127.0.0.1'
es_port = 9200

es = Elasticsearch(es_host, port=es_port)

redis_host = 'localhost'
redis_port = 6379
redis_pass = 'foobared'
redis_db_movie = 0

try:
    redis_movie = redis.StrictRedis(host=redis_host, port=redis_port, db=redis_db_movie, password=redis_pass)
except Exception as e:
    error_message = 'build redis connection failed, error: {}, shutting down...'.format(e)
    print(error_message)
    exit(1)

movie_id_list = redis_movie.keys()
for movie_id in movie_id_list:
    movie_data = dict()
    movie = redis_movie.get(movie_id)
    try:
        movie_info = json.loads(movie)
    except Exception as e:
        redis_movie.delete(movie_id)
        continue
    movie_info.pop('id')

    if movie_info.get('title'):
        try:
            movie_data['title'] = movie_info.get('title').decode('unicode-escape')
        except AttributeError as e:
            try:
                tmp = movie_info.get('title')
                tmp = '\\' + '\\'.join(re.findall('\w+', tmp))
                tmp = bytes(tmp, encoding='utf-8').decode('unicode-escape')
                movie_data['title'] = tmp
            except Exception as e:
                movie_data['title'] = movie_info.get('title')
    else:
        movie_data['title'] = None
    if movie_info.get('title'):
        movie_info.pop('title')

    if movie_info.get('movie_director_name'):
        try:
            movie_data['movie_director_name'] = movie_info.get('movie_director_name').decode('unicode-escape')
        except AttributeError as e:
            try:
                tmp = movie_info.get('movie_director_name')
                tmp = '\\' + '\\'.join(re.findall('\w+', tmp))
                tmp = bytes(tmp, encoding='utf-8').decode('unicode-escape')
                movie_data['movie_director_name'] = tmp
            except Exception as e:
                movie_data['movie_director_name'] = movie_info.get('movie_director_name')
    else:
        movie_data['movie_director_name'] = None
    if movie_info.get('movie_director_name'):
        movie_info.pop('movie_director_name')

    if movie_info.get('movie_publish_name'):
        try:
            movie_data['movie_publish_name'] = movie_info.get('movie_publish_name').decode('unicode-escape')
        except AttributeError as e:
            try:
                tmp = movie_info.get('movie_publish_name')
                tmp = '\\' + '\\'.join(re.findall('\w+', tmp))
                tmp = bytes(tmp, encoding='utf-8').decode('unicode-escape')
                movie_data['movie_publish_name'] = tmp
            except Exception as e:
                movie_data['movie_publish_name'] = movie_info.get('movie_publish_name')

    else:
        movie_data['movie_publish_name'] = None
    if movie_info.get('movie_publish_name'):
        movie_info.pop('movie_publish_name')

    if movie_info.get('movie_maker_name'):
        try:
            movie_data['movie_maker_name'] = movie_info.get('movie_maker_name').decode('unicode-escape')
        except AttributeError as e:
            try:
                tmp = movie_info.get('movie_maker_name')
                tmp = '\\' + '\\'.join(re.findall('\w+', tmp))
                tmp = bytes(tmp, encoding='utf-8').decode('unicode-escape')
                movie_data['movie_maker_name'] = tmp
            except Exception as e:
                movie_data['movie_maker_name'] = movie_info.get('movie_maker_name')
    else:
        movie_data['movie_maker_name'] = None
    if movie_info.get('movie_maker_name'):
        movie_info.pop('movie_maker_name')

    if movie_info.get('movie_series_name'):
        try:
            movie_data['movie_series_name'] = movie_info.get('movie_series_name').decode('unicode-escape')
        except AttributeError as e:
            try:
                tmp = movie_info.get('movie_series_name')
                tmp = '\\' + '\\'.join(re.findall('\w+', tmp))
                tmp = bytes(tmp, encoding='utf-8').decode('unicode-escape')
                movie_data['movie_series_name'] = tmp
            except Exception as e:
                movie_data['movie_series_name'] = movie_info.get('movie_series_name')
    else:
        movie_data['movie_series_name'] = None
    if movie_info.get('movie_series_name'):
        movie_info.pop('movie_series_name')

    movie_data['movie_genre'] = list()
    if movie_info.get('movie_genre'):
        for i in movie_info.get('movie_genre'):
            try:
                tmp_name = i.get('title').decode('unicode-escape')
            except AttributeError as e:
                try:
                    tmp = movie_info.get('title')
                    tmp = '\\' + '\\'.join(re.findall('\w+', tmp))
                    tmp = bytes(tmp, encoding='utf-8').decode('unicode-escape')
                    tmp_name = tmp
                except Exception as e:
                    tmp_name = i.get('title')
            movie_data['movie_genre'].append(tmp_name)
        movie_info.pop('movie_genre')

    movie_data['movie_star'] = list()
    if movie_info.get('movie_star'):
        for i in movie_info.get('movie_star'):
            try:
                tmp_name = i.get('name').decode('unicode-escape')
            except AttributeError as e:
                try:
                    tmp = movie_info.get('name')
                    tmp = '\\' + '\\'.join(re.findall('\w+', tmp))
                    tmp = bytes(tmp, encoding='utf-8').decode('unicode-escape')
                    tmp_name = tmp
                except Exception as e:
                    tmp_name = i.get('name')
            movie_data['movie_star'].append(tmp_name)
        movie_info.pop('movie_star')

    try:
        movie_data['movie_pub_date'] = datetime.datetime.strptime(movie_info.get('movie_pub_data'), '%Y-%m-%d').strftime('%Y-%m-%dT00:00:00Z')
    except Exception as e:
        movie_data['movie_pub_date'] = None
    if movie_info.get('movie_pub_data'):
        movie_info.pop('movie_pub_data')

    movie_data.update(movie_info)

    es.index(index='jp_movie', doc_type='movie',id=movie_id.lower(), body=movie_data)
    # pprint(movie_info)
    # break



