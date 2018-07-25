import redis


def build_conn(hostname, port=6379, db=0):
    conn = None
    try:
        conn = redis.StrictRedis(host=hostname, port=port, db=db)
    except Exception as e:
        print('build connection failed, error: {}'.format(e))
    return conn


conn = build_conn('localhost')
print(conn)