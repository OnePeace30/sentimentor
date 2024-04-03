import psycopg2 as ps
from contextlib import contextmanager
from decouple import config



@contextmanager
def connection():
    try:
        con = ps.connect(
            dbname=config('FB_NAME', cast=str), 
            user=config('FB_USER', cast=str), 
            password=config('FB_PWD', cast=str), 
            host=config('FB_HOST', cast=str)
        )
        cursor = con.cursor()
        yield cursor
    except Exception as err:
        raise
    finally:
        con.commit()
        con.close()