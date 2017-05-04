# coding=utf-8

from . import connect, cursors
import traceback
import logging


class ConnectionPool(object):

    def __init__(self, connect_conf, max_connection=1024, min_connection=5, keep_ratio=0.3, keep_threshold=10):
        self.connect_conf = connect_conf
        self.max_connection = max_connection
        self.min_connection = min_connection
        self.keep_ratio = keep_ratio
        self.keep_threshold = keep_threshold
        self.idle_pool = []
        self.status = [0, 0]
        for i in range(self.min_connection):
            conn = self._connect(self.connect_conf)
            if conn:
                self.idle_pool.append(conn)
                self.status[0] += 1
        logging.info('{} init mysql connection pool'.format(str(self.status)))

    def _connect(self, connect_conf):
        try:
            conn = connect(**connect_conf)
            def conn_close():
                self.return_connection(conn)
            conn.ret = conn_close
            logging.info('{} new mysql connection established'.format(str(self.status)))
            return conn
        except Exception as e:
            logging.error(e)
            traceback.print_exc()
            return None

    def get_connection(self):
        if len(self.idle_pool) > 0:
            conn = self.idle_pool.pop()
            self.status[0] -= 1
            self.status[1] += 1
            logging.info('{} get connection from pool'.format(str(self.status)))
            return conn
        elif sum(self.status) < self.max_connection:
            conn = self._connect(self.connect_conf)
            if conn:
                self.status[1] += 1
                return conn
        else:
            # wait
            return None

    def return_connection(self, conn):
        if self.status[0] > self.keep_threshold and self.status[0]*1.0 / self.status[1] > self.keep_ratio:
            self.status[1] -= 1
            conn.close()
            logging.info('{} close connection'.format(str(self.status)))
        else:
            self.status[0] += 1
            self.status[1] -= 1
            self.idle_pool.append(conn)
            logging.info('{} return connection to pool'.format(str(self.status)))

    def __del__(self):
        for conn in self.idle_pool:
            conn.close()
        self.status = [0, 0]
        logging.info('{} close all connection'.format(str(self.status)))


if __name__ == '__main__':

    import threading, time

    logging.basicConfig(
        level=logging.INFO, 
        format='[%(levelname)s %(asctime)s %(filename)s line:%(lineno)d] %(message)s',
        datefmt='%d %b %H:%M:%S'
    )

    mysql_conf = dict(user='root', passwd='113187Ff', db='jianxun', charset='utf8', cursorclass=cursors.DictCursor)

    pool = ConnectionPool(mysql_conf)

    def sql_get(sql):
        try:
            conn = pool.get_connection()
            with conn.cursor() as c:
                c.execute(sql)
                print(c.fetchone())
        except Exception as e:
            logging.error('{}'.format(e))
            traceback.print_exc()
        finally:
            time.sleep(12)
            conn.ret()

    sql = r"select count(*) as count from xxx"
    c = 0
    while True:
        threading.Thread(target=sql_get, args=(sql,)).start()
        c += 0.001
        time.sleep(1 + c)

    del pool