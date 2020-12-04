import requests
import time
import logging
import logging.handlers
from configs import config
import threading
import re
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import traceback
import pymysql
from sqlalchemy import engine
import mysqlx.errors

formatter = logging.Formatter("%(asctime)s|%(name)-12s|%(message)s", "%F %T")
logger = logging.getLogger("default")

log_file_handler = logging.FileHandler(filename='logs/log.txt', encoding='utf-8')
log_file_handler.setFormatter(formatter)
log_mem_handler = logging.handlers.MemoryHandler(100, logging.INFO)
log_mem_handler.setFormatter(formatter)

logger.addHandler(log_mem_handler)
logger.addHandler(log_file_handler)

threadLock = threading.Lock()
threadLock1 = threading.Lock()

unused_url = []
used_url = []


class myThread(threading.Thread):  # 继承父类threading.Thread
    def __init__(self, threadID, name, counter):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.counter = counter

    def run(self):
        global unused_url
        global used_url
        empty_times = 0
        new_urls = []
        while True:
            if not unused_url:
                empty_times += 1
                if (empty_times > 100):
                    logger.warning("unused url list is null")
                    exit()
            # 每次都初始化防止累积
            empty_times = 0
            if_new = True
            try:
                # 保证所有线程拿到的url不是相同的url
                """取出一条url，同时更新两张表"""
                threadLock.acquire()
                # 判断当前连接是否正常，否则自动重连
                reconnect_times=1
                while True:
                    try:

                        connection.ping()

                        break
                    except pymysql.OperationalError:
                        if reconnect_times%10==0 and reconnect_times<100:
                            logger.warning("access to db failed!")
                            time.sleep(5)
                        elif reconnect_times>=100:
                            cursor.close()
                            logger.error(traceback.format_exc())
                            logging.error(traceback.format_exc())
                            logger.critical("reconnecting to db failed for too many times, exit proc")
                            if threadLock.locked():
                                threadLock.release()
                            if threadLock1.locked():
                                threadLock1.release()
                            exit()

                        reconnect_times+=1
                        connection.ping(True)

                # 新捕获的url加入到unused
                if new_urls:
                    uniq_urls = [i for i in set(new_urls) if i not in (unused_url and used_url)]
                    cursor.executemany("insert into _unused_url (unused) value (%s);", uniq_urls)
                    unused_url = unused_url + uniq_urls
                    # 防止内存爆炸程序直接无法继续，但又不能太少导致去重不干净
                    if len(unused_url) > 100000:
                        logger.info("*********************\n\nreload from db\n\n************************")
                        cursor.execute("select unused from _unused_url limit 60000;")
                        unused_url = list(map(lambda d: d.get('unused'), cursor.fetchall()))

                # 取出unused顶部url，然后将其立即加入used
                # returns a dict, loc to string
                url = unused_url.pop(0)
                cursor.execute("delete from _unused_url where unused=(%s);",url)

                # 为了保证同步，只能通过这种方式传值
                if url in used_url:
                    logger.info("*********************\n\nused url\n\n************************")
                    if_new = False
                else:
                    used_url.append(url)
                    cursor.execute("insert into _used_url(used) value (%s);", [url])
                if threadLock.locked():
                    threadLock.release()


                # 未被爬去过,且是南开站点
                if if_new:
                    # if type(url)!=str:
                    #     logger.error("url is not a string, sleep for a sec")
                    #     time.sleep(1)
                    page = get_page(url, config.HEADERS)
                    if not page:
                        logger.warning(url + "  is empty")
                        time.sleep(3)
                        continue

                    soup = BeautifulSoup(page, 'lxml', from_encoding='utf-8')  # html.parser是解析器，也可是lxml

                    # 将各种标签内的文字存入一个list,新版本为一个string
                    strings = ''
                    for tag in ['title', 'a', 'div', 'li', 'span', 'p']:
                        strings = strings + '\t'.join(
                            [i.string.strip() for i in soup.select(tag) if i.string and len(i.string) > 1]) + '\t'
                    str_df = pd.DataFrame([[url, strings]], columns=['base_url', 'keywords'])

                    # 将链接关系（包括锚文本）写入csv文件
                    links = pd.DataFrame(columns=['base url', 'hook', 'linked url'])
                    for item in soup.select('a'):
                        href = item.get('href')
                        if href and href[:4] == 'http' and ('nankai.edu.cn' in href) and href != url:
                            links = links.append(
                                pd.DataFrame([[url, item.string, href]], columns=['base url', 'hook', 'linked url']),
                                                ignore_index=True)
                            # 若该url未被捕获过且未重复，那么将它加入到待爬取列表。若程序终止那么可能导致该页面捕获的所有url未能存入磁盘
                            # 可能因为没有加锁导致数据不同步，故这里舍弃判断，直接加入，待到有锁的时候进行完备的判断
                            new_urls.append(href)

                    #  write file into disk, perhaps two files,
                    #  one contains strings (txt), another contains url-name pairs
                    # 不包含header，否则由于是a模式会导致header重复
                    threadLock1.acquire()
                    str_df.to_csv("pages/_new_page_contents.csv", encoding='utf-8', index=False, mode='a',
                                  header=False)

                    links.to_sql('_links', engine1, 'everytinku', 'append', index=False)

                    # cursor.executemany("insert into _links(`base url`, hook, `linked url`) values (?,?,?);",)
                    if threadLock1.locked():
                        threadLock1.release()
                    logger.debug("write" + str(self.threadID) + url)

                # 若url已经获取过，则跳过，申请锁获取下一个url
                else:
                    continue
                time.sleep(3)
            except Exception:
                logger.error(traceback.format_exc())
                logging.error(traceback.format_exc())
                if threadLock.locked():
                    threadLock.release()
                if threadLock1.locked():
                    threadLock1.release()
                time.sleep(5)


def get_page(url, headers):
    """

    :param url:
    :param headers:
    :return:
    """
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            response.encoding = 'utf-8'
            # 避免ip被封
            if response.text == '':
                time.sleep(3)
                logger.error("nothing catched\n")

            return response.text
        raise Exception("get page failed")
    except requests.RequestException:
        logging.error("get page failed")
        return None


# def parse_page(page):


if __name__ == '__main__':
    # global unused_url
    # global used_url
    try:
        # 连接数据库
        engine1 = engine.create_engine("mysql+pymysql://root:Qazwsxedcrfv0957@localhost:3306/everytinku")
        connection = pymysql.connect(host='localhost',
                                     port=3306,
                                     user='root',
                                     password='Qazwsxedcrfv0957',
                                     db='everytinku',
                                     charset='utf8',
                                     cursorclass=pymysql.cursors.DictCursor,


                                     )
        connection.autocommit(True)
        with connection.cursor() as cursor:
            # 先初始化内存，维护两个列表。所有线程共享
            cursor.execute("select used from _used_url;")
            used_url = list(map(lambda d: d.get('used'), cursor.fetchall()))

            cursor.execute("select unused from _unused_url limit 50000;")
            unused_url = list(map(lambda d: d.get('unused'), cursor.fetchall()))

            threads = []

            # 创建新线程
            for i in range(config.THREADS_NUM):
                threads.append(myThread(i, "thread-" + str(i), i))

            # 开启新线程
            for thread in threads:
                thread.start()

            # 等待所有线程完成
            for t in threads:
                t.join()
            print("Exiting Main Thread")
            cursor.close()
    except Exception:
        logger.error(traceback.format_exc())
        exit(1)
