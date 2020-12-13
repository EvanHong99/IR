# -*-coding:utf-8-*-
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
from contextlib import contextmanager
import urllib.parse
import lxml
import BSBI
import os
import pickle as pkl

formatter = logging.Formatter("%(asctime)s|%(name)-12s|%(message)s", "%F %T")
logger = logging.getLogger("default")
logger.setLevel(logging.INFO)

log_file_handler = logging.FileHandler(filename='logs/new_log1.txt', encoding='utf-8')
log_file_handler.setFormatter(formatter)
log_file_handler.setLevel(logging.WARNING)

log_file_handler1 = logging.FileHandler(filename='logs/new_info_log1.txt', encoding='utf-8')
log_file_handler1.setFormatter(formatter)
log_file_handler1.setLevel(logging.INFO)

log_mem_handler = logging.handlers.MemoryHandler(100, logging.INFO)
log_mem_handler.setFormatter(formatter)

logger.addHandler(log_mem_handler)
logger.addHandler(log_file_handler1)
logger.addHandler(log_file_handler)

threadLock = threading.Lock()
threadLock1 = threading.Lock()
thread_local = threading.local()

unused_url = []
used_url = []





@contextmanager
def acquire(*locks):
    # sort locks by object identifier
    locks = sorted(locks, key=lambda x: id(x))

    # make sure lock order of previously acquired locks is not violated
    acquired = getattr(thread_local, 'acquired', [])
    if acquired and (max(id(lock) for lock in acquired) >= id(locks[0])):
        raise RuntimeError('Lock Order Violation')

    # Acquire all the locks
    acquired.extend(locks)
    thread_local.acquired = acquired

    try:
        for lock in locks:
            lock.acquire()
        yield
    finally:
        for lock in reversed(locks):
            if lock.locked():
                lock.release()
        del acquired[-len(locks):]


class myThread(threading.Thread):  # 继承父类threading.Thread
    def __init__(self, threadID, name, counter):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.counter = counter

    def run(self):
        global unused_url
        global used_url
        global connection

        # 重新自己维护的映射，使得文件输出名为id，用save函数保存该map，直接用数据库表行id来表示
        doc_id=0
        empty_times = 0
        # content_to_file_times=0
        new_urls = []
        while True:
            if not unused_url:
                empty_times += 1
                if (empty_times > 100):
                    logger.warning("unused url list is null")
                    logger.critical("unused url is empty")
                    exit(0)
                continue
            # 每次都初始化防止累积
            empty_times = 0
            if_new = True
            try:
                # 保证所有线程拿到的url不是相同的url
                """取出一条url，同时更新两张表"""
                with acquire(threadLock, threadLock1):
                    # 判断当前连接是否正常，否则自动重连
                    reconnect_times = 1
                    while True:
                        try:
                            connection.ping()

                            break
                        except pymysql.OperationalError:
                            if reconnect_times % 10 == 0 and reconnect_times < 100:
                                logger.warning("access to db failed!")
                                # time.sleep(5)
                            elif reconnect_times >= 100:
                                cursor.close()
                                logger.error(traceback.format_exc())
                                logging.error(traceback.format_exc())
                                logger.critical("reconnecting to db failed for too many times, exit proc")
                                if threadLock.locked():
                                    threadLock.release()
                                if threadLock1.locked():
                                    threadLock1.release()
                                exit()

                            reconnect_times += 1
                            connection.ping(True)

                    # 新捕获的url加入到unused
                    if new_urls:
                        uniq_urls = [i for i in set(new_urls) if i not in unused_url and i not in used_url]
                        connection.ping(True)
                        cursor.executemany("insert into unused_url (unused) value (%s);", uniq_urls)
                        unused_url = unused_url + uniq_urls
                        # 防止内存爆炸程序直接无法继续，但又不能太少导致去重不干净
                        # 新版本忽略该段
                        # if len(unused_url) > 100000:
                        #     logger.info("*********************\n\nreload from db\n\n************************")
                        #     cursor.execute("select unused from _unused_url group by unused;")
                        #     unused_url = list(map(lambda d: d.get('unused'), cursor.fetchall()))

                    # 取出unused顶部url，然后将其立即加入used
                    # returns a dict, loc to string
                    url = unused_url.pop(0)
                    connection.ping(True)
                    cursor.execute("delete from unused_url where unused=(%s);", url)

                    # 为了保证同步，只能通过这种方式传值
                    if url in used_url:
                        if_new = False
                    else:
                        used_url.append(url)
                        connection.ping(True)
                        cursor.execute("insert into used_url(used) value (%s);", [url])
                        cursor.execute("select id from used_url where used=(%s)",url)
                        doc_id=cursor.fetchall()[0]['id']

                # TODO 检查pdf doc爬取正确性
                # 未被爬去过,且是南开站点
                if if_new:
                    page = get_page(url, config.HEADERS)
                    if not page:
                        logger.warning(url + "  is empty")
                        time.sleep(3)
                        continue

                    soup = BeautifulSoup(page, 'lxml')  # html.parser是解析器，也可是lxml
                    # soup = BeautifulSoup(page, 'lxml', from_encoding='utf-8')  # html.parser是解析器，也可是lxml

                    # 将各种标签内的文字存入一个list,新版本为一个string
                    # 文本内容
                    # TODO: 对title单独建索引
                    strings = ''
                    for tag in ['title', 'a', 'div', 'li', 'span', 'p']:
                        strings = strings + ' '.join(
                            [i.string.strip() for i in soup.select(tag) if i.string and len(i.string) > 1]) + ' '

                    # 将链接关系（包括锚文本）写入csv文件，链接内容
                    links = pd.DataFrame(columns=['base url', 'hook', 'linked url'])
                    pdfs = pd.DataFrame(columns=['base url', 'hook', 'linked url'])
                    docs = pd.DataFrame(columns=['base url', 'hook', 'linked url'])
                    for item in soup.select('a'):
                        href = item.get('href')

                        # 增加的url
                        # 基与nankai的url
                        if href and ('nankai.edu.cn' in href) and href != url and ('bbs' not in href):
                            links = links.append(
                                pd.DataFrame([[url, item.string, href]], columns=['base url', 'hook', 'linked url']),
                                ignore_index=True)
                            # 若该url未被捕获过且未重复，那么将它加入到待爬取列表。若程序终止那么可能导致该页面捕获的所有url未能存入磁盘
                            # 可能因为没有加锁导致数据不同步，故这里舍弃判断，直接加入，待到有锁的时候进行完备的判断
                            new_urls.append(href)
                        # 相对url，有二次跳转的问题
                        if href and (href[0] == '/' and '.htm' in href[-6:] and ('.htm' not in url[-6:])):
                            links = links.append(
                                pd.DataFrame([[url, item.string, url + href]],
                                             columns=['base url', 'hook', 'linked url']),
                                ignore_index=True)
                            new_urls.append(url + href)
                        # pdf
                        if href and (href[0] == '/' and '.pdf' in href[-5:]):
                            pdfs = pdfs.append(
                                pd.DataFrame([[url, item.string, url + href]],
                                             columns=['base url', 'hook', 'linked url']),
                                ignore_index=True)
                        # doc
                        if href and (href[0] == '/' and '.doc' in href[-5:]):
                            docs = docs.append(
                                pd.DataFrame([[url, item.string, url + href]],
                                             columns=['base url', 'hook', 'linked url']),
                                ignore_index=True)

                    #  write file into disk, perhaps two files,
                    #  one contains strings (txt), another contains url-name pairs
                    # 不包含header，否则由于是a模式会导致header重复
                    with acquire(threadLock, threadLock1):
                        '''负载均衡，分块存储，但是没有保证连续性，但是一定保证均衡
                        str_df.to_csv("pages/contents"+str(content_to_file_times%8)+".csv", encoding='utf-8', index=False, mode='a',
                                      header=False)'''
                        '''# 查阅后发现csv无上限，那没事了，一个文件存到天黑
                        str_df.to_csv("pages/_new_page_contents.csv", encoding='utf-8',
                                      index=False, mode='a',
                                      header=False)'''
                        # 发现还是采用一个网页一个文件的形式最好，可以对接之前的作业，傻了傻了
                        # url=urllib.parse.unquote(url)
                        # 对于不符合windows文件命名规范的直接抛弃
                        repl_url = "pages/12_13_16_42/" +str(doc_id)+".txt"
                        # repl_url = "pages/new_page_txt/" + url.replace('/', "+").replace(':', '-').replace('=',
                        #                                                                                    ',').replace(
                        #     '?', '\'') + ".txt"
                        # repl_url=u"pages/page_txt/"+url+".txt"
                        # with open(file=repl_url.decode('utf-8'),mode='w',encoding='utf-8') as fw:
                        with open(file=repl_url, mode='w', encoding='utf-8') as fw:
                            fw.write(strings)
                            fw.close()

                        links.to_sql('links', engine1, 'everytinku', 'append', index=False)
                        pdfs.to_sql('pdfs', engine1, 'everytinku', 'append', index=False)
                        docs.to_sql('docs', engine1, 'everytinku', 'append', index=False)

                        # logger.debug("write" + str(self.threadID) + url)

                # 若url已经获取过，则跳过，申请锁获取下一个url
                else:
                    continue
                time.sleep(2)
            except Exception:
                logger.error(traceback.format_exc())
                # logging.error(traceback.format_exc())
                if threadLock.locked():
                    threadLock.release()
                if threadLock1.locked():
                    threadLock1.release()
                # time.sleep(5)


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
                time.sleep(1)
                logger.error(url + "nothing catched\n")

            return response.text
        # raise Exception("get page failed")
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
            cursor.execute("select used from used_url;")
            used_url = list(map(lambda d: d.get('used'), cursor.fetchall()))

            cursor.execute("select unused from unused_url group by unused;")
            unused_url = list(map(lambda d: d.get('unused'), cursor.fetchall()))

            threads = []

            # 创建新线程
            for i in range(config.THREADS_NUM):
            # for i in range(1):
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
        cursor.close()
        connection.close()
        logger.error(traceback.format_exc())
        exit(1)
