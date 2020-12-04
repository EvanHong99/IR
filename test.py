import requests
import time
import logging
import logging.handlers
from configs import config
import threading
import pymysql
import pandas as pd
from sqlalchemy import engine

formatter = logging.Formatter("%(asctime)s|%(name)-12s|%(message)s", "%F %T")
logger = logging.getLogger("default")

log_file_handler = logging.FileHandler(filename='logs/log.txt', encoding='utf-8')
log_file_handler.setFormatter(formatter)
log_mem_handler = logging.handlers.MemoryHandler(100, logging.WARNING)
log_mem_handler.setFormatter(formatter)

logger.addHandler(log_mem_handler)
logger.addHandler(log_file_handler)

threadLock = threading.Lock()
unused_url = []
used_url = []


class myThread(threading.Thread):  # 继承父类threading.Thread
    def __init__(self, threadID, name, counter):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.counter = counter


    # def run(self):
    #     url = unused_url.pop(0)
    #     try:
    #         with open("pages/_links.csv", "w", encoding='utf-8') as url_writer:
    #             times = 0
    #             while unused_url:
    #                 if url not in used_url:
    #                     page = get_page(url, config.HEADERS)
    #                     if not page:
    #                         # log_mem_handler.acquire()
    #                         logger.warning(url + "  is empty")
    #                         time.sleep(3)
    #                         # log_mem_handler.release()
    #                         continue
    #                     soup = BeautifulSoup(page, 'lxml')  # html.parser是解析器，也可是lxml
    #
    #                     strings=[]
    #                     for tag in ['title', 'a', 'div', 'li', 'span', 'p']:
    #                         strings.append([i.string.strip() for i in soup.select(tag) if i.string and len(i.string) > 1])
    #
    #                     url_list = list()
    #                     for item in soup.select('a'):
    #                         if item.get('href')[:4] == 'http':
    #                             url_list.append((item.get('href'), item.string))
    #                     print(url_list)
    #                     used_url.append(url)
    #                     logger.debug("write" + str(self.threadID) + url)
    #                     url = unused_url.pop(0)
    #
    #                 time.sleep(3)
    #             url_writer.close()
    #     except Exception:
    #         # log(Exception,url+"failed")
    #         logger.error(url + " failed")
    #         time.sleep(3)
    #     finally:
    #         url_writer.close()
    def run(self):
        global unused_url
        global used_url
        new_urls=[1]
        for i in range(10):
            threadLock.acquire()
            unused_url = unused_url + new_urls
            used_url = unused_url + new_urls
            print(unused_url)
            print(used_url)
            print()
            threadLock.release()
            time.sleep(1)



def get_page(url, headers, times=0):
    """

    :param url:
    :param headers:
    :param times: repeat times, max=3
    :return:
    """
    try:
        if times == 3:
            return None
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            response.encoding = 'utf-8'
            # 避免ip被封
            if response.text == '':
                time.sleep(300)
                log_mem_handler.handle("nothing catched\n")
                return get_page(url, headers, times + 1)
            return response.text
        raise Exception("get page failed")
    except requests.RequestException:
        logging.error("get page failed")
        return None


def test_loop():
    connection = pymysql.connect(host='localhost',
                                 port=3306,
                                 user='root',
                                 password='Qazwsxedcrfv0957',
                                 db='everytinku',
                                 charset='utf8',
                                 cursorclass=pymysql.cursors.DictCursor
                                 )
    connection.autocommit(True)
    with connection.cursor() as cursor:
        print("sql")
        t1=time.time()
        for i in range(100):

            cursor.execute("select unused from _unused_url where unused='http://fdy.nankai.edu.cn';")

        t2=time.time()
        print(t2-t1)
        print("if in")
        cursor.execute("select unused from _unused_url;")
        unused_url = list(map(lambda d: d.get('unused'), cursor.fetchall()))

        t1 = time.time()
        for i in range(100):
            if 'http://fdy.nankai.edu.cn' in unused_url:
                pass
        t2 = time.time()
        print(t2 - t1)

def test_sql():
    connection = pymysql.connect(host='localhost',
                                 port=3306,
                                 user='root',
                                 password='Qazwsxedcrfv0957',
                                 db='everytinku',
                                 charset='utf8',
                                 cursorclass=pymysql.cursors.DictCursor
                                 )
    connection.autocommit(True)
    with connection.cursor() as cursor:
        # [('1','1','1'),('2','2','2')]
        cursor.execute("select * from _unused_url limit 1000;")
        unused_url= list(map(lambda d: d.get('unused'), cursor.fetchall()))
        cursor.execute("select used from _used_url;")
        used_url = list(map(lambda d: d.get('used'), cursor.fetchall()))
        righttimes=0
        for i in range(1000):
            url=unused_url.pop(0)
            cursor.execute("select * from _used_url where used=(%s)",url)
            res=cursor.fetchone()
            if res:
                res=['used']
            if (res and (url in used_url)) or (not res and (url not in used_url)):
                righttimes+=1

        print(righttimes)


    # df=pd.DataFrame([['a','a','a'],['a','a','a'],['a','a','a']], columns=['base url', 'hook', 'linked url'])
    # df.to_sql('test',engine.create_engine("mysql+pymysql://root:Qazwsxedcrfv0957@localhost:3306/everytinku"),'everytinku','append',index=False)


if __name__ == '__main__':

    # threads = []
    #
    # # 创建新线程
    # for i in range(7):
    #     threads.append(myThread(i, "thread-" + str(i), i))
    #
    # # 开启新线程
    # for thread in threads:
    #     thread.start()
    #
    # # 等待所有线程完成
    # for t in threads:
    #     t.join()
    # test_loop()
    test_sql()



