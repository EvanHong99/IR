import requests
import time
import logging
import logging.handlers
from configs import config
import threading
import csv
import re
from bs4 import BeautifulSoup
import bs4
import pandas as pd

formatter = logging.Formatter("%(asctime)s|%(name)-12s|%(message)s", "%F %T")
logger = logging.getLogger("default")

log_file_handler = logging.FileHandler(filename='logs/log.txt', encoding='utf-8')
log_file_handler.setFormatter(formatter)
log_mem_handler = logging.handlers.MemoryHandler(100, logging.WARNING)
log_mem_handler.setFormatter(formatter)

logger.addHandler(log_mem_handler)
logger.addHandler(log_file_handler)

threadLock = threading.Lock()
unused_url = config.SEED_URL * 10
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
        try:
            threadLock.acquire()
            print(self.threadID)
            raise Exception
        except Exception:
            print("exception")
            time.sleep(3)
            print("release")
            threadLock.release()


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


# def parse_page(page):


if __name__ == '__main__':
    # threads = []
    #
    # # 创建新线程
    # for i in range(2):
    #     threads.append(myThread(i, "thread-" + str(i), i))
    #
    # # 开启新线程
    # for thread in threads:
    #     thread.start()
    #
    # # 等待所有线程完成
    # for t in threads:
    #     t.join()
    # print("Exiting Main Thread")
    # unused_url = pd.read_csv("pages/_unused_url.csv", "r", encoding='utf-8')
    # used_url = pd.read_csv("pages/_used_url.csv", "r", encoding='utf-8')
    # print(type(unused_url))
    # print(unused_url)
    # print(used_url)

    df=pd.DataFrame([1,2,3],columns=['a'])
    df1=pd.DataFrame([1,2,3],columns=['b'])
    t=df.head(1).rename(columns={'a':'b'})
    df1=df1.append(t,ignore_index=True)
    print(df1)

