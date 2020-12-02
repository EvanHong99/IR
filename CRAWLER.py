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
        empty_times = 0
        new_urls=[]
        while True:
            try:
                # 保证所有线程拿到的url不是相同的url
                threadLock.acquire()
                # 原来留有的url+新捕获的url
                unused_url = pd.read_csv("pages/_unused_url.csv", encoding='utf-8')
                unused_url=unused_url.append(new_urls)
                
                used_url = pd.read_csv("pages/_used_url.csv", encoding='utf-8')
                # print(type(unused_url), unused_url)
                # print(used_url)

                # 若出现错误，进行只读操作，不会写入新数据
                if not unused_url.size:
                    if empty_times == 1000:
                        if threadLock.locked():
                            threadLock.release()
                        break
                    empty_times += 1
                    raise Exception("unused url list is null")
                # 将计数器置零，防止累积
                empty_times=0
                url = unused_url.head(1)
                unused_url.drop(index=unused_url.index[0], inplace=True)
                # unused_url.pop(url.index)

                # 不论是否获取成功，直接加入used
                used_url = used_url.append(url.rename(columns={'unused': 'used'}), ignore_index=True)
                # print(used_url)

                # 立即写入，保证下一个线程或得到不一样的url。可以保留header，因为每次都是覆盖，对比下面的df
                used_url.to_csv("pages/_used_url.csv", encoding='utf-8', index=False)
                unused_url.to_csv("pages/_unused_url.csv", encoding='utf-8', index=False)
                threadLock.release()

                url = url.values.tolist()[0]
                if url not in list(used_url.values[:, 0]):
                    url=url[0]
                    page = get_page(url, config.HEADERS)
                    if not page:
                        logger.warning(url + "  is empty")
                        time.sleep(3)
                        continue
                    soup = BeautifulSoup(page, 'lxml', from_encoding='utf-8')  # html.parser是解析器，也可是lxml

                    # 将各种标签内的文字存入一个list
                    strings = []
                    for tag in ['title', 'a', 'div', 'li', 'span', 'p']:
                        strings += [i.string.strip() for i in soup.select(tag) if i.string and len(i.string) > 1]
                    str_df = pd.DataFrame([[url, strings]], columns=['base_url', 'keywords'])

                    # 将链接关系（包括锚文本）写入csv文件
                    links = pd.DataFrame(columns=['base url', 'hook', 'linked url'])
                    for item in soup.select('a'):
                        href = item.get('href')
                        if href and href[:4] == 'http':
                            links = links.append(
                                pd.DataFrame([[url, item.string, href]], columns=['base url', 'hook', 'linked url']),
                                ignore_index=True)
                            # 若该url未被捕获过，那么将它加入到待爬取列表。若程序终止那么可能导致该页面捕获的所有url未能存入磁盘
                            if href not in used_url:
                                new_urls.append(href)

                    # 锁读写
                    #  write file into disk, perhaps two files,
                    #  one contains strings (txt), another contains url-name pairs
                    # 不包含header，否则由于是a模式会导致header重复
                    threadLock1.acquire()
                    str_df.to_csv("pages/_page_contents.csv", encoding='utf-8', index=False, mode='a',
                                  header=False)
                    links.to_csv(r"pages/_links.csv", encoding='utf-8', index=False, mode='a',
                              header=False)
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

    threads = []

    # 创建新线程
    for i in range(7):
        threads.append(myThread(i, "thread-" + str(i), i))

    # 开启新线程
    for thread in threads:
        thread.start()

    # 等待所有线程完成
    for t in threads:
        t.join()
    print("Exiting Main Thread")
