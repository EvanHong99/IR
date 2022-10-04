import gensim
import os
import networkx as nx
import traceback
import pandas as pd
import numpy as np
from collections import defaultdict
import BSBI
import jieba
from configs import my_config
import pymysql
import logging
import logging.handlers


def cul_pagerank():
    """
    计算PageRank并存入数据库
    :return: 数据存储于PageRank数据库
    """
    G = nx.DiGraph()

    try:
        csv = pd.read_csv('tables/everytinku_links1.csv', encoding='unicode_escape',
                          names=['index', 'hook', 'link', 'base'])
        for i in csv.itertuples():
            G.add_edge(getattr(i, 'link'), getattr(i, 'base'))
    except Exception:
        print(traceback.format_exc())

    pr = nx.pagerank(G, alpha=0.85)

    df = pd.DataFrame(pr.items(), columns=['node', 'value'])

    from sqlalchemy import engine
    engine1 = engine.create_engine(my_config.DB_LOGIN)
    df.to_sql('pagerank', engine1, 'everytinku', if_exists='replace', index=False)


class VSM(BSBI.BSBIIndex):
    """
    Attributes
    ----------
    dictionary: set
        termID, 用来存放最后所有的词（包含查询的所有词和简单检索返回文档所包含的所有词），用于构建向量的基础
    """

    def __init__(self, data_dir, output_dir, index_name="BSBI",
                 postings_encoding=None):
        super().__init__(data_dir, output_dir, index_name, postings_encoding)
        # if len(self.term_id_map) == 0 or len(self.doc_id_map) == 0:
        #     self.load()

        self.dictionary = set()  # term_id

    def index(self, is_indexed=True, stopwords_path=None):
        super(VSM, self).index(is_indexed, stopwords_path)

    def get_common_terms(self, query_list, doc_name_list, stopwords_path=None):
        """

        :param query_list: list(str)
            仅需要为切分后的查询词，不需要转换成id，对外接口友好
            函数内部将其转换为id
        :param doc_name_list: list(str)
            仅需要为切分后的查询文本名，不需要转换成id，对外接口友好
            函数内部将其转换为id
        :param stopwords_path:
        :return: 无返回值，结果维护在self.dictionary: set()
        """
        stop_words = []
        if stopwords_path:
            stop_words = [sw.replace('\n', '') for sw in open(stopwords_path, encoding='utf-8').readlines()]
        # 维护一个共有词的词典
        for doc_name in doc_name_list:
            with open(self.data_dir + '/' + doc_name, 'r', encoding='utf-8') as fr:
                words_list = jieba.cut_for_search(fr.read().strip())
                if stop_words:
                    [self.dictionary.add(self.term_id_map.__getitem__(w)) for w in words_list if
                     w != ' ' and w not in stop_words]
                else:
                    [self.dictionary.add(self.term_id_map.__getitem__(w)) for w in words_list if w != ' ']
        for q in query_list:
            self.dictionary.add(self.term_id_map.__getitem__(q))

    def get_doc_pagerank(self, doc_name_list):
        """
        Attention
        ---------
        返回值是经过排序的，因为在做向量乘积的时候就不需要根据索引循环计算了

        :steps:
            1. 通过文档名(12345.txt)获取相应外部id（也就是数据库表used_url中的行id号,12345）
            2. 通过笛卡尔积链接两张表
            3. 返回向量:
         doc_name_id   pagerank
             1          0.2
             ...        ...

        :param doc_name_list:
        :return: Dataframe: sorted
        id(doc_name_id) value
             1          0.2
             ...        ...
        """
        connection = pymysql.connect(host='localhost',
                                     port=3306,
                                     user='root',
                                     password='Qazwsxedcrfv0957',
                                     db='everytinku',
                                     charset='utf8',
                                     cursorclass=pymysql.cursors.DictCursor,
                                     )
        connection.autocommit(True)

        doc_name_id_list = [int(name[:-4]) for name in doc_name_list]
        with connection.cursor() as cursor:
            cursor.execute("select used_url.id,value from used_url left join pagerank on used_url.used=pagerank.node;")
            pd.set_option('precision', 8)
            rank_array = pd.DataFrame(cursor.fetchall()).set_index(['id']).sort_index(inplace=False)
            return rank_array.loc[doc_name_id_list]

    def retrieve(self, query, stopwords_path=None, Personalized_query=False, special_info=None):
        """

        :param query: str
        :param stopwords_path:
        :param Personalized_query:
        :param special_info: str
        :return:
        """
        query_vector = []
        res = pd.DataFrame(columns=['value'])

        # 搜索文本的tf
        query_tf = defaultdict(int)
        query_list = []
        stop_words = []
        if stopwords_path:
            stop_words = [sw.replace('\n', '') for sw in open(stopwords_path, encoding='utf-8').readlines()]

        # TODO 个性化查询，可以用数据库存储用户的查询记录从而给出更加精确的个性化查询
        if Personalized_query and special_info:
            query +=  special_info

        for i in jieba.cut_for_search(query):
            if i not in stop_words:
                # 将文本转为id进行索引
                query_tf[i] += 1
                query_list.append(i)

        doc_name_list = super(VSM, self).retrieve(query_list)
        doc_id_list = [self.doc_id_map.__getitem__(i) for i in doc_name_list]
        doc_id_list.sort()
        pagerank = self.get_doc_pagerank(doc_name_list).fillna(value=0.000001)

        # 每个文档维护一个向量，{docid : np.array([])}
        # tfidf

        for term_name in query_list:
            query_vector.append(query_tf[term_name] *
                                self.term_idf[str(self.term_id_map.__getitem__(term_name))])
        for docID in doc_id_list:
            doc_vector_list = []
            for term_name in query_list:
                doc_vector_list.append(self.docid_terms_tfs[str(docID)]
                                       [str(self.term_id_map.__getitem__(term_name))] *
                                       self.term_idf[str(self.term_id_map.__getitem__(term_name))])
            res = res.append((np.dot(np.array(query_vector), np.transpose(np.array(doc_vector_list))) /
                              np.dot(np.array(doc_vector_list), np.transpose(np.array(doc_vector_list)))) *
                             pagerank.loc[int(self.doc_id_map.__getitem__(docID)[:-4])])
        res.sort_values(by='value', inplace=True, ascending=True)
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

            url_list = []
            for id in res.index.values[:10]:
                cursor.execute("select used_url.used from used_url where used_url.id=(%s)", int(id))
                url_list += list(map(lambda d: d.get('used'), cursor.fetchall()))

        return set(url_list)

    def file_retrieve(self, query, stopwords_path=None):
        """
        TODO 文档查询的完善
        :param query:
        :param stopwords_path:
        :return:
        """
        # 搜索文本的tf
        query_tf = defaultdict(int)
        query_list = []
        stop_words = []
        if stopwords_path:
            stop_words = [sw.replace('\n', '') for sw in open(stopwords_path, encoding='utf-8').readlines()]

        for i in jieba.cut_for_search(query):
            if i not in stop_words:
                # 将文本转为id进行索引
                query_tf[i] += 1
                query_list.append(i)

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

            url_list = []
            for qw in query_list:
                cursor.execute("select hook,`linked url` from docs where docs.hook like (%s)", '%' + qw + '%')
                pd.set_option('max_colwidth', 200)
                res = pd.DataFrame(cursor.fetchall(), dtype=str)
                # url_list += list((list(map(lambda d: d.get('hook'), temp)),list(map(lambda d:d.get('linked url'),temp))))
                cursor.execute("select hook,`linked url` from pdfs where pdfs.hook like (%s)", '%' + qw + '%')
                res.append(cursor.fetchall())
                # url_list += list((list(map(lambda d: d.get('hook'), temp)),list(map(lambda d:d.get('linked url'),temp))))
        connection.close()
        return res

    def reg_retrieve(self, query, stopwords_path=None):
        """
        通配查询，通过轮盘，构建几个不同的词向来进行查询
        :param query:
        :param stopwords_path:
        :param Personalized_query:
        :param special_info:
        :return:
        """
        query_vector = []
        res = pd.DataFrame(columns=['value'])

        # 搜索文本的tf
        query_tf = defaultdict(int)
        query_list = []
        stop_words = []
        if stopwords_path:
            stop_words = [sw.replace('\n', '') for sw in open(stopwords_path, encoding='utf-8').readlines()]

        for w in jieba.cut_for_search(query):
            if w not in stop_words:
                for i in range(len(w)):
                    # 将文本转为id进行索引
                    query_tf[w[i:] + w[:i]] += 1
                    query_list.append(w[i:] + w[:i])

        doc_name_list = super(VSM, self).retrieve(query_list)
        doc_id_list = [self.doc_id_map.__getitem__(i) for i in doc_name_list]
        doc_id_list.sort()
        pagerank = self.get_doc_pagerank(doc_name_list).fillna(value=0.000001)

        # 每个文档维护一个向量，{docid : np.array([])}
        # tfidf

        for term_name in query_list:
            query_vector.append(query_tf[term_name] *
                                self.term_idf[str(self.term_id_map.__getitem__(term_name))])
        for docID in doc_id_list:
            doc_vector_list = []
            for term_name in query_list:
                doc_vector_list.append(self.docid_terms_tfs[str(docID)]
                                       [str(self.term_id_map.__getitem__(term_name))] *
                                       self.term_idf[str(self.term_id_map.__getitem__(term_name))])
            res = res.append((np.dot(np.array(query_vector), np.transpose(np.array(doc_vector_list))) /
                              np.dot(np.array(doc_vector_list), np.transpose(np.array(doc_vector_list)))) *
                             pagerank.loc[int(self.doc_id_map.__getitem__(docID)[:-4])])
        res.sort_values(by='value', inplace=True, ascending=True)
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

            url_list = []
            for id in res.index.values[:10]:
                cursor.execute("select used_url.used from used_url where used_url.id=(%s)", int(id))
                url_list += list(map(lambda d: d.get('used'), cursor.fetchall()))

        return set(url_list)


if __name__ == '__main__':
    username = input("请输入用户名")
    college = input('请输入学院')

    formatter = logging.Formatter("%(asctime)s|%(name)-12s|%(message)s", "%F %T")
    logger = logging.getLogger(username)
    logger.setLevel(logging.INFO)

    log_file_handler1 = logging.FileHandler(filename='logs/' + username + '_retrieve.txt', encoding='utf-8')
    log_file_handler1.setFormatter(formatter)
    log_file_handler1.setLevel(logging.INFO)

    logger.addHandler(log_file_handler1)

    # cul_pagerank()
    vsm = VSM(data_dir='pages/12_13_16_42', output_dir='index/12_13_16_42', index_name='index', postings_encoding=None)
    vsm.index(is_indexed=True, stopwords_path='stopwords.txt')
    # print(vsm.retrieve('叶嘉莹', stopwords_path='stopwords.txt'))
    # print(vsm.retrieve('物联网', stopwords_path='stopwords.txt', Personalized_query=True, special_info='计算机'))

    while True:
        try:
            query_type = input("1、普通查询 2、文档查询 3、通配查询 4、个性化查询")
            query_type = int(query_type)
            if query_type == 1:
                query = input("请输入查询：")
                logger.info('query type' + str(query_type) + '| query word ' + query)
                print(vsm.retrieve(query, stopwords_path='stopwords.txt'))
            elif query_type == 2:
                query = input("请输入查询：")
                logger.info('query type' + str(query_type) + '| query word ' + query)
                print(vsm.file_retrieve(query, stopwords_path='stopwords.txt'))
            elif query_type == 3:
                print('待实现')
            elif query_type == 4:
                query = input("请输入查询：")
                logger.info('query type' + str(query_type) + '| query word ' + query)
                print(vsm.retrieve(query, stopwords_path='stopwords.txt', Personalized_query=True, special_info=college))
        except Exception:
            logger.error(traceback.format_exc())
