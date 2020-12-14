"""
import gensim : vsm
import networkx : pagerank
steps:
    1. 构建词典
    2.
"""
import gensim
import os
import networkx as nx
import traceback
import logging
import pandas as pd
import numpy as np
from collections import defaultdict
import BSBI
import jieba
from configs import my_config
import pymysql


def cul_pageranke():
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


        self.dictionary = set()# term_id

    def index(self, is_indexed=True, stopwords_path=None):
        super(VSM, self).index(is_indexed, stopwords_path)

    def get_common_terms(self,query_list,doc_name_list,stopwords_path=None):
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
        stop_words=[]
        if stopwords_path:
            stop_words = [sw.replace('\n', '') for sw in open(stopwords_path, encoding='utf-8').readlines()]
        # 维护一个共有词的词典
        for doc_name in doc_name_list:
            # 这里就不用去停用词了
            with open(self.data_dir + '/' + doc_name, 'r', encoding='utf-8') as fr:
                words_list = jieba.cut_for_search(fr.read().strip())
                if stop_words:
                    [self.dictionary.add(self.term_id_map.__getitem__(w)) for w in words_list if w != ' ' and w not in stop_words]
                else:
                    [self.dictionary.add(self.term_id_map.__getitem__(w)) for w in words_list if w != ' ']
        for q in query_list:
            self.dictionary.add(q)

    def get_doc_pagerank(self,doc_name_list):
        """
        TODO: 由于接口不够完善，只能通过文档名获取相应外部id

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

        doc_name_id_list=[int(name[:-4]) for name in doc_name_list]
        with connection.cursor() as cursor:
            cursor.execute("select used_url.id,value from used_url left join pagerank on used_url.used=pagerank.node;")
            rank_array=pd.DataFrame(cursor.fetchall()).set_index(['id']).sort_index(inplace=False)
            return rank_array.loc[doc_name_id_list]


    def retrieve(self, query,stopwords_path=None):
        query_vector=[]
        res=[]

        # 搜索文本的tf
        query_tf=defaultdict(int)
        query_list =[]
        for i in jieba.cut_for_search(query):
            if i !=' ':
                query_tf[i]+=1
                query_list.append(i)

        # [self.dictionary.add(self.term_id_map.__getitem__(i)) for i in query_list]
        doc_name_list = super(VSM, self).retrieve(query_list)
        doc_id_list=[self.doc_id_map.__getitem__(i[:-4]) for i in doc_name_list]
        doc_id_list.sort()
        self.get_common_terms(query_list,doc_name_list,stopwords_path)
        pagerank=self.get_doc_pagerank(doc_name_list)
        # 每个文档维护一个向量，{docid : np.array([])}
        # tfidf
        all_terms=[self.dictionary]
        all_terms.sort()
        for termID in all_terms:
            query_vector.append(query_tf[termID]*self.term_idf[termID])
        for docID in doc_id_list:
            doc_vector_list = []
            for termID in all_terms:
                doc_vector_list.append(self.docid_terms_tfs[docID][termID]*self.term_idf[termID])
            res.append(doc_vector_list*pagerank[int(self.doc_id_map.__getitem__(docID)[:-4])])


        return res

def bit_product_sum(x, y):
    return sum([item[0] * item[1] for item in zip(x, y)])


def cosine_similarity(x, y, norm=False):
    """
     计算两个向量x和y的余弦相似度
    :param x:
    :param y:
    :param norm:
    :return:
    """
    assert len(x) == len(y), "len(x) != len(y)"
    zero_list = [0] * len(x)
    if x == zero_list or y == zero_list:
        return float(1) if x == y else float(0)

    # method 1
    res = np.array([[x[i] * y[i], x[i] * x[i], y[i] * y[i]] for i in range(len(x))])
    cos = sum(res[:, 0]) / (np.sqrt(sum(res[:, 1])) * np.sqrt(sum(res[:, 2])))

    # method 2
    # cos = bit_product_sum(x, y) / (np.sqrt(bit_product_sum(x, x)) * np.sqrt(bit_product_sum(y, y)))

    # method 3
    # dot_product, square_sum_x, square_sum_y = 0, 0, 0
    # for i in range(len(x)):
    #     dot_product += x[i] * y[i]
    #     square_sum_x += x[i] * x[i]
    #     square_sum_y += y[i] * y[i]
    # cos = dot_product / (np.sqrt(square_sum_x) * np.sqrt(square_sum_y))

    return 0.5 * cos + 0.5 if norm else cos  # 归一化到[0, 1]区间内


# cul_pageranke()
vsm = VSM(data_dir='pages/test', output_dir='index/test', index_name='test', postings_encoding=None)
vsm.index(is_indexed=True, stopwords_path='stopwords.txt')
print(vsm.retrieve('南开大学 曹雪涛 校学术委员会考核评审百名青'))

vsm.get_doc_pagerank(['52837.txt', '52838.txt', '52844.txt'])