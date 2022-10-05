


# %% md

# 作业一：布尔检索


# %%

import re
import os
import nltk
import numpy as np
import pandas as pd
from string import punctuation
from nltk.corpus import stopwords
from collections import defaultdict

sw = stopwords.words('english')


# %% md


# %%

def get_words(text):
    """
    构造dict，key为词，value为list，存放字的位置
    :param text:
    :return:defaultdict(<class 'list'>, {'a': [0, 2], 'aa': [1, 3], 'b': [4], 'c': [5], 'd': [6], 'e': [7], 'ff': [8], 'g': [9]})
    """
    text = re.sub(r"[{}]+".format(punctuation), " ", text)  # 将标点符号转化为空格
    # print(text)
    text = text.lower()  # 全部字符转为小写
    words = nltk.word_tokenize(text)  # 分词
    # print(words)
    i = 0
    result = defaultdict(list)
    # words = list(set(words).difference(set(sw)))  # 去停用词
    for word in words:
        result[word].append(i)
        i += 1
    return result


# %%

def get_files(dir, file_type='.txt'):
    '''
    获取文件名列表
    :param dir:
    :param file_type:
    :return:
    '''
    file_list = []
    for home, dirs, files in os.walk(dir):
        for filename in files:
            if file_type in filename:
                file_list.append(os.path.join(home, filename))
    return file_list


# 构造每种类型词的正则表达式，()代表分组，?P<NAME>为组命名
token_or = r'(?P<OR>\|\|)'
token_not = r'(?P<NOT>\!)'
token_word = r'(?P<WORD>[a-zA-Z]+)'
token_and = r'(?P<AND>&&)'
token_lp = r'(?P<LP>\()'
token_rp = r'(?P<RP>\))'
token_quotation = r'(?P<QT>\")'

lexer = re.compile('|'.join([token_or, token_not, token_word,
                             token_and, token_lp, token_rp, token_quotation]))  # 编译正则表达式


# %%

# 用编译好的正则表达式进行词法分析
def get_tokens(query):
    """
    [('abc', 'WORD'), ('(', 'LP'), ('ab', 'WORD'), (')', 'RP'), ('hhh', 'WORD'), ('aa', 'WORD')]
    :param query:
    :return:
    """
    tokens = []  # tokens中的元素类型为(token, token类型)
    for token in re.finditer(lexer, query):
        tokens.append([token.group(), token.lastgroup])
    return tokens


# %%


class BoolRetrieval:
    """
    布尔检索类
    index为字典类型，其键为单词，值为文件ID列表，如{"word": [1, 2, 9], ...}
    self.query_tokens为需要查询的词序列，list
    self.files 文件名列表
    """

    def __init__(self, index_path=''):
        '''

        :param index_path:
        '''
        if index_path == '':
            self.index = defaultdict(list)
        # 已有构建好的索引文件
        else:
            data = np.load(index_path, allow_pickle=True)
            self.files = data['files'][()]  # ()用来填充，否则报错。可以获取文件中所有该key下的内容，此处key为files
            self.index = data['index'][()]
            # print(self.index)
        self.query_tokens = []

    def build_index(self, text_dir):
        """
        {key:{doc_num:[word_place]}}
        :example: {'the': {1: [0, 17, 20, 36, 50, 56, 73, 76], 2: [47, 71, 76, 83], 3: [4, 39, 56]}}
        self.index就像是一个字典的列表，索引键为词，索引值为所在文档的序号
        在索引的构建过程中，不仅需要记录单词出现的文档，还要记录单词在文档中出现的位置，因
        此索引结构要进行相应的改变 (图 1)。另外在分词的时候停用词和标点都需要保留以确保短语的完
        整性。
        :param text_dir:
        :return:
        """
        self.files = get_files(text_dir)  # 获取所有文件名
        self.index = {}
        for num in range(0, len(self.files)):
            f = open(self.files[num])
            text = f.read()
            # words格式为：defaultdict(<class 'list'>, {'a': [0, 2], 'aa': [1, 3], 'b': [4], 'c': [5], 'd': [6],
            # 'e': [7], 'ff': [8], 'g': [9]})
            words = get_words(text)  # 分词

            # 构建倒排索引
            for word in words.keys():
                # self.index[word]={num:words.get(word)}
                temp = {num: words.get(word)}
                # print(temp)
                if self.index.get(word) == None:
                    self.index[word] = temp
                else:
                    self.index[word][num] = words.get(word)
        # print(self.files, self.index)
        np.savez('index.npz', files=self.files, index=self.index)

    def search(self, query):
        """
        bool retrieval功能的统一入口
        :param query:
        :return:
        """
        self.query_tokens = get_tokens(query)  # 获取查询的tokens
        result = []
        # 将查询得到的文件ID转换成文件名
        for num in self.evaluate(0, len(self.query_tokens) - 1):
            result.append(self.files[num])
        return result

    # 递归解析布尔表达式，p、q为子表达式左右边界的下标
    def evaluate(self, p, q):
        """

        :param p:
        :param q:
        :return: list, indexes of files that contains the string queried
        """
        # 解析错误
        if p > q:
            return []
        # 单个token，一定为查询词
        elif p == q:
            # print(list(self.index[self.query_tokens[p][0]].keys()))
            return list(self.index[self.query_tokens[p][0]].keys())
        # 去掉外层括号
        elif self.check_parentheses(p, q):
            return self.evaluate(p + 1, q - 1)
        elif self.check_quotation(p, q):  # 被 双 引 号 包 围 的 短 语 ， 注 意 双 引 号 中 间 不 应 有 其 他 双 引 号 或 逻 辑 运 算 符

            return self.phrase_search(p + 1, q - 1)
        else:
            # 非单个token
            op, and_list = self.find_operator(p, q)
            # 只有一个and运算符
            if len(and_list) == 1 or not and_list:
                if op == -1:
                    return []
                # files1为运算符左边得到的结果，files2为右边
                if self.query_tokens[op][1] == 'NOT':
                    files1 = []
                else:
                    files1 = self.evaluate(p, op - 1)
                files2 = self.evaluate(op + 1, q)
                return self.merge(files1, files2, self.query_tokens[op][1])
            else:
                if op == -1:
                    return []
                # files1为运算符左边得到的结果，files2为右边
                if self.query_tokens[op][1] == 'NOT':
                    files1 = []
                elif self.query_tokens[op][1] == 'AND':  # 最小优先级为and，且有多个and

                    # 将p和q加入列表，方便统一格式
                    and_list.insert(0, p - 1)
                    and_list.insert(len(and_list), q + 1)
                    num_and = len(and_list)
                    files = []
                    # files.append(len(self.evaluate(p,and_list[0]-1)))
                    i = 0
                    while i < num_and - 1:
                        files.append(len(self.evaluate(and_list[i] + 1, and_list[i + 1] - 1)))
                        i += 1
                    # files.append(len(self.evaluate(and_list[-1]+1,q)))
                    print('出现多个and的情况，进行优化')
                    print('各个段出现文章的个数分别为：', files)
                    temp = list(pd.Series(files).sort_values()[:2].index)
                    print('即将进行运算的是以下两个段：', temp)
                    #需要将query tokens进行调序，将文件数较少的两个放到前面来
                    tuples=[]
                    for t in range(len(files)):
                        tuples.append(self.query_tokens[(and_list[t]+1):(and_list[t+1])])# 将token序列根据and切分开来，后半部分不用减1，因为是：
                    self.query_tokens=tuples[temp[0]]+[('&&','AND')]+tuples[temp[1]]
                    tuples.pop(temp[0])
                    tuples.pop(temp[1]-1)# 必须保证前面pop的元素在此元素前
                    for tpl in tuples:
                        self.query_tokens=self.query_tokens+[('&&','AND')]+tpl
                    #重新寻找第一个and
                    op, and_list = self.find_operator(p, q)
                    files1 = self.evaluate(p,op-1)
                    files2 = self.evaluate(op+1,q)

                    return self.merge(files1, files2, self.query_tokens[op][1])



                else:
                    files1 = self.evaluate(p, op - 1)
                files2 = self.evaluate(op + 1, q)
                return self.merge(files1, files2, self.query_tokens[op][1])

    # 判断表达式是否为 (expr)
    # 判断表达式是否为 (expr)
    def check_parentheses(self, p, q):
        """
        判断表达式是否为 (expr)
        整个表达式的左右括号必须匹配才为合法的表达式
        返回True或False

        #注意(based!day)&&(based&&day)
        """
        LP = 0
        if (self.query_tokens[p][0] == '(') & (self.query_tokens[q][0] == ')'):
            i = p + 1
            while (i < q):
                if self.query_tokens[i][0] == ')':
                    LP -= 1
                    if LP < 0: return False
                elif self.query_tokens[i][0] == '(':
                    LP += 1
                i += 1
            return True
        return False

    def check_quotation(self, p, q):
        """
        检查英文引号配对
        # 被 双 引 号 包 围 的 短 语 ， 注 意 双 引 号 中 间 不 应 有 其 他 双 引 号 或 逻 辑 运 算 符
        :param p:
        :param q:
        :return: true or false
        """
        if self.query_tokens[p][1] == 'QT' and self.query_tokens[q][1] == 'QT':
            for i in range(p + 1, q):
                if self.query_tokens[i][1] != 'WORD': return False
            return True
        return False

    def phrase_search(self, p, q):
        """
        短语查询
        :param p:
        :param q:
        :return: list,短语查询结果
        """
        # {'a': {1:[0, 2],2:[6,6,6,6]}}

        result = list(self.index[self.query_tokens[p][0]])
        # 构建一个list，包含短语中每个词（此处无所谓这个词是啥了，只需要考虑各个词之间的shunxuguanxi)
        # 先确定所有共同的文档index
        """#不确定是否上界为q"""
        for i in range(p + 1, q + 1):
            result = set(result).intersection(set(self.index[self.query_tokens[i][0]]))
            if not result:
                return []

            i += 1

        # 再确定在共同的文档中是否是短语，因此每次只筛选一个文档，若没有交集则将该文档从result删除
        for file_num in result:
            # temp用来存word的位置，不需要考虑文档编号，因为已经在上面的循环刨去文档数了，现在所有word都是能出现在同一个文档里的
            temp = self.index[self.query_tokens[p][0]][file_num]
            for i in range(p + 1, q + 1):
                # 求列表交集
                temp = set([t + 1 for t in temp]).intersection(set(self.index[self.query_tokens[i][0]][file_num]))
                if not temp:
                    result.remove(file_num)
        return list(result)
        # word_dict_list = []  # [{1:[0, 2],2:[6,6,6,6]},{1:[0, 2],2:[6,6,6,6]}...]
        # for i in range(p + 1, q):
        #     word_dict_list.append(self.index[self.query_tokens[i][0]])
        # for file_index in word_dict_list[0].keys():
        #     for word_dict in word_dict_list:

    # 寻找表达式的dominant的运算符（优先级最低）
    def find_operator(self, p, q):
        """
        寻找表达式的dominant的运算符（优先级最低）
        其必定在括号外面（不存在整个子表达式被括号包围，前面以已处理）
        返回dominant运算符的下标位置
        """
        # 找出第一个非括号运算符，并将其位置记录在列表中
        LP = 0
        lowest_op = 'NOT'
        first_and = -1
        first_not = -1
        and_list = []
        while p < q:
            temp_token = self.query_tokens[p][1]
            if temp_token != 'WORD' and temp_token != 'QT':
                if temp_token == 'LP':
                    LP += 1
                elif temp_token == 'RP':
                    LP -= 1
                elif LP == 0:
                    if temp_token == 'OR':
                        return p, []
                    elif temp_token == 'AND':
                        lowest_op = 'AND'
                        if first_and == -1: first_and = p
                        and_list.append(p)
                    elif temp_token == 'NOT':
                        if first_not == -1: first_not = p

            p += 1
        if lowest_op == 'AND':
            return first_and, and_list
        else:
            return first_not, []

    def merge(self, files1, files2, op_type):
        """
        根据运算符对进行相应的操作
        在Python中可以通过集合的操作来实现
        但为了练习算法，请遍历files1, files2合并
        """
        result = []

        if op_type == 'AND':
            # result = list(set(files1) & set(files2))
            result = [item for item in files1 if item in files2]
        elif op_type == "OR":
            # result = list(set(files1) | set(files2))
            temp_file_list = files1 + files2
            if temp_file_list:
                result = pd.array(temp_file_list).unique().to_numpy().tolist()
        elif op_type == "NOT":
            # result = list(set(range(0, len(self.files))) - set(files2))
            # all_files=np.array()

            result = [item for item in range(0, len(self.files)) if item not in files2]

        return result


if __name__ == '__main__':
    # br = BoolRetrieval()
    # br.build_index('text')
    br = BoolRetrieval('index.npz')
    print(br.search('\"advantages clean\"&&\"easy to implement\"&&the'))
    print(br.search('\"advantages clean\"&&\"easy to implement\"'))
    print(br.search('\"advantages clean\"&&the&&\"easy to implement\"'))
    print(br.search('\"easy to implement\"&&the'))
    print(br.search('the'))
