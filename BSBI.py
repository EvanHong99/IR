# You can add additional imports here
import heapq
import re
import sys
import pickle as pkl
import array
import os
import timeit
import contextlib
from string import punctuation
import time
import jieba
import json

# import nltk
from collections import defaultdict


class IdMap:
    """
        Helper class to store a mapping from strings to ids.

        :param self.id_to_str: list, 存储相应单词，其id为数组中的下标

    """

    def __init__(self):

        self.str_to_id = {}
        "self.id_to_str: list, 存储相应单词，其id为数组中的下标"
        self.id_to_str = []

    def __len__(self):
        """Return number of terms stored in the IdMap"""
        return len(self.id_to_str)

    def _get_str(self, i):
        """Returns the string corresponding to a given id (`i`)."""
        ### Begin your code
        if i >= self.__len__():
            raise IndexError
        return self.id_to_str[i]
        ### End your code

    def _get_id(self, s):
        """Returns the id corresponding to a string (`s`).
        If `s` is not in the IdMap yet, then assigns a new id and returns the new id.
        """
        ### Begin your code
        res = self.str_to_id.get(s)
        if res or res == 0:
            return res
        return self._assign_id(s)
        ### End your code

    def _assign_id(self, s):
        """
        给一个不在id map中的单词分配id
        :param s: str
        :return: 分配的id
        """
        len = self.__len__()
        self.id_to_str.append(s)
        self.str_to_id[s] = len
        return len

    def __getitem__(self, key):
        """If `key` is a integer, use _get_str;
           If `key` is a string, use _get_id;"""
        if type(key) is int:
            return self._get_str(key)
        elif type(key) is str:
            return self._get_id(key)
        else:
            raise TypeError

    def save(self, filepath):
        with open(os.path.join(filepath, 'docs.dict'), 'wb') as f:
            pkl.dump(self, f)

    def load(self, filepath):
        """Loads doc_id_map and term_id_map from output directory"""
        with open(os.path.join(filepath, 'docs.dict'), 'rb') as f:
            self = pkl.load(f)


class UncompressedPostings:

    @staticmethod
    def encode(postings_list):
        """Encodes postings_list into a stream of bytes


        Parameters
        ----------
        postings_list: List[int]
            List of docIDs (postings)

        Returns
        -------
        bytes
            bytearray representing integers in the postings_list
        """
        return array.array('L', postings_list).tobytes()

    @staticmethod
    def decode(encoded_postings_list):
        """Decodes postings_list from a stream of bytes

        Parameters
        ----------
        encoded_postings_list: bytes
            bytearray representing encoded postings list as output by encode
            function

        Returns
        -------
        List[int]
            Decoded list of docIDs from encoded_postings_list
        """

        decoded_postings_list = array.array('L')
        decoded_postings_list.frombytes(encoded_postings_list)
        return decoded_postings_list.tolist()


class CompressedPostings:
    # If you need any extra helper methods you can add them here
    ### Begin your code

    ### End your code

    @staticmethod
    def encode(postings_list):
        """Encodes `postings_list` using gap encoding with variable byte
        encoding for each gap

        Parameters
        ----------
        postings_list: List[int]
            The postings list to be encoded

        Returns
        -------
        bytes:
            Bytes reprsentation of the compressed postings list
            (as produced by `array.tobytes` function)
        """
        ### Begin your code
        miner = [0] + postings_list[0:-1]
        postings_list = list(map(lambda x: x[0] - x[1], zip(postings_list, miner)))
        return array.array('L', postings_list).tobytes()

        ### End your code

    @staticmethod
    def decode(encoded_postings_list):
        """Decodes a byte representation of compressed postings list

        Parameters
        ----------
        encoded_postings_list: bytes
            Bytes representation as produced by `CompressedPostings.encode`

        Returns
        -------
        List[int]
            Decoded postings list (each posting is a docIds)
        """
        ### Begin your code
        base = 0
        decoded_postings_list = array.array('L')
        decoded_postings_list.frombytes(encoded_postings_list)
        postings_list = decoded_postings_list.tolist()
        for i in range(len(postings_list)):
            base += postings_list[i]
            postings_list[i] = base
        return postings_list

        ### End your code


class InvertedIndex:
    """A class that implements efficient reads and writes of an inverted index
    to disk

    Attributes
    ----------
    postings_dict: Dictionary mapping: termID->(start_position_in_index_file,
                                                number_of_postings_in_list,
                                               length_in_bytes_of_postings_list)
        termID是输入
        This is a dictionary that maps from termIDs to a 3-tuple of metadata
        that is helpful in reading and writing the postings in the index file
        to/from disk. This mapping is supposed to be kept in memory.
        start_position_in_index_file is the position (in bytes) of the postings
        list in the index file
        number_of_postings_in_list is the number of postings (docIDs) in the
        postings list
        length_in_bytes_of_postings_list is the length of the byte
        encoding of the postings list

    terms: List[int]
        A list of termIDs to remember the order in which terms and their
        postings lists were added to index.

        After Python 3.7 we technically no longer need it because a Python dict
        is an OrderedDict, but since it is a relatively new feature, we still
        maintain backward compatibility with a list to keep track of order of
        insertion.



    """

    def __init__(self, index_name, postings_encoding=None, directory=''):
        """
        Parameters
        ----------
        index_name :(str) Name used to store files related to the index
        postings_encoding: A class implementing static methods for encoding and
            decoding lists of integers. Default is None, which gets replaced
            with UncompressedPostings
        directory :(str) Directory where the index files will be stored
        """

        self.index_file_path = os.path.join(directory, index_name + '.index')
        self.metadata_file_path = os.path.join(directory, index_name + '.dict')

        if postings_encoding is None:
            self.postings_encoding = UncompressedPostings
        else:
            self.postings_encoding = postings_encoding
        self.directory = directory

        self.postings_dict = {}  # 用于存储倒排索引term-postlist
        self.terms = []  # Need to keep track of the order in which the
        # terms were inserted. Would be unnecessary
        # from Python 3.7 onwards

    def __enter__(self):
        """Opens the index_file and loads metadata upon entering the context"""
        # Open the index file
        self.index_file = open(self.index_file_path, 'rb+')

        # Load the postings dict and terms from the metadata file
        with open(self.metadata_file_path, 'rb') as f:
            self.postings_dict, self.terms = pkl.load(f)
            self.term_iter = self.terms.__iter__()
            # print(self.postings_dict,self.terms)

        return self

    def __exit__(self, exception_type, exception_value, traceback):
        """Closes the index_file and saves metadata upon exiting the context"""
        # Close the index file
        self.index_file.close()

        # Write the postings dict and terms to the metadata file
        with open(self.metadata_file_path, 'wb') as f:
            pkl.dump([self.postings_dict, self.terms], f)


# Do not make any changes here, they will be overwritten while grading
class BSBIIndex:
    """
    Attributes
    ----------
    term_id_map(IdMap): For mapping terms to termIDs
    doc_id_map(IdMap): For mapping relative paths of documents
        eg: 12345.txt - docID
    data_dir(str): Path to data
    output_dir(str): Path to output index files
    index_name(str): Name assigned to index
    postings_encoding: Encoding used for storing the postings.
        The default (None) implies UncompressedPostings

    term_tf_docid: dict { key=termid, value=(tf, [doc id list])}

    term_idf: dict {termID: tfidf}
    """

    def __init__(self, data_dir, output_dir, index_name="BSBI",
                 postings_encoding=None):
        self.term_id_map = IdMap()
        self.doc_id_map = IdMap()
        self.data_dir = data_dir
        self.output_dir = output_dir
        self.index_name = index_name
        self.postings_encoding = postings_encoding

        # Stores names of intermediate indices
        self.intermediate_indices = []

        # { key=termid, value=(tf, [doc id list])}
        self.term_tf_docid = defaultdict(lambda: [0, set()])
        # TODO 这里本来是  self.term_idf = {}，不会报错
        self.term_idf = defaultdict(float)
        self.docid_terms_tfs = defaultdict(lambda: defaultdict(int))

    def save(self):
        """
        都是单独的map，而非倒排索引表
        Dumps doc_id_map and term_id_map into output directory"""

        def set_default(obj):
            if isinstance(obj, set):
                return list(obj)
            raise TypeError

        with open(os.path.join(self.output_dir, 'terms.dict'), 'wb') as f:
            pkl.dump(self.term_id_map, f)
        with open(os.path.join(self.output_dir, 'docs.dict'), 'wb') as f:
            pkl.dump(self.doc_id_map, f)
        with open(os.path.join(self.output_dir, 'term_tf_doclist.dict'), 'wb') as f:
            pkl.dump(json.dumps(self.term_tf_docid, default=set_default), f)
        with open(os.path.join(self.output_dir, 'term_idf.dict'), 'wb') as f:
            pkl.dump(json.dumps(self.term_idf), f)
        with open(os.path.join(self.output_dir, 'docid_terms_tfs.dict'), 'wb') as f:
            pkl.dump(json.dumps(self.docid_terms_tfs), f)

    def load(self):
        """Loads doc_id_map and term_id_map from output directory"""

        with open(os.path.join(self.output_dir, 'terms.dict'), 'rb') as f:
            self.term_id_map = pkl.load(f)
        with open(os.path.join(self.output_dir, 'docs.dict'), 'rb') as f:
            self.doc_id_map = pkl.load(f)
        with open(os.path.join(self.output_dir, 'term_tf_doclist.dict'), 'rb') as f:
            self.term_tf_docid = dict(json.loads(pkl.load(f)))
        with open(os.path.join(self.output_dir, 'term_idf.dict'), 'rb') as f:
            self.term_idf = dict(json.loads(pkl.load(f)))
        with open(os.path.join(self.output_dir, 'docid_terms_tfs.dict'), 'rb') as f:
            self.docid_terms_tfs = dict(json.loads(pkl.load(f)))

    def merge(self, indices, merged_index):
        """
        未能实现完全的不利用多余内存的合并方法
        Merges multiple inverted indices into a single index
        利用merged index来进行写入磁盘



        import heapq
        animal_lifespans = [('Giraffe', 28),
                           ('Rhinoceros', 40),
                           ('Indian Elephant', 70),
                           ('Golden Eagle', 80),
                           ('Box turtle', 123)]

        tree_lifespans = [('Gray Birch', 50),
                          ('Black Willow', 70),
                          ('Basswood', 100),
                          ('Bald Cypress', 600)]

        lifespan_lists = [animal_lifespans, tree_lifespans]

        for merged_item in heapq.merge(*lifespan_lists, key=lambda x: x[1]):
            print(merged_item)


        Parameters
        ----------
        indices: List[InvertedIndexIterator]
            A list of InvertedIndexIterator objects, each representing an
            **iterable inverted index for a block**
        merged_index: InvertedIndexWriter
            An instance of InvertedIndexWriter object into which each merged
            postings list is written out one at a time
        """
        # print(len(self.term_id_map))
        # 此时我应该能够在一个循环中存下所有该词的posting list
        stack = defaultdict(list)  # 用来保存next出来，但是不能立即加入到pl中的tuple元组
        for termid in range(len(self.term_id_map)):
            pl = []
            for iii in indices:
                try:
                    while iii:
                        tup = next(iii)
                        if tup[0] == termid:  # 完成一次插入就跳出，否则会直接遍历完iter的所有元素并且无法重新置位为起始
                            # print("tuple",tup[0])
                            pl += tup[1]
                            break
                        stack[tup[0]] += tup[1]  # 返回的tup[0]并非当前正在合并的term，因此将它放到列表中
                except StopIteration:
                    # print('iteration finished')
                    continue
            if stack.get(termid):
                # print(stack)
                pl += stack.pop(termid)
                # stack.__delattr__(termid)
            merged_index.append(termid, sorted(pl))
            # print(termid)

    def index(self, is_indexed=True, stopwords_path=None):
        """Base indexing code

        既然我们知道文档列表已经排过序了，那么我们可以在线性时间内对它们进行合并排序。
        事实上heapq(参考文档) 是Python中一个实现了堆排序算法的标准模板。
        另外它还包含一个实用的函数heapq.merge，可以将多个已排好序的输入（可迭代）合并成一个有序的输出并返回它的迭代器。
        它不仅可以用来合并倒排列表，也可以合并倒排索引词典。

        This function loops through the data directories,
        calls parse_block to parse the documents
        calls invert_write, which inverts each block and writes to a new index
        then saves the id maps and calls merge on the intermediate indices

        Parameters
        ---------
        is_indexed: bool
            是否需要重新构建索引，默认为不需要
        """

        # 从顶层目录开始解析
        # 选择性地打开此开关
        if not is_indexed:
            # for block_dir_relative in sorted(next(os.walk(self.data_dir))[1]):
            #     time_start = time.time()
            #
            #     td_pairs = self.parse_block(self.data_dir + '/' + block_dir_relative + '/')
            #
            #     index_id = 'index_' + block_dir_relative
            #     self.intermediate_indices.append(index_id)
            #     # 将一个目录下的文件写入磁盘
            #     with InvertedIndexWriter(index_id, directory=self.output_dir,
            #                              postings_encoding=
            #                              self.postings_encoding) as index:
            #         self.invert_write(td_pairs, index)  # 应该没问题了
            #         td_pairs = None
            #     time_end = time.time()
            #     print("time=", time_end - time_start)
            time_start = time.time()

            td_pairs = self.parse_block(self.data_dir + '/', stopwords_path=stopwords_path)

            index_id = 'index_'
            self.intermediate_indices.append(index_id)
            # 将一个目录下的文件写入磁盘
            with InvertedIndexWriter(index_id, directory=self.output_dir,
                                     postings_encoding=
                                     self.postings_encoding) as index:
                self.invert_write(td_pairs, index)  # 应该没问题了
                td_pairs = None
            time_end = time.time()
            print("time=", time_end - time_start)
            self.save()
            with InvertedIndexWriter(self.index_name, directory=self.output_dir,
                                     postings_encoding=
                                     self.postings_encoding) as merged_index:
                # 入栈
                with contextlib.ExitStack() as stack:
                    # 根据index id向stack压栈，组成一个列表，每个列表项indices[i]是一个
                    indices = [stack.enter_context(
                        InvertedIndexIterator(index_id,
                                              directory=self.output_dir,
                                              postings_encoding=
                                              self.postings_encoding))
                        for index_id in self.intermediate_indices]
                    # print(indices)
                    time_start = time.time()

                    self.merge(indices, merged_index)
                    time_end = time.time()
                    print("time=", time_end - time_start)

    def parse_block(self, block_dir_relative, stopwords_path=None):
        """
        维护self.term_tf_docid and self.term_tfidf
        仅返回td pairs的list，非倒排索引表，未进行文档去重
        会维护self的几个变量
        Parses a tokenized text file into termID-docID pairs

        Attention
        ---------
        对函数内部接口均为docID和termID，对外为filename（由数据库维护其映射）

        Parameters
        ----------
        block_dir_relative : str
            Relative Path to the directory that contains the files for the block

        Returns
        -------

        List[(Int, Int)]
            Returns all the td_pairs extracted from the block
            # used to be List[Tuple[Int, Int]]

        Should use self.term_id_map and self.doc_id_map to get termIDs and docIDs.
        These persist across calls to parse_block
        """
        global stop_words
        result = []
        if stopwords_path:
            stop_words = [sw.replace('\n', '') for sw in open(stopwords_path, encoding='utf-8').readlines()]
        for filepath, dirs, fs in os.walk(block_dir_relative):
            for filename in fs:
                fullpath = os.path.join(filepath, filename)
                with open(fullpath, 'r', encoding='utf-8') as fr:
                    docID = self.doc_id_map.__getitem__(filename)
                    # docID = self.doc_id_map.__getitem__(filename[:-4].replace("+",'/').replace('-',':',1).replace(',','=').replace('\'','?'))
                    # pattern=re.compile('\S*?')
                    # words = pattern.findall(fr.read())
                    words_list = jieba.cut_for_search(fr.read().strip())
                    if stopwords_path:
                        words = [w for w in words_list if w != ' ' and w not in stop_words]
                    else:
                        words = [w for w in words_list if w != ' ']

                    for word in words:
                        # 返回一个词的id
                        termID = self.term_id_map.__getitem__(word)
                        result.append((termID, docID))
                        self.term_tf_docid[termID][0] += 1
                        self.term_tf_docid[termID][1].add(docID)
                        self.docid_terms_tfs[docID][termID] += 1
        # 计算idf
        for termID, docID in result:
            self.term_idf[termID] = log(len(self.doc_id_map) / len(self.term_tf_docid[termID][1]))

        return result

    def invert_write(self, td_pairs, index):
        """
        写一个块的数据，index是一个块的writer
        构建倒排索引列表，并写入相应index文件
        Inverts td_pairs into postings_lists and writes them to the given index

        Parameters
        ----------
        td_pairs: List[Tuple[Int, Int]]
            一个块中的td pairs
            List of termID-docID pairs
            和parse blocks函数返回值相同
        index: InvertedIndexWriter
            对应块的index文件
            Inverted index on disk corresponding to the block
        """
        res = defaultdict(list)
        # 遍历所有td pairs并将其归类，形成term - doclist的inverted list
        for pair in td_pairs:
            termID = pair[0]
            docID = pair[1]
            res[termID].append(docID)
        terms = sorted(res.keys())
        # 将每个term 以及其postinglist写入磁盘
        for t in terms:
            # print("inverted")
            index.append(t, sorted(list(set(res[t]))))
            # print(list(set(res[t])))
        pass

    def retrieve(self, query):
        """Retrieves the documents corresponding to the conjunctive query

        Parameters
        ----------
        query: str or list
            Space separated list of query tokens

        Result
        ------
        List[str]
            Sorted list of documents which contains each of the query tokens.
            Should be empty if no documents are found.

        Should NOT throw errors for terms not in corpus
        """
        if len(self.term_id_map) == 0 or len(self.doc_id_map) == 0:
            self.load()

        if isinstance(query, str):
            ### Begin your code
            terms = query.split(' ')
            with InvertedIndexMapper(self.index_name, self.postings_encoding, directory=self.output_dir) as iim:
                res = []

                for term in terms:
                    try:
                        termid = self.term_id_map.str_to_id[term]
                        if not res:
                            res = iim._get_postings_list(termid)
                        else:
                            res = sorted_intersect(res, iim._get_postings_list(termid))
                    except Exception:
                        print(term, '没找到')
                        continue
                # 未在sorted intersection函数中保证res的有序，但是在invert write保证了有序
                r = [self.doc_id_map.id_to_str[r] for r in res]
                return r
            ### End your code
        if isinstance(query, list):
            with InvertedIndexMapper(self.index_name, self.postings_encoding, directory=self.output_dir) as iim:
                res = []

                for term in query:
                    try:
                        termid = self.term_id_map.str_to_id[term]
                        if not res:
                            res = iim._get_postings_list(termid)
                        else:
                            res = sorted_intersect(res, iim._get_postings_list(termid))
                    except Exception:
                        print(term, '没找到')
                        continue
                # 未在sorted intersection函数中保证res的有序，但是在invert write保证了有序
                r = [self.doc_id_map.id_to_str[r] for r in res]
                return r


def sorted_intersect(list1, list2):
    """Intersects two (ascending) sorted lists and returns the sorted result

    Parameters
    ----------
    list1: List[Comparable]
    list2: List[Comparable]
        Sorted lists to be intersected

    Returns
    -------
    List[Comparable]
        Sorted intersection
    """
    ### Begin your code
    return list(set(list1).intersection(set(list2)))
    ### End your code


class InvertedIndexWriter(InvertedIndex):
    """

    Attributes
    ----------
    terms: list[int]
        term ids
    postings_dict: Dictionary mapping: termID->(start_position_in_index_file,
                                            number_of_postings_in_list,
                                           length_in_bytes_of_postings_list)
    index_file: context manager
        write metadata index
    append(term, postings_list):
        将postinglist的信息记录到posting dict
        注意：直接写入磁盘！！！
    """

    def __enter__(self):
        self.index_file = open(self.index_file_path, 'wb+')
        return self

    def append(self, term, postings_list):
        """
        需要一次性构建完整的posting list吗？？？？
        Appends the term and encoded postings_list to end of the index file.
        并且将postinglist的信息记录到posting dict
        注意：直接写入磁盘！！！



        This function does three things,
        1. Encodes the postings_list using self.postings_encoding
        2. Stores metadata in the form of self.terms and self.postings_dict
           Note that self.postings_dict maps termID to a 3 tuple of
           (start_position_in_index_file,
           number_of_postings_in_list,
           length_in_bytes_of_postings_list)
        3. Appends the bytestream to the index file on disk

        Hint: You might find it helpful to read the Python I/O docs
        (https://docs.python.org/3/tutorial/inputoutput.html) for
        information about appending to the end of a file.

        Parameters
        ----------
        term:
            term or termID is the unique identifier for the term
            最好是termID吧
        postings_list: List[Int]
            List of docIDs where the term appears


        """
        # print(term,postings_list)
        encoded_postlist = self.postings_encoding.encode(postings_list)
        # if term not in self.terms:
        self.terms.append(term)
        # 不会覆盖，因为term是独立的。
        # self.index_file.tell()用于指明当前indexfile的末端位置
        self.postings_dict[term] = (self.index_file.tell(), len(postings_list), len(encoded_postlist))
        self.index_file.write(encoded_postlist)  # self.index_file可以被写入数据
        # print(self.postings_dict[term])


class InvertedIndexIterator(InvertedIndex):
    """
    用next函数逐个获取term和posting list

    file.read([size])：size 未指定则返回整个文件，如果文件大小 >2 倍内存则有问题，f.read()读到文件尾时返回""(空字串)。

    f.tell()：返回一个整数,表示当前文件指针的位置(就是到文件头的字节数)。

    f.seek(偏移量,[起始位置])：用来移动文件指针。

    偏移量: 单位为字节，可正可负
    起始位置: 0 - 文件头, 默认值; 1 - 当前位置; 2 - 文件尾
    f.close() 关闭文件

    Attributes
    ----------
    indicator: int
        用于指明当前遍历的字节流index file的位置


    """

    def __enter__(self):
        """Adds an initialization_hook to the __enter__ function of super class
        """
        super().__enter__()
        self._initialization_hook()
        return self

    def _initialization_hook(self):
        """
        初始化一个标志位，用于标明当前遍历的字节流index file的位置
        """
        ### Begin your code
        # self.indicator=0
        ### End your code

    def __iter__(self):
        return self

    def __next__(self):
        """
        Returns the next (term, postings_list) pair in the index.

        file.read([size])：size 未指定则返回整个文件，如果文件大小 >2 倍内存则有问题，f.read()读到文件尾时返回""(空字串)。

        f.tell()：返回一个整数,表示当前文件指针的位置(就是到文件头的字节数)。

        f.seek(偏移量,[起始位置])：用来移动文件指针。

        偏移量: 单位为字节，可正可负
        起始位置: 0 - 文件头, 默认值; 1 - 当前位置; 2 - 文件尾

        Note: This function should only read a small amount of data from the
        index file. In particular, you should not try to maintain the full
        index file in memory.

        Returns
        ---------
        term: int
            单词的编号 term id
        posting_list: list[int]
            倒排索引表

        """
        ### Begin your code
        #
        # if self.indicator==len(self.terms):
        #     raise StopIteration
        # term = self.terms[self.indicator]
        # self.indicator+=1
        term = next(self.term_iter)
        pd = self.postings_dict[term]
        self.index_file.seek(pd[0])  # 将文件指针向后移，read时以文件指针位置为基址
        encoded_posting_list = self.index_file.read(pd[2])
        return term, self.postings_encoding.decode(encoded_posting_list)
        ### End your code

    def delete_from_disk(self):
        """Marks the index for deletion upon exit. Useful for temporary indices
        """
        self.delete_upon_exit = True

    def __exit__(self, exception_type, exception_value, traceback):
        """Delete the index file upon exiting the context along with the
        functions of the super class __exit__ function"""
        self.index_file.close()
        if hasattr(self, 'delete_upon_exit') and self.delete_upon_exit:
            os.remove(self.index_file_path)
            os.remove(self.metadata_file_path)
        else:
            with open(self.metadata_file_path, 'wb') as f:
                pkl.dump([self.postings_dict, self.terms], f)


class InvertedIndexMapper(InvertedIndex):
    def __getitem__(self, key):
        return self._get_postings_list(key)

    def _get_postings_list(self, term):
        """:return a postings list (of docIds) for `term`.

        This function should not iterate through the index file.
        I.e., it should only have to read the bytes from the index file
        corresponding to the postings list for the requested term.
        """
        ### Begin your code
        try:
            pd = self.postings_dict[term]
            if pd:
                self.index_file.seek(pd[0])  # 将文件指针向后移，read时以文件指针位置为基址
                encoded_posting_list = self.index_file.read(pd[2])
                return self.postings_encoding.decode(encoded_posting_list)
        except KeyError:
            print(term, 'not found')

        ### End your code


from math import log


class GammaCompressor:
    @staticmethod
    def int_to_bin(int_in):
        """

        :param int_in:
        :return:
            ret: str

        """
        if int_in == 0:
            return '0'
        # 去除第一个位，因此是print(bin(5)) 0b101
        ret = '1' * int(log(int_in, 2)) + '0' + bin(int_in)[3:]
        return ret

    @staticmethod
    def encode(postings_list):
        """Encodes `postings_list`

        Parameters
        ----------
        postings_list: List[int]
            The postings list to be encoded

        Returns
        -------
        bytes:
            bytes reprsentation of the compressed postings list
        """
        ### Begin your code
        encoded_postings_list = ''
        for i in range(0, len(postings_list)):
            # 加一是为了处理0和1的情况
            encoded_postings_list += GammaCompressor.int_to_bin(postings_list[i] + 1)
        return encoded_postings_list.encode()

    @staticmethod
    def decode(encoded_postings):
        """Decodes a byte representation of compressed postings list

        Parameters
        ----------
        encoded_postings: bytes
            Bytes representation as produced by `CompressedPostings.encode`

        Returns
        -------
        List[int]
            Decoded postings list (each posting is a docId)
        """
        res = []
        i = 0
        byteslen = len(encoded_postings)
        encoded_postings = encoded_postings.decode()
        while True:
            length = 0
            while encoded_postings[i] != '0':
                i += 1
                length += 1
            i += 1
            val = int('1' + encoded_postings[i:i + length], 2) - 1
            res.append(val)
            i = i + length
            if i >= byteslen:
                return res

# if __name__ == '__main__':
# bsbi = BSBIIndex(data_dir='toy-data', output_dir='tmp', index_name='test',postings_encoding=CompressedPostings)
# bsbi.index(False)
# print(bsbi.retrieve('very cool'))

# print(bsbi.parse_block('toy-data'))
# iiw = InvertedIndexWriter('test', directory=r"tmp")
#
# bsbi.invert_write(bsbi.parse_block('toy-data'), iiw)
#
#
#
#
# iii = InvertedIndexIterator('test', directory=r"tmp")
# with iii as i:#这些iter、next函数只有在被当做上下文管理器打开时才能访问到
#     try:
#         while i:
#             print(i.__next__())
#             # pass
#     except StopIteration:
#         i.__exit__(StopIteration,StopIteration.value,StopIteration.__traceback__)
#
#
# BSBI_instance = BSBIIndex(data_dir='toy-data', output_dir='toy_output_dir')
# BSBI_instance.index()

# BSBI_instance = BSBIIndex(data_dir='test-pa1-data1', output_dir='test-output-diroutput_dir')
# BSBI_instance.index(False)
# print(BSBI_instance.retrieve('very good'))

# with InvertedIndexMapper(BSBI_instance.index_name, BSBI_instance.postings_encoding,
#                          directory=BSBI_instance.output_dir) as iim:
#     iim._get_postings_list("boolean")

############mian

# BSBI_instance = BSBIIndex(data_dir='pa1-data', output_dir='output_dir')
# BSBI_instance.intermediate_indices = ['index_' + str(i) for i in range(10)]
# with InvertedIndexWriter(BSBI_instance.index_name, directory=BSBI_instance.output_dir,
#                          postings_encoding=BSBI_instance.postings_encoding) as merged_index:
#     with contextlib.ExitStack() as stack:
#         indices = [stack.enter_context(InvertedIndexIterator(index_id, directory=BSBI_instance.output_dir,postings_encoding=BSBI_instance.postings_encoding)) for
#                    index_id in BSBI_instance.intermediate_indices]
#         BSBI_instance.merge(indices, merged_index)

# test
# BSBI_instance1 = BSBIIndex(data_dir='test-pa1-data1',postings_encoding=CompressedPostings ,output_dir='test-output-dir')
# BSBI_instance1.index(False)
# print(BSBI_instance1.retrieve('boolean retrieval'))

# BSBI_instance1 = BSBIIndex(data_dir='pa1-data1',output_dir='output_dir')
# print(BSBI_instance1.retrieve('we are'))

# BSBI_instance = BSBIIndex(data_dir='pa1-data',output_dir='output_dir')
# BSBI_instance.index(True)

#
# BSBI_instance = BSBIIndex(data_dir='pa1-data', output_dir='output_dir_compressed',postings_encoding=CompressedPostings)
# BSBI_instance.index(False)
# for i in range(1, 9):
#     with open('dev_queries/query.' + str(i)) as q:
#         query = q.read().strip('\n')
#         my_results = sorted(set([os.path.normpath(path) for path in BSBI_instance.retrieve(query)]))
#         with open('dev_output/' + str(i) + '.out') as o:
#             reference_results = sorted(set(['pa1-data\\' + os.path.normpath(x.strip()) for x in o.readlines()]))
#             try:
#                 assert my_results==reference_results
#                 print("Results match for query:", query.strip())
#             except AssertionError:
#                 print(set(my_results) - set(reference_results))
#                 print(set(reference_results) - set(my_results))
#                 print(str(i) + " Results DO NOT match for query: " + query.strip())
#

# BSBI_instance = BSBIIndex(data_dir='test-pa1-data1',output_dir='test_gamma_output_dir',postings_encoding=GammaCompressor)
# BSBI_instance.index(False)
# print(BSBI_instance.retrieve('department'))

# TODO:
#  将数据库中的数据导出为文件（或者修改源程序使支持数据库内数据建倒排索引），直接用这个函数就可以构建好倒排索引
#  计算PageRank（networkx）并保存，计算文档向量（gensim）并保存
#  根据查询，返回url，根据url，获取PageRank、文档向量，进行相乘，评分，返回用户
# BSBI_instance = BSBIIndex(data_dir='pages/test', output_dir='index/test', index_name='test', postings_encoding=None)
# BSBI_instance.index(is_indexed=True,stopwords_path='stopwords.txt')
# print(jieba.cut('南开大学 曹雪涛 校学术委员会考核评审百名青',cut_all=True))
# print(''.join(jieba.cut('南开大学 曹雪涛 校学术委员会考核评审百名青')))
# print(BSBI_instance.retrieve([i for i in jieba.cut('南开大学 曹雪涛 校学术委员会考核评审百名青') if i !=' ']))
# print(BSBI_instance.term_tfidf[str(BSBI_instance.term_id_map.__getitem__('南开大学'))])
# print(BSBI_instance.term_tf_docid[BSBI_instance.term_id_map.__getitem__('南开大学')][0])
# print(BSBI_instance.term_tf_docid[BSBI_instance.term_id_map.__getitem__('南开大学')][1])
