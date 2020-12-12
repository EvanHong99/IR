"""
steps:
    1.
    2.
"""

# from whoosh.fields import Schema, TEXT, ID
#
# schema = Schema(url=TEXT, title=TEXT, hook=TEXT, content=TEXT)
# import os.path
# from whoosh.index import create_in
#
# # 创建 schema 的索引
# if not os.path.exists("index"):
#     os.mkdir("index")
# idx = create_in("index", schema)
#
# # 打开一个已创建的索引
# # 方法一 使用FileStorage对象
# from whoosh.filedb.filestore import FileStorage
# storage = FileStorage(idx_path)  #idx_path 为索引路径
# idx = storage.open_index(indexname=indexname, schema=schema)
#
# # 方法二 使用open_dir函数
# from whoosh.index import open_dir
# idx = open_dir(indexname=indexname)  #indexname 为索引名
#
#
# # 一旦有了 index 对象，我们就需要在 index 里写入需要被检索的信息，所以 IndexWriter 对象就是用来提供一个 add_document(**kwargs) 方法来在之前声明的各种 Fields 里写入数据
# writer = idx.writer()  #IndexWriter对象
# writer.add_document(
#     title=u"Document Title",
#     path=u"/a",
#     content=u"Hello Whoosh"
# )  # Field 和 schema 中声明的一致
# writer.commit()  # 保存以上document
#
# # search
# with idx.sercher() as searcher:
#     ...
#
# # 使用解析器解析查询字段
# from whoosh.qparser import QueryParser
# parser = QueryParser("content", idx.schema)
# myquery = parser.parse(querystring)
#
# # TODO:值得注意的是 search() 接收一个默认参数 weighting=BM25F 这是搜索的权重算法，它是个 whoosh.scoring.Weighting 对象，通过使用内置的 score 方法来计算搜索的优先级从而查询文档索引
# # 常用searchpage
# results = searcher.search_page(myquery, page_num, page_len)
# results = searcher.search(myquery)
# print(results[0])
from __future__ import unicode_literals

from whoosh.fields import Schema, TEXT, ID
import os.path
from whoosh.index import create_in
from jieba.analyse import ChineseAnalyzer
from whoosh.qparser import QueryParser
analyzer = ChineseAnalyzer()

schema = Schema(title=TEXT(stored=True), path=ID(stored=True), content=TEXT(stored=True, analyzer=analyzer))

idx = create_in("index/test/", schema,indexname="test_index")
writer = idx.writer()
writer.add_document(
    title="test-document",
    path="/a",
    content="This is the document for test first 中文 你好"
)
writer.add_document(
    title="test-document1",
    path="index/test",
    content="水果 你 the test"
)
writer.commit()
searcher = idx.searcher()
parser = QueryParser("content", schema=idx.schema)
parser1 = QueryParser("title", schema=idx.schema)

for keyword in ("水果","你","first","中文","交换机","交换","test","the"):
    print("result of ",keyword)
    q = parser.parse(keyword)
    q1 = parser1.parse(keyword)
    results = searcher.search(q)
    results1 = searcher.search(q1)
    for i in results:
        print(i)
    for i in results1:
        print(i)
    print("="*10)


