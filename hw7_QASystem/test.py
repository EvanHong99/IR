# from py2neo import Graph,Node,Relationship
# graph = Graph("http://localhost:7474",username='neo4j',password='Qazwsxedcrfv0957')
#
from ExtraSPO_LTP import Extractor

if __name__ == '__main__':
    e=Extractor()
    e.read_file(r'test_page/9042.txt')
    print(e)