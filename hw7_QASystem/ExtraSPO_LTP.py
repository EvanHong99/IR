from ltp import LTP
from py2neo import Graph, Node, Relationship

graph = Graph("http://localhost:7474", username='neo4j', password='Qazwsxedcrfv0957')
ltp = LTP()


class Extractor:
    def __init__(self):
        global ltp
        global graph


    def read_file(self, path):
        """
        读取一个文档
        """
        with open(path, 'r',encoding='utf-8') as fr:
            self.segs, self.hidden = ltp.seg(fr.read().split(' '))
            self.srls = ltp.srl(self.hidden, keep_empty=False)
            assert len(self.segs) == len(self.srls)

    def concat_str(self, seg, triple: tuple):
        """
        Parameters
        --------
        :param triple: tuple, ('ARG2', 3, 5)
        """
        return ''.join(seg[triple[1]:(triple[2] + 1)])

    def extract(self):
        """
        完成对一篇文章的分析
        """
        for i in range(len(self.segs)):
            seg = self.segs[i]
            srl = self.srls[i]
            for tup in srl:
                relation_name = seg[tup[0]]
                so_list = tup[1]
                s = []
                o = []
                properties = []
                for arg in so_list:
                    if arg[0] == 'A0':
                        s.append(arg)
                    elif 'ARGM' not in arg[0]:
                        o.append(arg)
                    else:
                        properties.append(arg)

                # 生成关系语句
                for node1 in s:
                    for node2 in o:
                        graph.run("match (m:" + node1[0] + "{name:\'" + self.concat_str(seg,node1) + "\'}),\
                        (n:" +node2[0] + "{name:\'" + self.concat_str(seg, node2) + "\'})\
                            merge (m)-[r:" + relation_name + "]-(n)\
                            ON CREATE SET r.value=1\
                            on match set r.value=r.value+1;")



    def retrieve(self, question:str):
        """
        
        Args:
            question: 

        Returns:

        """
        segs, hidden = ltp.seg([question])
        for i in range(len(segs)):
            seg = segs[i]
            srl = srls[i]
            for tup in srl:
                relation_name = seg[tup[0]]
                so_list = tup[1]
                s = []
                o = []
                properties = []
                for arg in so_list:
                    if arg[0] == 'A0':
                        s.append(arg)
                    elif 'ARGM' not in arg[0]:
                        o.append(arg)
                    else:
                        properties.append(arg)

                # 生成关系语句
                for node1 in s:
                    for node2 in o:

if __name__ == '__main__':
    e=Extractor()
    e.read_file(r'test_page/9042.txt')
    e.extract()

#     [[(6, [('A0', 0, 1), ('ARGM-LOC', 2, 5), ('A1', 8, 24)]),
#       (9, [('A1', 10, 13)]),
#       (18, [('ARGM-TMP', 8, 14), ('A0', 16, 16), ('ARGM-ADV', 17, 17), ('A1', 19, 24)]),
#       (19, [('ARGM-TMP', 8, 14), ('A1', 20, 22), ('A2', 23, 24)]),
#       (27, [('ARGM-ADV', 26, 26)]),
#       (29, [('ARGM-ADV', 26, 26)]), (39, [('A1', 41, 41)]),
#       (49, [('A0', 44, 46), ('A1', 50, 50)]),
#       (51, [('ARGM-TMP', 8, 14), ('A0', 16, 16), ('A1', 52, 52)]),
#       (60, [('A2', 54, 59), ('A1', 61, 61)]),
#       (69, [('A0', 16, 16)]),
#       (78, [('A1', 79, 79), ('A0', 81, 81)]),
#       (86, [('A0', 81, 81), ('ARGM-MNR', 82, 85)]),
#       (87, [('A0', 81, 81), ('A1', 88, 89)]),
#       (92, [('A0', 81, 81), ('A1', 93, 95)]),
#       (95, [('A0', 93, 93), ('ARGM-ADV', 94, 94)])]]
#
# [[(1, 2, 'FEAT'), (2, 7, 'AGT'), (3, 5, 'mRELA'), (4, 5, 'FEAT'), (6, 5, 'mDEPD'), (7, 0, 'Root'), (8, 7, 'mPUNC'),
#   (11, 12, 'FEAT'), (12, 14, 'FEAT'), (14, 10, 'CONT'), (15, 10, 'mDEPD'), (17, 20, 'AGT'), (17, 52, 'AGT'),
#   (18, 19, 'mDEPD'), (20, 7, 'dCONT'), (20, 19, 'dCONT'), (21, 22, 'SCO'), (22, 20, 'DATV'), (22, 23, 'eCOO'),
#   (24, 20, 'eSUCC'), (25, 24, 'MANN'), (26, 24, 'mPUNC'), (27, 28, 'MANN'), (28, 20, 'eSUCC'), (29, 28, 'mRELA'),
#   (29, 30, 'mRELA'), (31, 33, 'mRELA'), (33, 40, 'LOC'), (34, 33, 'mDEPD'), (40, 42, 'rCONT'), (41, 40, 'mDEPD'),
#   (42, 30, 'CONT'), (45, 47, 'SCO'), (46, 47, 'FEAT'), (46, 60, 'FEAT'), (47, 50, 'AGT'), (48, 49, 'MANN'),
#   (51, 50, 'CONT'), (53, 52, 'CONT'), (54, 52, 'mPUNC'), (55, 60, 'mRELA'), (55, 69, 'mRELA'), (56, 57, 'FEAT'),
#   (56, 60, 'FEAT'), (57, 60, 'FEAT'), (58, 57, 'mDEPD'), (59, 60, 'FEAT'), (60, 61, 'DATV'), (62, 61, 'CONT'),
#   (63, 61, 'mPUNC'), (64, 60, 'mRELA'), (64, 69, 'mRELA'), (65, 57, 'FEAT'), (65, 66, 'FEAT'), (68, 60, 'FEAT'),
#   (68, 69, 'FEAT'), (68, 78, 'FEAT'), (71, 70, 'CONT'), (72, 70, 'mPUNC'), (73, 69, 'mRELA'), (73, 78, 'mRELA'),
#   (74, 75, 'FEAT'), (75, 78, 'FEAT'), (76, 75, 'mDEPD'), (77, 69, 'FEAT'), (77, 78, 'FEAT'), (78, 79, 'DATV'),
#   (79, 70, 'eCOO'), (80, 79, 'CONT'), (81, 7, 'mPUNC'), (82, 87, 'AGT'), (82, 88, 'AGT'), (82, 93, 'AGT'),
#   (83, 85, 'mRELA'), (84, 85, 'mPUNC'), (85, 87, 'EXP'), (86, 85, 'mPUNC'), (89, 90, 'FEAT'), (90, 88, 'PAT'),
#   (91, 87, 'mPUNC'), (91, 88, 'mPUNC'), (92, 79, 'mRELA'), (92, 93, 'mRELA'), (94, 96, 'EXP'), (95, 96, 'mDEPD'),
#   (96, 93, 'CONT'), (97, 7, 'mPUNC'), (97, 93, 'mPUNC')]]
