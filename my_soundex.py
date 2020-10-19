import threading


local = threading.local()


class My_Soundex(object):

    def __init__(self, string):
        # 将用户输入的string进行首尾去空格处理并转换为大写
        self.string = string.strip().upper()
        # 第一个字母之后应该去掉的字母
        self.__noise = ['A', 'E', 'I', 'O', 'U', 'Y', 'H', 'W']
        # 用来存储前一个字母的数字映射
        self.preNum = None
        # 数字映射列表
        self.Map = [  # A   B   C   D   E   F   G   H   I   J   K   L   M
            '0', '1', '2', '3', '0', '1', '2', '0', '0', '2', '2', '4', '5',
            # N   O   P   Q   R   S   T   U   V   W   X   Y   Z
            '5', '0', '1', '2', '6', '2', '3', '0', '1', '0', '2', '0', '2']
        # 存储结果
        self.__result = ''

    def Del_Noise(self):
        '''
        :param:self
        :return: 去噪后单词
        '''
        # 去掉不是字母的噪声
        for st in self.string:
            # 如果不是字母
            if not st.isalpha():
                self.string = self.string.replace(st, '')
        # 如果全部都是噪声，返回‘wrong’字符串
        if self.string == '':
            return 'Wrong'

        # 取第一个字母后的所有字母，用于去除有在['A','E','I','O','U','Y','H','W']中的字母
        stringBuffer = self.string[1:]
        # 如果第一个字母后的所有字母为空
        if not stringBuffer:
            # 直接返回
            return self.string
        # 如果不为空
        else:
            # 去除有在['A','E','I','O','U','Y','H','W']中的字母
            for noise in self.__noise:
                if stringBuffer == '':
                    break
                stringBuffer = stringBuffer.replace(noise, '')
            # 返回去除后的字符串
            return self.string[0] + stringBuffer

    def __call__(self, *args, **kwargs):
        return self.Del_Noise()

    @property
    def result(self):
        return self.__result

    def Soundex(self, size=4):
        '''
        :param size: 默认为4
        :return: 单词的Soundex映射结果
        '''
        # 第一步：去噪声和去除第一个字母后有在['A','E','I','O','U','Y','H','W']中的字母，赋给self.string
        self.string = self.Del_Noise()

        # 如果只有一个字母，补0后返回
        if self.string.__len__() == 1:
            return self.string + '0' * (size - 1)

        # 如果不止一个字母
        # 将第一个字母加入结果
        self.__result += self.string[0]

        # 遍历第一个字母后的所有单词
        for string in self.string[1:]:
            # 取出数字映射
            each = self.Map[ord(string) - ord('A')]
            # 如果取出的数字映射不等于前一个字母的数字映射，即相邻的两个被替换为同一个数字的字母只保留一个
            if each != self.preNum:
                # 将数字映射加入结果
                self.__result += each
                self.preNum = each

        # 如果得到的结果长度小于size，补0直到结果长度与size相同
        if self.__result.__len__() < size:
            for j in range(size - self.__result.__len__()):
                self.__result += '0'
        # 如果得到的结果长度大于或等于size，从左取出与size相同长度的result
        else:
            self.__result = self.__result[:size]
        # 返回单词的Soundex映射结果
        return self.result
