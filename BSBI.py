
# You can add additional imports here
import sys
import pickle as pkl
import array
import os
import timeit
import contextlib

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
        if res or res==0:
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

if __name__ == '__main__':

    testIdMap = IdMap()
    assert testIdMap['a'] == 0, "Unable to add a new string to the IdMap"
    assert testIdMap['bcd'] == 1, "Unable to add a new string to the IdMap"
    # print(testIdMap['a'])
    assert testIdMap['a'] == 0, "Unable to retrieve the id of an existing string"
    assert testIdMap[1] == 'bcd', "Unable to retrive the string corresponding to a\
                                    given id"
    try:
        testIdMap[2]
    except IndexError as e:
        assert True, "Doesn't throw an IndexError for out of range numeric ids"
    assert len(testIdMap) == 2
