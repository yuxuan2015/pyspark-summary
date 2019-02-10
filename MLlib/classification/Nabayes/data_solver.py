# -*- coding:utf-8 -*-

import os, re, sys, unicodedata

# 清洗用的正则表达式
res = re.compile(r'\s+')
#清洗全是数字的词
red = re.compile(r'^(\d+)$')
# 清洗标点符号等异常字符
todel = dict.fromkeys(i for i in range(sys.maxunicode)
                      if unicodedata.category(chr(i)) not in ('Lu', 'Ll', 'Lt', 'Lo', 'Nd', 'Nl', 'Zs'))

#文件路径

def file_path(n):
    if n == 0:
        return os.path.abspath('.')
    else:
        return '\\'.join(os.path.abspath('.').split('\\')[:-n])

#载入停用词、保留词、词典删除词
def load_data(filepath):
    return [value.replace('\n', '') for value in open(filepath, encoding='utf8').readlines()]

###########清洗分词结果
#去除标点符号
def sub_punction(corpus):
    return re.sub("[\s+\.\!\/_,$%^*(+\"\']+|[+——！，。？?、~@#￥%……&*（）]+", "", corpus)


# 清洗分词结果的方法
def remove_muns(x):
    if x == '2g':
        return '2G'
    else:
        return re.sub(red, '', x)

def cleantext(text):
    # try:
    #     text = unicode(text)
    # except:
    #     pass
    if text != '':
        return re.sub(res, ' ', ' '.join(map(lambda x: remove_muns(x), text.translate(todel).split(' ')))).strip()
    else:
        return text