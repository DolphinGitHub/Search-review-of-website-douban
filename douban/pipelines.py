# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import json
import codecs
from lxml import etree
import urllib2
import re,string

import sys
reload(sys)
sys.setdefaultencoding('utf-8')


def FindContent(url): #把文章写入到TXT中
    num=url[32:38]

    u=urllib2.urlopen(url)
    f=u.read()

    tree=etree.HTML(f)
    Cont=tree.xpath(r'//div[@id="link-report"]/div//text()')
    p = re.compile(r'main-title-rating" title=".*?"')
    rate=p.findall(f)
    Rate=rate[0][-7:-1]
    if (len(Cont)!=0):

        Num_r = open('num.txt', 'r')
        n=string.atoi(Num_r.read())
        Num_r.close()

        Num_w = open('num.txt', 'w')
        Num_w.write(str(n+1))
        Num_w.close()

        if(n%2==0):
            if (Rate == '\xe5\x8a\x9b\xe8\x8d\x90'): path=r'/Users/dolphin/Desktop/good/essay' + str(num) + '.txt'
            if (Rate == '\xe6\x8e\xa8\xe8\x8d\x90'): path=r'/Users/dolphin/Desktop/good/essay' + str(num) + '.txt'
            if (Rate == '\xe8\xbe\x83\xe5\xb7\xae'): path=r'/Users/dolphin/Desktop/good/essay' + str(num) + '.txt'
            if (Rate == '\xe5\xbe\x88\xe5\xb7\xae'): path=r'/Users/dolphin/Desktop/bad/essay' + str(num) + '.txt'
            if (Rate == '\xe8\xbf\x98\xe8\xa1\x8c'): path=r'/Users/dolphin/Desktop/bad/essay' + str(num) + '.txt'
        else:
            path = r'/Users/dolphin/Desktop/review/essay' + str(num) + '.txt'
        fw = open(path, 'w')
        for i in Cont:
            if(i!="    "):
                fw.write('    '+i+'\n')
        fw.close()




class DoubanPipeline(object):
    def __init__(self):
        self.file = codecs.open('Douban_URL.json', mode='wb', encoding='utf-8')

    def process_item(self, item, spider):
        line = json.dumps(dict(item))+'\n'
        self.file.write(line.decode("unicode_escape"))
        fw=open('url.txt','a')
        for i in item['url']:
            fw.write(i+'\n')
            FindContent(i)
        fw.close()

        return item
