# -*- coding:utf-8 -*-

from scrapy.spiders import CrawlSpider, Rule
from scrapy.contrib.linkextractors.sgml import SgmlLinkExtractor
from scrapy.selector import Selector
from douban.items import DoubanItem
import cmath
from BitVector import BitVector


class BloomFilter(object):    #用于去除重复的URL
    def __init__(self, error_rate, elementNum):
        #计算所需要的bit数
        self.bit_num = -1 * elementNum * cmath.log(error_rate) / (cmath.log(2.0) * cmath.log(2.0))

        #四字节对齐
        self.bit_num = self.align_4byte(self.bit_num.real)

        #分配内存
        self.bit_array = BitVector(size=self.bit_num)

        #计算hash函数个数
        self.hash_num = cmath.log(2) * self.bit_num / elementNum

        self.hash_num = self.hash_num.real

        #向上取整
        self.hash_num = int(self.hash_num) + 1

        #产生hash函数种子
        self.hash_seeds = self.generate_hashseeds(self.hash_num)

    def insert_element(self, element):
        for seed in self.hash_seeds:
            hash_val = self.hash_element(element, seed)
            #取绝对值
            hash_val = abs(hash_val)
            #取模，防越界
            hash_val = hash_val % self.bit_num
            #设置相应的比特位
            self.bit_array[hash_val] = 1

    #检查元素是否存在，存在返回true，否则返回false
    def is_element_exist(self, element):
        for seed in self.hash_seeds:
            hash_val = self.hash_element(element, seed)
            #取绝对值
            hash_val = abs(hash_val)
            #取模，防越界
            hash_val = hash_val % self.bit_num

            #查看值
            if self.bit_array[hash_val] == 0:
                return False
        return True

    #内存对齐
    def align_4byte(self, bit_num):
        num = int(bit_num / 32)
        num = 32 * (num + 1)
        return num

    #产生hash函数种子,hash_num个素数
    def generate_hashseeds(self, hash_num):
        count = 0
        #连续两个种子的最小差值
        gap = 50
        #初始化hash种子为0
        hash_seeds = []
        for index in xrange(hash_num):
            hash_seeds.append(0)
        for index in xrange(10, 10000):
            max_num = int(cmath.sqrt(1.0 * index).real)
            flag = 1
            for num in xrange(2, max_num):
                if index % num == 0:
                    flag = 0
                    break

            if flag == 1:
                #连续两个hash种子的差值要大才行
                if count > 0 and (index - hash_seeds[count - 1]) < gap:
                    continue
                hash_seeds[count] = index
                count = count + 1

            if count == hash_num:
                break
        return hash_seeds

    def hash_element(self, element, seed):
        hash_val = 1
        for ch in str(element):
            chval = ord(ch)
            hash_val = hash_val * seed + chval
        return hash_val


bf = BloomFilter(0.001, 1000000)

class CSDNBlogCrawlSpider(CrawlSpider):
    """继承自CrawlSpider，实现自动爬取的爬虫。"""

    name = "douban"
    # 设置下载延时
    download_delay = 2
    allowed_domains = ['douban.com']
    # 第一篇文章地址
    start_urls = ['https://book.douban.com/tag/%E9%83%AD%E6%95%AC%E6%98%8E']

    # rules编写法一，官方文档方式
    # rules = [
    #    #提取“下一篇”的链接并**跟进**,若不使用restrict_xpaths参数限制，会将页面中所有
    #    #符合allow链接全部抓取
    #    Rule(SgmlLinkExtractor(allow=('/u012150179/article/details'),
    #                          restrict_xpaths=('//li[@class="next_article"]')),
    #         follow=True)
    #
    #    #提取“下一篇”链接并执行**处理**
    #    #Rule(SgmlLinkExtractor(allow=('/u012150179/article/details')),
    #    #     callback='parse_item',
    #    #     follow=False),
    # ]

    # rules编写法二，更推荐的方式（自己测验，使用法一时经常出现爬到中间就finish情况，并且无错误码）
    rules = [
             Rule(SgmlLinkExtractor(allow=('book\.douban\.com/subject/.*?/$'),deny=('wishes|discussion|offer|doings|doulists|reviews|annotation'), restrict_xpaths=('//div')),
                  callback='parse_item', follow=True)
    ]
    #follow=True一定要写！
    #allow(a regular expression( or list of)) – 必须要匹配这个正则表达式(或正则表达式列表)的URL才会被提取｡如果没有给出(或为空), 它会匹配所有的链接｡

    def parse_item(self, response):
        # print "parse_item>>>>>>"
        item = DoubanItem()
        sel = Selector(response)
        blog_url = sel.xpath('//a[contains(@href,\'book.douban.com/review/\')]/@href').extract()

        join_url=[]
        for i in blog_url:
            if(bf.is_element_exist(i)==False):
                join_url.append(i)
                bf.insert_element(i)


        item['url'] = [n.encode('utf-8') for n in join_url]

        yield item