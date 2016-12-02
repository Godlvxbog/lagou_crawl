#!/usr/bin/env python
# -*- encoding: utf-8 -*-

from pyspider.libs.base_handler import *
import pymysql
import random


class Handler(BaseHandler):
    crawl_config = {
            'headers': {
                'User-Agent': 'GoogleBot',
                'Host':'www.lagou.com',
            },
             'itag': 'v233'
    }

    def  __init__(self):
        self.db=pymysql.connect(host="localhost",user="root",passwd="xbog",db="words",charset="utf8")

    def add_info(self,url,title,content,money,district,jinyan,xueli,quanzhi,youshi,date,gongsi_name,gongsi_lingyu,gongsi_guimo,gongsi_web,gongsi_jieduan,gongsi_addr):
        try:
            cursor =self.db.cursor()
            sql='insert into lagou6(url,title,content,money,district,jinyan,xueli,quanzhi,youshi,date,gsname,lingyu,guimo,zhuye,jieduan,addr) VALUES ("%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s")' %(url,title,content,money,district,jinyan,xueli,quanzhi,youshi,date,gongsi_name,gongsi_lingyu,gongsi_guimo,gongsi_web,gongsi_jieduan,gongsi_addr);

            print sql
            cursor.execute(sql)
            print cursor.lastrowid
            self.db.commit()
        except Exception as e:
            print(e)
            self.db.rollback()



    @every(minutes=24 * 60)
    def on_start(self):
        self.crawl('https://www.lagou.com/', callback=self.index_page, validate_cert=False)

    @config(age=10 * 24 * 60 * 60)
    def index_page(self, response):
         if(response.ok):
            for each in response.doc('dl dd a[href^="https://www.lagou.com/zhaopin/"] ').items():
                url=each.attr['href']
                if url.find('//') ==0:
                      url=url[url.find('//')+2:]
                print(url!=None)
                if(url!=None):
                    self.crawl(url, callback=self.board_page, validate_cert=False,itag=each.find('.update-time').text())


    @config(priority=9999976792)
    def board_page(self, response):
        if(response.ok):
           for each in response.doc('a.position_link').items():
                url=each.attr.href
                if(response.ok):
                     if(url!=None):
                        self.crawl(url, callback=self.detail_page, validate_cert=False)
           for each in response.doc('div.pager_container a[href^="https://www.lagou.com/zhaopin/"]').items():
                if(each.attr.href!=None):
                    print(each.attr.href)
                    self.crawl(each.attr.href, callback=self.board_page, validate_cert=False)



    @config(priority=1)
    def detail_page(self, response):
         if(response.ok):
            title=response.doc('h1').text()
            url=response.url
            content=response.doc('dd.job_bt > p').text()
            money=response.doc('dd.job_request > p:nth-child(1) > span:nth-child(1)').text()
            district=response.doc('dd.job_request > p:nth-child(1) > span:nth-child(2)').text()
            jinyan=response.doc('dd.job_request > p:nth-child(1) > span:nth-child(3)').text()
            xueli=response.doc('dd.job_request > p:nth-child(1) > span:nth-child(4)').text()
            quanzhi=response.doc('dd.job_request > p:nth-child(1) > span:nth-child(5)').text()
            youshi=response.doc('dd.job_request > p:nth-child(2) ').text()
            date=response.doc('p.publish_time').text()

            gongsi_name=response.doc('h2.fl').text()
            gongsi_lingyu=response.doc('dd > ul:nth-child(1) > li:nth-child(2)').text()
            gongsi_guimo=response.doc('dd > ul:nth-child(1) > li:nth-child(1)').text()
            gongsi_web=response.doc('dd > ul> li a').text()
            gongsi_jieduan=response.doc('dd > ul:nth-child(3) > li').text()
            gongsi_addr=response.doc('div.work_addr').text()


            self.add_info(url,title,content,money,district,jinyan,xueli,quanzhi,youshi,date,gongsi_name,gongsi_lingyu,gongsi_guimo,gongsi_web,gongsi_jieduan,gongsi_addr)


            return {
                '路径':url,
                '标题':title,
                '内容':content,
                '薪资':money,
                '地区':district,
                '经验要求':jinyan,
                '学历要求':xueli,
                '全职否':quanzhi,
                '优势':youshi,
                '发布日期':date,
                '公司':gongsi_name,
                '领域':gongsi_lingyu,
                '规模':gongsi_guimo,
                '公司主页':gongsi_web,
                '公司阶段':gongsi_jieduan,
                '公司地址':gongsi_addr

            }




