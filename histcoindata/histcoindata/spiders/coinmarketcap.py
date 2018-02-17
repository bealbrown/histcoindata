# -*- coding: utf-8 -*-
import scrapy
from scrapy.linkextractors import LinkExtractor
import re
from datetime import datetime
from dateutil import parser
import sqlite3
import pandas
from pandas.io import sql
from sqlalchemy import create_engine

def df_to_sql(df, date):
    disk_engine = create_engine('sqlite:///histcoinprices.db')
    df.to_sql(date.strftime('%Y%b%d'), disk_engine, if_exists='append')


class CoinmarketcapSpider(scrapy.Spider):
    name = 'coinmarketcap'
    allowed_domains = ['coinmarketcap.com']
    start_urls = ['http://coinmarketcap.com/historical/']

    def parse(self, response):

        for link in LinkExtractor(allow=(r'historical/[0-9]{8}')).extract_links(response):

            yield scrapy.Request(
                link.url,
                callback=self.day_to_df
            )

        # yield scrapy.Request(
        #     "https://coinmarketcap.com/historical/20160214/",
        #     callback=self.day_to_df
        # )

    def day_to_df(self, response):

        table = response.css('table#currencies-all').extract()

        regex = re.compile(r'(?:Historical Snapshot - )(.*?[0-9]{4})', re.I)
        date = regex.search(str(response.body)).groups()
        date = datetime.strptime(date[0], '%B %d, %Y')

        table = ''.join(table).replace("\n","")

        df = pandas.read_html(table)
        df = df[0] # read html returns list of dataframes, so have to get first (and only) one

        return (df_to_sql(df,date))





