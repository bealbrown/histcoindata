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

def df_to_sql(df, nameofcoin):

    disk_engine = create_engine('sqlite:///histcoinprices.db')
    df.to_sql(nameofcoin, disk_engine, if_exists='append')

class AllcurrencySpider(scrapy.Spider):
    name = 'allcurrency'
    allowed_domains = ['coinmarketcap.com']
    start_urls = ['https://coinmarketcap.com/all/views/all/']

    def parse(self, response):

        for link in LinkExtractor(allow=(r'currencies'), deny=(r'markets')).extract_links(response):

            yield scrapy.Request(
                link.url + "historical-data/?start=20100101&end=20200101",
                callback=self.hist_to_df
            )

    def hist_to_df(self, response):

        nameofcoin = re.search('currencies/(.*)/historical-data', response.url)
        nameofcoin = nameofcoin.group(1)

        table = response.css('table.table').extract()

        table = ''.join(table).replace("\n","")

        df = pandas.read_html(table)
        df = df[0] # read html returns list of dataframes, so have to get first (and only) one

        return (df_to_sql(df,nameofcoin))





