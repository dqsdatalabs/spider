# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import json
import re
import urllib
import itemadapter
import scrapy
from scrapy.loader import ItemLoader
from python_spiders.helper import format_date
from ..loaders import ListingLoader
from word2number import w2n
from scrapy import Request,FormRequest

class NVEstatesSpider(scrapy.Spider):
 
    name = "nv_estates"
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    external_source="nv_estates_PySpider_united_kingdom_en"
    def start_requests(self):
        url = "https://www.nvestates.co.uk/property-results/"
        yield Request(url, callback=self.parse)

    def parse(self, response):
        for item in response.xpath("//a[.='More Details']/@href").getall():
            yield Request(item, callback=self.populate_item)
        next_button = f"https://www.nvestates.co.uk/property-results/page/{page}/"
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse,meta={"page":page+1})

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source",self.external_source)      

        title=response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title",title)
        adres=response.xpath("//h1[@class='property_title entry-title']/text()").get()
        if adres:
            item_loader.add_value("address",adres)
        rent=response.xpath("//div[@class='price twnewprice']/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("£")[-1].split("pcm")[0].replace(",",""))
        item_loader.add_value("currency","GBP")
        description=response.xpath("//div[@class='summary-contents']/text()").get()
        if description:
            item_loader.add_value("description",description)
        images=[x for x in response.xpath("//ul//li//img//@src").getall()]
        if images:
            item_loader.add_value("images",images)
        item_loader.add_value("landlord_phone","+44 (0)1865 435736")
        yield item_loader.load_item()


