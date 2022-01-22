# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re

class MySpider(Spider):
    name = 'huntercampbell_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'

    def start_requests(self):

        start_urls = [
            {
                "url" : [
                    "http://www.huntercampbell.co.uk/search/685641/page1/",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "http://www.huntercampbell.co.uk/search/685642/page1/",
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//ul[@id='list']/li/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        next_button = response.xpath("//li[@class='next']/a/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Huntercampbell_Co_PySpider_united_kingdom")

        title = response.xpath("//h1//text()").get()
        if title:
            item_loader.add_value("title", title)
            item_loader.add_value("address", title)
            item_loader.add_value("zipcode", title.split(",")[-1].strip())
            item_loader.add_value("city", title.split(",")[1].split(",")[0].strip())
        
        rent = response.xpath("//div[contains(@class,'price')]//span//text()").get()
        if rent:
            rent = rent.split("£")[1].split("pm")[0].replace(",","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "GBP")

        room_count = "".join(response.xpath("//div[contains(@class,'dtsm')]//li[contains(.,'Bed')]//text()").getall())
        if room_count:
            room_count = re.sub('\s{2,}', ' ', room_count.strip())
            item_loader.add_value("room_count", room_count.split(" ")[0])
        
        terrace = "".join(response.xpath("//div[contains(@class,'dtsm')]//li[contains(.,'Terrace')]//text()").getall())
        if terrace:
            terrace = re.sub('\s{2,}', ' ', terrace.strip())
            item_loader.add_value("terrace", True)

        images = [x for x in response.xpath("//ul[contains(@id,'gallery')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        desc = " ".join(response.xpath("//ul[contains(@class,'feats')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        else:
            desc = " ".join(response.xpath("//h2/parent::div/div[contains(@class,'textbp')][1]").getall())
            if desc:
                desc = re.sub('\s{2,}', ' ', desc.strip())
                item_loader.add_value("description", desc)

        if desc and "deposit " in desc.lower():
            try:
                deposit = desc.lower().split("deposit ")[1].split("£")[1].strip().split(" ")[0]
                item_loader.add_value("deposit", deposit)
            except:
                deposit = response.xpath("//li[contains(text(),'Deposit')]/text()").get()
                if deposit:
                    deposit = deposit.split()[-2].split("€ ")
                    item_loader.add_value("deposit",deposit)


        features = "".join(response.xpath("//ul[@class='feats']/li/text()").getall())
        if features:
            if "parking" in features.lower():
                item_loader.add_value("parking", True)

        if "floor" in desc:
                floor = desc.split("floor")[0].strip().split(" ")[-1]
                not_list = ["new"]
                status = True
                for i in not_list:
                    if i in floor.lower():
                        status = False
                if status:
                    item_loader.add_value("floor", floor.replace("-","").upper())

        landlord_name = response.xpath("//span[contains(@class,'name')]//text()").get()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name.strip())
            if "Carrickfergus" in landlord_name:
                item_loader.add_value("landlord_email", "nick@huntercampbell.co.uk")
            elif "Larne" in landlord_name:
                item_loader.add_value("landlord_email", "jennifer@huntercampbell.co.uk")
            else:
                item_loader.add_value("landlord_email", "jill@huntercampbell.co.uk")
        
        landlord_phone = response.xpath("//span[contains(.,'Office')]//a//text()").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone)


        yield item_loader.load_item()