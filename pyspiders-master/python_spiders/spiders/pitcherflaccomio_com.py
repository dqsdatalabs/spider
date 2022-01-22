# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import re
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
 
class MySpider(Spider):
    name = 'pitcherflaccomio_com'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = "Pitcherflaccomio_PySpider_italy"
    start_urls = ['https://www.pitcherflaccomio.com/rentals/long-term/']  # LEVEL 1

    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get('page', 2)
        
        seen = False
        for item in response.xpath("//h4/a[1]/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 2 or seen:
            url = f"https://www.pitcherflaccomio.com/rentals/long-term/page{page}/"
            yield Request(url, callback=self.parse, meta={"page": page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        property_type = "".join(response.xpath("//div[@id='cx']//p//text()").getall())
        if get_p_type_string(property_type):
            item_loader.add_value("property_type", get_p_type_string(property_type))
        else:
            return
        item_loader.add_value("external_source", self.external_source)


        external_id = response.xpath(
            "//h1[@class='propertydetails']//span[@class='code']//text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id)

        title = response.xpath(
            "//title//text()").get()
        if title:
            item_loader.add_value("title", title)

        description = response.xpath(
            "//div[@id='cx']//p//text()").getall()
        if description:
            item_loader.add_value("description", description)

        rent = response.xpath(
            "//p[contains(text(),'Monthly')]/text()").get()
        if rent:
            if '€' in rent:
                item_loader.add_value("rent", rent.split("€")[-1].replace(",",""))
            else:
                rent = response.xpath("//p[contains(text(),'Monthly')]/text()").get()
                if rent:
                    rent = rent.split("€")[-1].replace(",","")
                    item_loader.add_value("rent",rent)
        item_loader.add_value("currency", "EUR")
 

        images = [response.urljoin(x) for x in response.xpath("//a[@class='immaginitop']/@href").getall()]
        if images:
            item_loader.add_value("images", images)

        item_loader.add_value("landlord_name", "Pitcher & Flaccomio")
        item_loader.add_value("landlord_phone", "+39 055 2343354")
        item_loader.add_value("city","Florence")
        landlord_mail = response.xpath("//a[contains(@href,'mailto')]/@href").get()
        if landlord_mail:
            landlord_mail = landlord_mail.split(":")[-1]
            item_loader.add_value("landlord_email",landlord_mail)

        address = response.xpath("//span[@class='des']/text()").get()
        if address:
                address = address.split("–")[1].strip()
                item_loader.add_value("address",address + " Florence")



        room_count = response.xpath("//span[@class='des']/text()").get()
        if room_count:
            room_count = room_count.split("–")[-1].strip().split()[0]
            item_loader.add_value("room_count",room_count)

        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and ("appartament" in p_type_string.lower() or "apartment" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("trilocale" in p_type_string.lower() or "house" in p_type_string.lower() or "villetta" in p_type_string.lower() or "villino" in p_type_string.lower() or "villa" in p_type_string.lower() or "attico" in p_type_string.lower()):
        return "house"
    elif p_type_string and "monolocale" in p_type_string.lower():
        return "studio"
    elif p_type_string and "bedroom" in p_type_string.lower():
        return "apartment"
    else:
        return None