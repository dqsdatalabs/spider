# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from typing import NewType
from parsel.utils import extract_regex
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser
import re

class MySpider(Spider):
    name = 'calibrerealestate_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    external_source='Calibrerealestate_PySpider_australia'
    custom_settings = {
    "HTTPCACHE_ENABLED": False
    }
    def start_requests(self):
        start_urls = [
            {"url": "https://www.calibrerealestate.com.au/renting/search-rental-apartments-and-houses-in-brisbane/?keyword=&property_type%5B%5D=Apartment&price_min=&price_max=&bedrooms=&bathrooms=","prop_type":"apartment"},
            {"url": "https://www.calibrerealestate.com.au/renting/search-rental-apartments-and-houses-in-brisbane/?keyword&property_type%5B0%5D=House&property_type%5B1%5D=Unit&price_min&price_max&bedrooms&bathrooms","prop_type":"house"},
            {"url": "https://www.calibrerealestate.com.au/renting/search-rental-apartments-and-houses-in-brisbane/?keyword=&property_type%5B%5D=Studio&price_min=&price_max=&bedrooms=&bathrooms=","prop_type":"studio"}
        ]  # LEVEL 1       
        for url in start_urls:
            yield Request(url=url.get('url'),callback=self.parse,meta={"prop_type":url.get("prop_type")})
    # 1. FOLLOWING


    def parse(self, response):


        for item in response.xpath("//div[@class='overview text-center']/a/@href").getall():
            follow_url = item
            yield Request(follow_url,callback=self.populate_item,meta={"prop_type":response.meta.get("prop_type")})


        follow_page = response.xpath("//a[@class='next_page_link']/@href").get()
        if  follow_page:

            yield Request(follow_page, callback=self.parse,meta={'prop_type':response.meta.get("prop_type")})
            

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("property_type",response.meta.get("prop_type"))

        title = response.xpath("//span[@class='headline-title']/text()").get()
        if title:
            item_loader.add_value("title",title)

        address = response.xpath("//span[@class='headline-address']/text()").get()
        if address:
            item_loader.add_value("address",address)
            city = address.split(",")[-1]
            item_loader.add_value("city",city)

        price = response.xpath("//p[@class='price-value']/text()").get()
        if price:
            price = re.search("\d+",price)
            if price:
                price = price[0]
                item_loader.add_value("rent",4 * int(price))

        available = response.xpath("//p[@class='available-date']/text()").get()
        if available:
            available = available.split(":")[-1].strip()
            item_loader.add_value("available_date",available)

        
        room = response.xpath("//li[@class='bedrooms']/span[@class='room_count']/text()").get()
        if room:
            item_loader.add_value("room_count",room)

        bathroom = response.xpath("//li[@class='bathrooms']/span[@class='room_count']/text()").get()
        if bathroom:
            item_loader.add_value("bathroom_count",bathroom)

        desc = " ".join([txt.strip() for txt in response.xpath("//div[@id='realty_widget_property_description']/p/text()").getall()])
        if desc:
            item_loader.add_value("description",desc)

        
        images = response.xpath("//figure/a/@href").getall()
        if images:
            item_loader.add_value("images",images)

        if "furnished" in desc.lower():
            item_loader.add_value("furnished",True)

        if "balcony" in desc.lower():
            item_loader.add_value("balcony",True)

        if "washing machine" in desc.lower():
            item_loader.add_value("washing_machine",True)

        if "dishwasher" in desc.lower():
            item_loader.add_value("dishwasher",True)

        external_id = response.url.strip("/").split("-")[-1]
        item_loader.add_value("external_id",external_id)


        garage = response.xpath("//li[@class='carspaces']")
        if garage:
            item_loader.add_value("parking",True)

        item_loader.add_value("landlord_phone","07 3367 3411")
        item_loader.add_value("landlord_email","sales@calibrerealestate.com.au")
        item_loader.add_value("landlord_name","RED HILL SALES AND PROPERTY MANAGEMENT OFFICE")
        item_loader.add_value("currency","USD")

        lat = response.xpath("//meta[@itemprop='latitude']/@content").get()
        long = response.xpath("//meta[@itemprop='latitude']/@content").get()

        item_loader.add_value("latitude",lat)
        item_loader.add_value("longitude",long)

        yield item_loader.load_item()





def get_p_type_string(p_type_string):
    if p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "villa" in p_type_string.lower() or "duplex" in p_type_string.lower() or "triplex" in p_type_string.lower() or "unit" in p_type_string.lower() or "maison" in p_type_string.lower()):
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    else:
        return None    