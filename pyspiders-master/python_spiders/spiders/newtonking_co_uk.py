# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from python_spiders.helper import ItemClear
import re

class MySpider(Spider):
    name = 'newtonking_co_uk'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.newtonking.co.uk/search/{}.html?instruction_type=Letting&property_type=Apartment&address_keyword=",
                    "https://www.newtonking.co.uk/search/{}.html?instruction_type=Letting&property_type=Maisonette&address_keyword=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.newtonking.co.uk/search/{}.html?instruction_type=Letting&property_type=Bungalow&address_keyword=",
                    "https://www.newtonking.co.uk/search/{}.html?instruction_type=Letting&property_type=Detached&address_keyword=",
                    "https://www.newtonking.co.uk/search/{}.html?instruction_type=Letting&property_type=Semi-Detached&address_keyword=",
                    "https://www.newtonking.co.uk/search/{}.html?instruction_type=Letting&property_type=Terraced&address_keyword=",
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item.format(1),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'), "base_url":item})


    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//div[@class='thumb-box']"):
            follow_url = response.urljoin(item.xpath(".//a[@class='thumbnail']/@href").get())
            room_count = item.xpath(".//h4[contains(.,'Bedroom')]/text()").get()
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"], "room_count":room_count})
            seen = True

        if page == 2 or seen:
            base_url = response.meta["base_url"]
            p_url = base_url.format(page)
            yield Request(
                p_url,
                callback=self.parse,
                meta={"page":page+1, "property_type":response.meta["property_type"], "base_url":base_url})   
        
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Newtonking_Co_PySpider_united_kingdom")
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split("?")[0])
        item_loader.add_value("external_id", response.url.split("property-details/")[1].split("/")[0])

        prp_type = response.xpath("//div[@id='moreInfo']//text()[contains(.,'commercial premises/offices located')]").get()
        if prp_type:
            return
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//h1//text()", input_type="M_XPATH", replace_list={"\n":"", "\t":""})
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//h1/span[@itemprop='name']/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//h1/span[@itemprop='name']/text()", input_type="F_XPATH", split_list={",":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//h1//span[@itemprop='price']/text()",get_num=True, input_type="M_XPATH", split_list={"PCM":0}, replace_list={",":""})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="GBP", input_type="VALUE")
        
        room_count = response.meta.get("room_count").split(" ")[0]
        if room_count and room_count != "0":
            item_loader.add_value("room_count", room_count)
        
        desc = " ".join(response.xpath("//div[@id='moreInfo']//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        if "sq. m" in desc:
            square_meters = desc.split("sq. m")[0].split("/")[-1].strip()
            item_loader.add_value("square_meters", square_meters)
        
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@class='carousel-inner']//img/@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="floor_plan_images", input_value="//div[@id='floorplanModal']//img/@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'googlemap')]/text()", input_type="M_XPATH", split_list={"&q=":1, "%2C":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'googlemap')]/text()", input_type="M_XPATH", split_list={"%2C":1, '"':0})
        
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="NEWTON KING", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="//h2/strong/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="taunton@newtonking.co.uk", input_type="VALUE")
        
                
        yield item_loader.load_item()