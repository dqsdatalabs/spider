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
    name = 'leaderproperties_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    external_source='Leaderproperties_PySpider_australia'
    custom_settings = {
    "HTTPCACHE_ENABLED": False
    }
    def start_requests(self):
        start_urls = [
            {"url": "https://www.leaderproperties.com.au/renting/properties-for-lease/?property_type%5B%5D=Apartment&min_price=&max_price=&bedrooms=&bathrooms=&carspaces=#apsp","prop_type":"apartment"},
            {"url": "https://www.leaderproperties.com.au/renting/properties-for-lease/?property_type%5B%5D=House&min_price=&max_price=&bedrooms=&bathrooms=&carspaces=","prop_type":"house"}

        ]  # LEVEL 1       
        for url in start_urls:
            yield Request(url=url.get('url'),callback=self.parse,meta={"prop_type":url.get("prop_type")})
    # 1. FOLLOWING


    def parse(self, response):



        for item in response.xpath("//div[@class='listing-item position-relative px-3']//a[not(@title)]/@href").getall():
            follow_url = item
            yield Request(follow_url,callback=self.populate_item,meta={"prop_type":response.meta.get("prop_type")})


        follow_page = response.xpath("//a[@class='next page-numbers']/@href").get()
        if  follow_page:

            yield Request(follow_page, callback=self.parse,meta={'prop_type':response.meta.get("prop_type")})
            

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("property_type",response.meta.get("prop_type"))

        title = response.xpath("//div[@class='suburb-address']/text()").get()
        if title:
            item_loader.add_value("title",title)
            item_loader.add_value("address",title)
            city = title.split(",")[-1]
            item_loader.add_value("city",city)

        price = response.xpath("//div[@class='suburb-price']/text()").get()
        if price:
            price = re.search("\d+",price)
            if price:
                price = price[0]
                item_loader.add_value("rent",4*int(price))
        
        id = response.xpath("//label[text()='Property ID']/following-sibling::div/text()").get()
        if id:
            item_loader.add_value("external_id",id)
        
        room = response.xpath("//label[text()='Bedrooms']/following-sibling::div/text()").get()
        if room:
            item_loader.add_value("room_count",room)
        roomcount=item_loader.get_output_value("room_count")
        if not roomcount:
            room=response.xpath("//h5[@class='sub-title']/text()").get()
            if room and "studio" in room.lower():
                item_loader.add_value("room_count","1")
            elif room and "bedroom" in room.lower():
                room=room.split("Bedroom")[0].strip().split(" ")[1]
                if room and "One" in room:
                    item_loader.add_value("room_count","1")



        bath = response.xpath("//label[text()='Bathrooms']/following-sibling::div/text()").get()
        if bath:
            item_loader.add_value("bathroom_count",bath)  

        garage = response.xpath("//label[text()='Garage' or text()='Car Spaces']/following-sibling::div/text()").get()
        if garage:
            item_loader.add_value("parking",True)  

        images = response.xpath("//a[@class='col']/@href").getall()
        if images:
            item_loader.add_value("images",images)


        available = response.xpath("//label[text()='Available From']/following-sibling::div/text()").get()
        if available:
            if available.lower() != 'now':
                available = dateparser.parse(available,date_formats=["%d-%m-%Y"]).strftime("%d-%m-%Y")

                item_loader.add_value("available_date",available) 

        desc =" ".join([txt.strip() for txt in response.xpath("//div[@class='detail-description']//text()").getall()])
        if desc:
            item_loader.add_value("description",desc)

        if "furnished" in desc.lower():
            item_loader.add_value("furnished",True)

        if "balcony" in desc.lower():
            item_loader.add_value("balcony",True)


        item_loader.add_value("landlord_name","Xiang (john) Zhou")
        item_loader.add_value("landlord_phone","02 9745 5038")
        item_loader.add_value("landlord_email","johnz@leaderproperties.com.au")
        item_loader.add_value("currency","USD")



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