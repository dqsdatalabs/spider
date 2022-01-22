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
from word2number import w2n
class MySpider(Spider):
    name = 'alex_matteo_com'    
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    start_urls = ["https://www.alex-matteo.com/properties-for-rent"]
    custom_settings = {
        "HTTPCACHE_ENABLED": False,
        "HTTPERROR_ALLOWED_CODES": [301,302]
    }

    LET_LIST = []
    # 1. FOLLOWING
    def parse(self, response):

        str_body = str(response.body)
        for i in str_body.split("LET"):
            if i != "":
                self.LET_LIST.append(i.split("href")[1].split("target")[0].replace("\"","").strip("=").strip())

        for item in response.xpath("//a[contains(@class,'card')]/@href").extract():
            #follow_url = response.urljoin(item.xpath(".//@href").get())
            yield Request(item, callback=self.populate_item)
        
        next_page_url = response.xpath('//li[@class="next arrow"]/a/@href').get()
        if next_page_url:
            yield Request(
                url=next_page_url, 
                callback=self.parse, 
                )
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        # for i in self.LET_LIST:
        #     if i == response.url:
        #         return
        
        item_loader.add_value("external_link", response.url)

        property_type = " ".join(response.xpath("//li[contains(.,'Type')]//text()").getall()).strip()
        if get_p_type_string(property_type): item_loader.add_value("property_type", get_p_type_string(property_type))
        else: return
              
        item_loader.add_value("external_source", "Alexmatteo_PySpider_"+ self.country + "_" + self.locale)
        title = response.xpath("//div/h1/text()").get()   
        if title:   
            item_loader.add_value("title",title.strip()) 
            item_loader.add_value("address",title.strip()) 
            if len(title.split(","))>2:
                item_loader.add_value("zipcode",title.split(",")[-1].strip()) 
        item_loader.add_value("city","London") 
  
        rent = response.xpath("//div/p/text()[contains(.,'£')]").extract_first()
        if rent:
            item_loader.add_value("rent_string", rent.replace(",","."))
        
        room = response.xpath("//li[contains(.,'Bedroom')]//text()").re(r'\d')
        if room:
            item_loader.add_value('room_count', room[0])
                           
        bathroom = response.xpath("//li[contains(.,'Bath')]/text()").re(r'\d')
        if bathroom:     
            item_loader.add_value("bathroom_count", bathroom[0])

        floor = response.xpath("//li[contains(.,'Floor') and not(contains(.,'Wooden') )]//text()").extract_first()
        if floor:
            item_loader.add_value("floor", floor.split("Floor")[0])
        
        swimming_pool = response.xpath("//li[contains(.,'Swimming Pool')]//text()").extract_first()
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)

        balcony = response.xpath("//li[contains(.,'Balcon')]//text()").extract_first()
        if balcony:
            item_loader.add_value("balcony", True)
        
        terrace = response.xpath("//li[contains(.,'Terrace')]//text()").extract_first()
        if terrace:
            item_loader.add_value("terrace", True)
        
        energy = response.xpath("//li[contains(.,'EPC')]//text()").re(r'EPC Rating (.)')
        if energy:                        
                item_loader.add_value("energy_label", energy[0])

        parking = response.xpath("//li[contains(.,'Parking') or contains(.,'parking')]//text()").extract_first()
        if parking:
            item_loader.add_value("parking", True)

        desc = "".join(response.xpath("//div[@class='content-area']/p//text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())

            if "utility bills:" in desc.lower():
                utilities = desc.lower().split("utility bills:")[1].strip().split(" ")[0].split(".")[0].replace("£","")
                item_loader.add_value("utilities", utilities)
 
        images = response.xpath('//ul[@class="image-carousel__items"]//div[@class="image-carousel__item"]/@style').re(r'url\((.*)\)')
        if images:
            item_loader.add_value("images", images)      

        item_loader.add_value("landlord_phone", "0203 9831 833")
        item_loader.add_value("landlord_email", "hello@alex-matteo.com")
        item_loader.add_value("landlord_name", "Alex & Matteo")

        location = response.xpath("//iframe/@src").re_first(r"q=(.*)&zoom")
        lat, log = location.split(',')
        if lat and log:
            item_loader.add_value('latitude', lat)
            item_loader.add_value('longitude', log)
        status = response.xpath('//p[@class="color-grey label label--md margin-bottom-1"]/text()').get()
        if status == 'Let' or status == 'Let Agreed':
            return
    
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "local" in p_type_string.lower():
        return None
    elif p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("studio" in p_type_string.lower()):
        return "studio"
    elif p_type_string and ("appartement" in p_type_string.lower() or "flat" in p_type_string.lower() ):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "villa" in p_type_string.lower()):
        return "house"
    elif p_type_string and "chambre" in p_type_string.lower():
        return "room"   
    else:
        return None
