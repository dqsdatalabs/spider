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
    name = '1_4_sale_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    thousand_separator = ','
    scale_separator = '.' 
    start_urls = ['https://www.1-4-sale.co.uk/properties-to-rent']  # LEVEL 1

    # 1. FOLLOWING
    def parse(self, response):        
        data = "["+",".join(response.xpath("//property[@small='true']/@raw-data").getall())+"]"
        json_value = json.loads(data)
        for item in json_value:
            url = item["propertyUri"]
            status = item["avilabilityLabel"]
            address = item["displayAddress"]
            bedroom = item["bedrooms"]
            bathroom = item["bathrooms"]
            if status and "let agreed" in status.lower():
                continue                   
            yield Request(url, callback=self.populate_item,meta={"address":address,"bedroom":bedroom,"bathroom":bathroom})     

        next_page = response.xpath("//li[@class='page-item']/a[@rel='next']/@href").get()
        if next_page:
            yield Request(next_page, callback=self.parse)     


    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("/")[-1])
        description = " ".join(response.xpath("//div[@id='propertySummary']/text()").getall())
        if get_p_type_string(description):
            item_loader.add_value("property_type", get_p_type_string(description))
        else:
            print(response.url)
            return
        item_loader.add_value("external_source", "14Sale_Co_PySpider_united_kingdom")
        
        item_loader.add_xpath("title", "//h1/text()")
        address = response.meta.get("address")
        if address:
            item_loader.add_value("address", address)
            item_loader.add_value("city", address.split(",")[-1])
        item_loader.add_value("room_count", str(response.meta.get("bedroom")))
        item_loader.add_value("bathroom_count", str(response.meta.get("bathroom")))
        item_loader.add_xpath("rent_string", "//div[@class='col-lg-8']//span[@class='float-right p-2']//text()")
    
        desc = " ".join(response.xpath("//div[@id='propertySummary']/text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())
 
        parking = response.xpath("//li[contains(.,'Parking') or contains(.,'parking')]/text()").get()
        if parking:
            item_loader.add_value("parking", True)
        furnished = response.xpath("//li[contains(.,'furnished') or contains(.,'Furnished')]/text()").get()
        if furnished:
            if "unfurnished" in furnished.lower():
                item_loader.add_value("furnished", False)
            elif "furnished" in furnished.lower():
                item_loader.add_value("furnished", True)

        images = [x for x in response.xpath("//div[@class='carousel-inner']/div//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)        
      
        item_loader.add_value("landlord_name", "1-4-Sale")
        item_loader.add_value("landlord_phone", "0141 550 8888")

        zipcode = response.xpath('/html/head//comment()').get().split(',')[-1].split('|')[0].strip()
        if zipcode:
            item_loader.add_value('zipcode', zipcode)

        if 'office' not in desc.lower() and 'business' not in desc.lower():
            yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower() or "unit" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "home" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "cottage" in p_type_string.lower() or "detached" in p_type_string.lower()):
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    else:
        return None