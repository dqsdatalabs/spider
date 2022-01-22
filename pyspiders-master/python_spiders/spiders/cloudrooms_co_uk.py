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
import dateparser 
from math import *
 
class MySpider(Spider): 
    name = 'cloudrooms_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    thousand_separator = ','
    scale_separator = '.'  
    start_urls = ["https://cloudrooms.co.uk/rental/"]

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//a[@class='overlay-typography']/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
        
        next_page = response.xpath("//span[contains(.,'Older Posts')]/../@href").get()
        if next_page:
            yield Request(response.urljoin(next_page), callback=self.parse)
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("external_link", response.url)
        p_type = response.xpath("//span[@class='epl-property-category']/text()").get()
        if p_type and "house" in p_type.lower():
            item_loader.add_value("property_type", "house")
        elif p_type and "apartment" in p_type.lower():
            item_loader.add_value("property_type", "apartment")
        else:
            item_loader.add_value("property_type", "room")
        
        item_loader.add_value("external_source", "Cloudrooms_Co_PySpider_united_kingdom")
         
        title = response.xpath("//div/h1/text()").extract_first()
        if title:
            item_loader.add_value("title", title.strip())
            item_loader.add_value("address", title.strip())
            zipcode = ""
            zipcode_list = title.split(",")
            for i in zipcode_list:
                if not i.strip().split(" ")[0].isalpha():
                    zipcode = i.strip()
                    break
            print(zipcode.strip())
            
            if zipcode:
                item_loader.add_value("zipcode", zipcode)
            else:
                zipcode_address = response.xpath("//div[@id='epl-default-map']/@data-address").get()
                if zipcode_address:
                    zipcode_list = zipcode_address.split(",")
                    for i in zipcode_list:
                        if not i.strip().split(" ")[0].strip().isalpha():
                            zipcode = i.strip()
                            break
        


        # if not item_loader.get_collected_values("zipcode"):
        #     address = response.xpath("//div[contains(@class,'epl-section-map')]/div/@data-address").extract_first()
        #     if address:            
        #         item_loader.add_value("zipcode", address.split(",")[1].strip())

        city = response.xpath("//div/h3/span[@class='state']/text()").extract_first()
        if city:
            item_loader.add_value("city", city.strip())
  
        room_count = response.xpath("//div[contains(@class,'epl-icon-container-bed')]/div[@class='icon-value']/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)     
        
        bathroom_count=response.xpath("//div[contains(@class,'epl-icon-container-bath')]/div[@class='icon-value']/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
 
        rent = " ".join(response.xpath("//span[@class='page-price-rent']//text()").extract())
        if rent:
            if "week" in rent.lower():
                rent_week = rent.split("/")[0].split("£")[1]
                rent = int(rent_week.replace(",",""))*4
                item_loader.add_value("rent", rent)     
                item_loader.add_value("currency", "GBP")    

            else:
                item_loader.add_value("rent_string", rent)    
        deposit = response.xpath("//span[@class='bond']//text()").extract_first()
        if deposit:
            item_loader.add_value("deposit", deposit)  
       
        desc = " ".join(response.xpath("//div[contains(@class,'epl-section-description')]//p//text()").extract())
        if desc:
            item_loader.add_value("description",desc.strip())      
            if "parking" in desc.lower():
                item_loader.add_value("parking", True)   
    
        images = [response.urljoin(x) for x in response.xpath("//div[@class='post-image']//a/@href").extract()]
        if images:
            item_loader.add_value("images", images)    

        square_meters = response.xpath("//ul/li[@class='land-size']/text()").get()
        if square_meters:
            square_meters=square_meters.split("square")[0].strip().split(" ")[-1]
            item_loader.add_value("square_meters", str(floor(float(square_meters)* 0.09290304))) 
        dishwasher = response.xpath("//ul/li[@class='dishwasher']/text()").get()
        if dishwasher:
            item_loader.add_value("dishwasher", True) 
        balcony = response.xpath("//ul/li[@class='balcony']/text()").get()
        if balcony:
            item_loader.add_value("balcony", True) 
    
        energy = response.xpath("//ul/li[@class='energy_rating']/text()").get()
        if energy:
            item_loader.add_value("energy_label", energy.strip().split(" ")[-1]) 
            
        furnished = response.xpath("//ul/li[contains(@class,'furnished')]/text()").get()
        if furnished:
            if "unfurnished" in furnished.lower():
                item_loader.add_value("furnished", False) 
            elif "furnished" in furnished.lower():
                item_loader.add_value("furnished", True) 

        from datetime import datetime
        available_date = response.xpath("//div[contains(@class,'available')]/text()").get()
        if available_date and "now" in available_date:
            item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
        
        item_loader.add_value("landlord_phone", "+447587093331")
        item_loader.add_value("landlord_email", "info@cloudrooms.co.uk")
        item_loader.add_value("landlord_name", "Cloud Rooms")  
        yield item_loader.load_item()
