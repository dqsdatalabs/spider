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

class MySpider(Spider):
    name = 'riseestateagents_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    thousand_separator = ','
    scale_separator = '.' 
    start_urls = ["https://www.riseestateagents.co.uk/search.ljson?channel=lettings&fragment="]

    # 1. FOLLOWING
    def parse(self, response):
        data = json.loads(response.body)
        
        for item in data["properties"]:
            if "status" in item and item["status"].lower() not in ["let agreed"]:
                follow_url = response.urljoin(item["property_url"])
                yield Request(follow_url, callback=self.populate_item)
        
        next_page = data["pagination"]["has_next_page"]
        if next_page:
            page = data["pagination"]["current_page"] + 1
            p_url = f"https://www.riseestateagents.co.uk/search.ljson?channel=lettings&fragment=page-{page}"
            yield Request(p_url, callback=self.parse)
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("external_link", response.url)

        desc = "".join(response.xpath("//div[contains(@class,'content page-content')]//text()").getall())
        if desc and ("apartment" in desc.lower() or "flat" in desc.lower() or "maisonette" in desc.lower()):
            item_loader.add_value("property_type", "apartment")
        elif desc and "house" in desc.lower():
             item_loader.add_value("property_type", "house")
        elif desc and "studio" in desc.lower():
             item_loader.add_value("property_type", "studio")
        elif desc and "student" in desc.lower():
             item_loader.add_value("property_type", "student_apartment")
        else:
            return
        item_loader.add_value("external_source", "Riseestateagents_Co_PySpider_united_kingdom")
        
        title = response.xpath("//div/h1/text()").extract_first()
        if title:
            if "Enterprise House" in title:
                return
            item_loader.add_value("title", title.strip())
            item_loader.add_value("address", title.strip())
            if "," in title:
                item_loader.add_value("city", title.split(",")[-1].strip())
            else:
                if response.xpath("//ul[@class='property-transport']/li[1]/text()").get():
                    city = response.xpath("//ul[@class='property-transport']/li[1]/text()").get()
                    item_loader.add_value("city", city.split(" ")[0])

        ext_id = response.xpath("substring-after(//div[contains(@class,'page-content')]//text()[contains(.,'Property Ref')],':')").extract_first()
        if ext_id:
            item_loader.add_value("external_id", ext_id.strip())
  
        room_count = response.xpath("//li[@class='property-bedrooms']/span/text()[normalize-space()]").get()
        if room_count:
            item_loader.add_value("room_count", room_count)     
        
        bathroom_count=response.xpath("//li[@class='property-bathrooms']/span/text()[normalize-space()]").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)

        rent = response.xpath("//div/span[@class='property-price']//text()").extract_first()
        if rent:
            if "pw" in rent.lower():
                rent_week = rent.split("pw")[0].split("£")[1]
                rent = int(rent_week.replace(",",""))*4
                item_loader.add_value("rent", rent)    
                item_loader.add_value("currency", "GBP")    
            else:
                item_loader.add_value("rent_string", rent)    
     
        desc = " ".join(response.xpath("//div[contains(@class,'page-content')]/p[not(contains(.,'Property Ref'))]//text()").extract())
        if desc:
            item_loader.add_value("description",desc.strip())      
       
        images = [response.urljoin(x) for x in response.xpath("//div[@id='slider']/ul/li/img/@src").extract()]
        if images:
            item_loader.add_value("images", images)   

        map_coordinate = response.xpath("//script/text()[contains(.,'new google.maps.LatLng(')]").extract_first()
        if map_coordinate:
            map_coordinate = map_coordinate.split("LatLng(")[1].split(")")[0].strip()
            latitude = map_coordinate.split(",")[0].strip()
            longitude = map_coordinate.split(",")[1].strip()
            if latitude and longitude:
                item_loader.add_value("longitude", longitude)
                item_loader.add_value("latitude", latitude)

        furnished = response.xpath("//ul[@class='property-features']/li/text()[contains(.,'furnished') or contains(.,'Furnished') or contains(.,'UNFURNISHED')]").get()
        if furnished:
            if "unfurnished" in furnished.lower():
                item_loader.add_value("furnished", False)  
            elif "furnished" in furnished.lower():
                item_loader.add_value("furnished", True) 
        parking = response.xpath("//ul[@class='property-features']/li/text()[contains(.,'parking') or contains(.,'Parking') or contains(.,'garage')]").get()
        if parking:
            item_loader.add_value("parking", True)  

        elevator = response.xpath("//ul[@class='property-features']/li/text()[contains(.,'Lift') or contains(.,'lift')]").get()
        if elevator:
            item_loader.add_value("elevator", True)  
                     
        available_date = response.xpath("//ul[@class='property-features']/li/text()[contains(.,'Available')]").get()
        if available_date:
            try:
                date_parsed = dateparser.parse(available_date.split("Available")[1], languages=['en'])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)      
            except:
                pass

        item_loader.add_value("landlord_phone", "0191 386 5349")
        item_loader.add_value("landlord_email", "info@riseestateagents.co.uk")
        item_loader.add_value("landlord_name", "Rise Estate Agents")  
        

        yield item_loader.load_item()
