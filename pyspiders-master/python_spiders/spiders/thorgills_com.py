# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

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
    name = 'thorgills_com'
    execution_type = 'testing'
    country = 'united_kingdom' 
    locale = 'en'
    def start_requests(self):

        url="https://www.thorgills.com/search.ljson?channel=lettings&fragment=page-1"
        yield Request(url, callback=self.parse)



    # 1. FOLLOWING
    def parse(self, response):
        data = json.loads(response.body)
        page = response.meta.get('page', 2)
        for item in data["properties"]:
            follow_url = response.urljoin(item["property_url"])
            yield Request(follow_url, callback=self.populate_item,meta={"item":item})
        
        if data["pagination"]["has_next_page"]:
            base_url = f"https://www.thorgills.com/search.ljson?channel=lettings&fragment=page-{page}"
            yield Request(
                base_url,
                callback=self.parse,
                meta={"page":page+1}) 
        
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item = response.meta.get("item")
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Thorgills_PySpider_united_kingdom")    
        item_loader.add_xpath("title", "//title/text()")

        description = " ".join(response.xpath("//div[@class='content page-content']/p//text()").getall())
        item_loader.add_value("description", re.sub("\s{2,}", " ", description))
         
        if get_p_type_string(description):
            item_loader.add_value("property_type", get_p_type_string(description))
        else:
            return

        item_loader.add_value("room_count", item["bedrooms"])
        item_loader.add_value("bathroom_count", item["bathrooms"])
        item_loader.add_value("latitude", str(item["lat"]))
        item_loader.add_value("longitude", str(item["lng"]))
        item_loader.add_value("rent_string", item["price"])
        ext_id = response.xpath("//div/text()[contains(.,'Property Ref')]").extract_first()
        if ext_id:
            item_loader.add_value("external_id", ext_id.split(":")[-1].strip())
    
        
        address = item["display_address"]
        if address: 
            item_loader.add_value("address", address.strip())
            city = address.split(",")[-1].strip()
            if city:
                item_loader.add_value("city", city.strip())
            citycheck=item_loader.get_output_value("city")
            if not citycheck:
                city=address.split(",")[-2].strip()
                if city:
                    item_loader.add_value("city",city)
        

        images = [response.urljoin(x) for x in response.xpath("//div[@id='slider']//li/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)

        floor_plan_images = [ response.urljoin(x) for x in response.xpath("//div[@id='floorplan-area']//img/@src").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)
    
        parking = response.xpath("//ul[@class='property-features']/li[contains(.,'Garage') or contains(.,'Parking') ]/text()").get()
        if parking:
            item_loader.add_value("parking", True)
        
        balcony = response.xpath("//ul[@class='property-features']/li[contains(.,'Balcony')]/text()").get()
        if balcony:
            item_loader.add_value("balcony", True)

        terrace = response.xpath("//ul[@class='property-features']/li[contains(.,'Terrace')]/text()").get()
        if terrace:
            item_loader.add_value("terrace", True)
      
        item_loader.add_value("landlord_name", "Thorgills")
        item_loader.add_value("landlord_email", "brentford@thorgills.com")
        item_loader.add_value("landlord_phone", "020 89949886")
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