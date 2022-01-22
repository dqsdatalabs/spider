# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json 

class MySpider(Spider):
    name = 'cloud9estates_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    thousand_separator = ','
    scale_separator = '.'
    def start_requests(self):
        start_url = "https://www.cloud9estates.co.uk/search.ljson?channel=lettings&fragment="
        yield Request(start_url, callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):
        data = json.loads(response.body)
        page = response.meta.get('page', 2)
        for item in data["properties"]:
            url = response.urljoin(item["property_url"])
            status = item["status"]
            if status == "Let agreed":
                continue
            yield Request(url, callback=self.populate_item, meta={"item": item})
        
        next_button = data["pagination"]["has_next_page"]
        if next_button: 
            url = f"https://www.cloud9estates.co.uk/search.ljson?channel=lettings&fragment=page-{page}"
            yield Request(url, callback=self.parse, meta={"page": page+1})
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item = response.meta.get('item')
        # address = item["display_address"]
        bathrooms = item["bathrooms"]
        bedrooms = item["bedrooms"]
        lat = str(item["lat"])
        lng = str(item["lng"])
        property_id = str(item["property_id"])
        property_type = item["property_type"]
        price = item["price"]
        agency_name = item["agency_name"]
        item_loader.add_xpath("title", "//title/text()")
        
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", property_id)
        address = response.xpath("//h1/text()").get()
        if address:
            item_loader.add_value("address", address)
            item_loader.add_value("city", address.split(",")[-2].strip())
            zipcode = address.split(",")[-1].strip()
        
            item_loader.add_value("zipcode", zipcode)
        item_loader.add_value("bathroom_count", bathrooms)
        item_loader.add_value("room_count", bedrooms)
        item_loader.add_value("rent_string", price)
        item_loader.add_value("longitude", lat)
        item_loader.add_value("latitude", lng)
        
        f_text = property_type
        if get_p_type_string(f_text):
            item_loader.add_value("property_type", get_p_type_string(f_text))
        else:
            return
        
        item_loader.add_value("external_source", "Cloud9estates_PySpider_united_kingdom")  
        
        description = " ".join(response.xpath("//div[@class='property--content']//p//text()[normalize-space()]").getall())
        if description:
            item_loader.add_value("description", description.strip())
        
      
        images = [response.urljoin(x) for x in response.xpath("//div[@id='royalSlider']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)

        item_loader.add_value("landlord_name", "Cloud9 Estate Agents")
        landlord_phone = response.xpath("//p[@class='branch--phone']/a/text()").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone)

        furnished = response.xpath("//li[contains(.,'Furnished') or contains(.,'Furnished')]/text()").get()
        if furnished:
            if "unfurnished" in furnished.lower():
                item_loader.add_value("furnished", False)  
            else:
                item_loader.add_value("furnished", True)
        furnishedcheck=item_loader.get_output_value("furnished")
        if not furnishedcheck:
            if "furnished" in description.lower():
                item_loader.add_value("furnished",True)

        parking = response.xpath("//li[contains(.,'parking') or contains(.,'Parking')]/text()").get()
        if parking:
            item_loader.add_value("parking", True) 
    

        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "appartement" in p_type_string.lower() or "bovenwoning" in p_type_string.lower() or "flat" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "cottage" in p_type_string.lower() or "terrace" in p_type_string.lower()):
        return "house"    
    else:
        return None