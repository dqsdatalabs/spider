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
    name = 'grandapartments_nl'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'en'
    
    start_urls = ["https://grandapartments.nl/grand/nl/search"]

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='photo_block']//a"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item)
            
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        f_text = "".join(response.xpath("//div[.='Description']/following-sibling::p//text()").getall())
        if get_p_type_string(f_text):
            item_loader.add_value("property_type", get_p_type_string(f_text))
        else:
            return
        item_loader.add_value("external_source", "Grandapartments_PySpider_netherlands")        
        item_loader.add_xpath("title", "//title//text()")
     
        city = response.xpath("//ul[contains(@class,'list-unstyled')]/li[@style='font-weight:bold;']//text()").extract_first() 
        if city: 
            item_loader.add_value("city",city) 

        address =response.xpath("//title//text()").extract_first()
        if address:  
            if city:
                address = address.strip()+", "+city.strip()
            item_loader.add_value("address",address.strip() ) 
               
        bathroom_count = response.xpath("//ul[contains(@class,'list-unstyled')]/li[contains(.,'Bathroom')]//text()").extract_first() 
        if bathroom_count: 
            item_loader.add_value("bathroom_count",bathroom_count.split(":")[1]) 
        room_count = response.xpath("//ul[contains(@class,'list-unstyled')]/li[contains(.,'Bedroom')]//text()").extract_first() 
        if room_count: 
            item_loader.add_value("room_count",room_count.split(":")[1]) 
  
        rent =" ".join(response.xpath("//ul[contains(@class,'list-unstyled')]/li[contains(@class,'price')]//text()").extract() )
        if rent: 
            item_loader.add_value("rent_string",rent)   
       
        available_date = response.xpath("//ul[contains(@class,'list-unstyled')]/li[contains(.,'Available per')]/span[2]/text()").extract_first() 
        if available_date:     
            date_parsed = dateparser.parse(available_date.replace(":","").strip(),date_formats=["%d-%m-%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
      
        square =" ".join(response.xpath("//ul[contains(@class,'list-unstyled')]/li[contains(.,'m2')]//text()").extract())
        if square:
            square_meters =  square.split("m")[0].strip()
            if square_meters != '0' :
                item_loader.add_value("square_meters", square_meters) 
        floor =" ".join(response.xpath("//ul[contains(@class,'list-unstyled')]/li[contains(.,'floor')]//text()").extract())
        if floor:
            floor =  floor.split("floor")[0].strip()
            item_loader.add_value("floor", floor) 
        furnished =response.xpath("//ul[contains(@class,'list-unstyled')]/li[.='Furnished' or .='furnished']//text()").extract_first()    
        if furnished:
            if "unfurnished" in furnished.lower():
                item_loader.add_value("furnished", False)
            elif "furnished" in furnished.lower():
                item_loader.add_value("furnished", True)
        washing_machine =response.xpath("//ul[contains(@class,'apartment-description-ul')]/li[contains(.,'Wasmachine')]//text()").extract_first()    
        if washing_machine:
            item_loader.add_value("washing_machine", True)    
        balcony =response.xpath("//ul[contains(@class,'apartment-description-ul')]/li[contains(.,'Balkon')]//text()").extract_first()    
        if balcony:
            item_loader.add_value("balcony", True)  
        parking =response.xpath("//ul[contains(@class,'apartment-description-ul')]/li[contains(.,'parkeren')]//text()").extract_first()    
        if parking:
            item_loader.add_value("parking", True) 
             
        script_map = response.xpath("//script[@type='application/ld+json']/text()[contains(.,'latitude')]").get()
        if script_map:
            item_loader.add_value("latitude", script_map.split('"latitude":"')[1].split('"')[0].strip())
            item_loader.add_value("longitude", script_map.split('"longitude":"')[1].split('"')[0].strip())

        desc = " ".join(response.xpath("//div[div[contains(.,'Description')]]/p//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
              
        images = [response.urljoin(x) for x in response.xpath("//div[@class='fotorama']/a/img/@src").extract()]
        if images:
            item_loader.add_value("images", images)

        item_loader.add_value("landlord_name", "Grand Apartments Amsterdam")
        item_loader.add_value("landlord_phone", "+31 6 52 04 70 74")
        item_loader.add_value("landlord_email", "info@grandapartments.nl")  
        
        yield item_loader.load_item()


def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "appartement" in p_type_string.lower() or "bovenwoning" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "cottage" in p_type_string.lower() or "terrace" in p_type_string.lower()):
        return "house"
    else:
        return None