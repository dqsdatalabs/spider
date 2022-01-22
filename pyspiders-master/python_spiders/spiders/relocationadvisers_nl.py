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
from datetime import datetime
class MySpider(Spider):
    name = 'relocationadvisers_nl'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'
    start_urls = ["https://www.relocationadvisers.nl/aanbod/"]

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//div[@class='image-text-box']/a"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 2 or seen:
            p_url = f"https://www.relocationadvisers.nl/aanbod/?ppage={page}"
            yield Request(
                p_url,
                callback=self.parse,
                meta={"page":page+1})
        
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)

        f_text = "".join(response.xpath("//div[@class='objectDetailsInfo']/p/text()").getall())
        if get_p_type_string(f_text):
            item_loader.add_value("property_type", get_p_type_string(f_text))
        else:
            return
        item_loader.add_value("external_source", "Relocationadvisers_PySpider_netherlands")        
        item_loader.add_xpath("title", "//h1[1]/text()")
        item_loader.add_xpath("external_id", "substring-after(//div[@class='objectDetailsInfo']/p//text()[contains(.,'Object')],': ')")

        address =response.xpath("//h1[1]/text()").extract_first()
        if address:  
            item_loader.add_value("address",address.strip() ) 
   
        room_count = response.xpath("//div[@class='objectDetailsInfo']/p//text()[contains(.,'Slaapkamer')]").extract_first() 
        if room_count: 
            item_loader.add_value("room_count",room_count.strip().split(" ")[0]) 
  
        rent = response.xpath("//h1[@class='price']/text()").extract_first()
        if rent: 
            item_loader.add_value("rent_string",rent)   

        washing_machine =response.xpath("//ul/li[contains(.,'Wasmachine')]//text()").extract_first()    
        if washing_machine:
            item_loader.add_value("washing_machine", True)    
        terrace =response.xpath("//ul/li[contains(.,'Terras') or contains(.,'terras') ]//text()").extract_first()    
        if terrace:
            item_loader.add_value("terrace", True)  
        balcony =response.xpath("//ul/li[contains(.,'Balkon')]//text()").extract_first()    
        if balcony:
            item_loader.add_value("balcony", True)  
        parking =response.xpath("//ul/li[contains(.,'Garage') or contains(.,'parkeerplaa') ]//text()").extract_first()    
        if parking:
            item_loader.add_value("parking", True) 
  
        available_date = response.xpath(" substring-after(//div[@class='objectDetailsInfo']/p//text()[contains(.,'Beschikbaar pe')],': ')").extract_first() 
        if available_date:     
            if "direct" in available_date.lower():
                item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
            else:
                date_parsed = dateparser.parse(available_date.replace("Vanaf","").strip(),date_formats=["%d-%m-%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
        furnished =response.xpath("//div[@class='objectDetailsInfo']/p//text()[contains(.,'Gemeubileerd')]").extract_first()    
        if furnished:
            item_loader.add_value("furnished", True)
        furnished =response.xpath("//div[@class='objectDetailsInfo']/p//text()[contains(.,'Gestoffeerd')]").extract_first()    
        if furnished:
            item_loader.add_value("furnished", False)
    
        desc = " ".join(response.xpath("//div[@class='objectDetailsInfo']/p[1]//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
            if "approximately" in desc.lower():
                unit_pattern = re.findall(r"[+-]? *((?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?)\s*(m²|meters2|metres2|meter2|metre2|mt2|m2|M2)",desc.lower().replace("(","").replace(")","").split("approximately")[1].replace(",","."))
                if unit_pattern:
                    sq=int(float(unit_pattern[0][0]))
                    item_loader.add_value("square_meters", str(sq))
              
        images = [response.urljoin(x) for x in response.xpath("//div[contains(@class,'object_photos')]/a/img/@src").extract()]
        if images:
            item_loader.add_value("images", images)

        item_loader.add_value("landlord_name", "Relocation Advisers BV")
        item_loader.add_value("landlord_phone", "+31 (0)206647470")
        item_loader.add_value("landlord_email", "info@relocationadvisers.nl")  

        city_script = response.xpath("//script[contains(.,'getLatLong')]//text()").get()
        if city_script:
            item_loader.add_value("city", city_script.split("getLatLong(")[-1].split(");")[0].split(",")[1].replace("'","").strip())

        latitude_longitude = response.xpath("//a//@href[contains(.,'maps.google')]").get()
        if latitude_longitude:
            lat = latitude_longitude.split("?ll=")[1].split(",")[0]
            lng = latitude_longitude.split("?ll=")[1].split(",")[1].split("&")[0]
            item_loader.add_value("latitude", lat)
            item_loader.add_value("longitude", lng)

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