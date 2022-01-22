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
    name = 'indestad_nl' 
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'
    start_urls = ["https://www.indestad.nl/huurwoningen/"]

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//a[contains(@class,'button primary')]/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
        

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)

        f_text = "".join(response.xpath("//div[contains(@class,'omschrijving')]/p/text()").getall())
        if get_p_type_string(f_text):
            item_loader.add_value("property_type", get_p_type_string(f_text))
        else:
            return
        item_loader.add_value("external_source", "Indestad_PySpider_netherlands")
        item_loader.add_xpath("title", "//div/h1/text()") 
        id=response.xpath("//link[@rel='shortlink']/@href").get()
        if id:
            id=id.split("=")[-1]
            item_loader.add_value("external_id",id) 
        city =response.xpath("//li[span[contains(.,'Plaats')]]/span[@class='value']//text()").extract_first()
        if city:     
            item_loader.add_value("city", city.strip())   
        else:
            city =response.xpath("//span[@class='woning-beschrijving']/text()[1]").extract_first()
            if city:     
                item_loader.add_value("city", city.replace("\t","").strip())  

        zipcode =response.xpath("//li[span[contains(.,'Postcode')]]/span[@class='value']//text()").extract_first()
        if zipcode:     
            item_loader.add_value("zipcode", zipcode.strip()) 

        address =", ".join(response.xpath("//li[contains(@class,'wpp_stat_plain_list_buurt')]/span[@class='value']//text() | //li[contains(@class,'wpp_stat_plain_list_straat_')]/span[@class='value']//text()").extract()) 
        if address:
            if city:
                address = address +", "+ city.strip()
            item_loader.add_value("address",address.strip() ) 
        else:
            address =" ".join(response.xpath("//span[@class='woning-beschrijving']/text()[1] | //span[@class='woning-beschrijving']/strong//text()[1]").extract()) 
            if address:                
                address = address.replace("\t","").strip()
                item_loader.add_value("address",address.strip() ) 

        room_count =response.xpath("//li[span[contains(.,'slaapkamer')]]/span[@class='value']//text()").extract_first()
        if room_count:     
            if "(" in room_count:
                room_count = room_count.split("(")[0]
            item_loader.add_value("room_count", room_count)  
        elif "studio" in get_p_type_string(f_text):
            item_loader.add_value("room_count", "1") 
        else:
            room = response.xpath("substring-after(substring-after(//span[@class='woning-beschrijving']/text()[contains(.,'kamers')],'('),'- ')").extract_first()
            if room:
                item_loader.add_value("room_count", room.split(" ")[0].strip()) 
        item_loader.add_xpath("bathroom_count", "//li[span[contains(.,'Badkamer')]]/span[@class='value']//text()")
         
        rent =response.xpath("//div/span[@class='price']//text()").extract_first()
        if rent:     
            item_loader.add_value("rent_string", rent)  

        utilities = response.xpath("//div/span[@class='price-sc']//text()[.!='excl.  servicekosten']").extract_first()
        if utilities:     
            item_loader.add_value("utilities", utilities) 

        available_date = response.xpath("//li[span[contains(.,'beschikbaar')]]/span[@class='value']//text()").extract_first() 
        if available_date:
            date_parsed = dateparser.parse(available_date.replace("Per direct beschikbaar","now").strip())
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
    
        square =response.xpath("//li[span[contains(.,'Oppervlakte')]]/span[@class='value']//text()").extract_first()
        if square:
            square_meters =  square.split("m")[0].strip()
            item_loader.add_value("square_meters", square_meters) 
        else: 
            square =response.xpath("//span[@class='woning-beschrijving']//text()[contains(.,'m2') or contains(.,'m²')]").extract_first()
            if square:
                unit_pattern = re.findall(r"[+-]? *((?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?)\s*(m²|meters2|metres2|meter2|metre2|mt2|m2|M2)",square.replace(",","."))
                if unit_pattern:
                    sq=int(float(unit_pattern[0][0]))
                    item_loader.add_value("square_meters", str(sq))


        desc = " ".join(response.xpath("//div[contains(@class,'omschrijving')]/p[contains(.,'Indeling:')]/preceding-sibling::p//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
        else:
            desc = " ".join(response.xpath("//div[contains(@class,'omschrijving')]/p//text()").extract())
            if desc:
                item_loader.add_value("description", desc.strip())

        balcony =response.xpath("//li[span[contains(.,'Balkon')]]/span[@class='value']//text()").extract_first()    
        if balcony:
            item_loader.add_value("balcony", True)    
        terras =response.xpath("//li[span[contains(.,'terras')]]/span[@class='value']//text()").extract_first()    
        if terras:
            item_loader.add_value("terrace", True)  
        furnished =response.xpath("//li[span[contains(.,'interieur')]]/span[@class='value']//text()").extract_first()    
        if furnished:
            if "kaal" in furnished.lower():
                item_loader.add_value("furnished", False)
            elif "gemeubileerd" in furnished.lower():
                item_loader.add_value("furnished", True)
        else:
            furnished = response.xpath("//li[contains(.,'gemeubileerd')]//text()").get()
            if furnished:
                item_loader.add_value("furnished", True)
        parking = response.xpath("//li[contains(.,'parkeren')]//text()").get()
        if parking:
            item_loader.add_value("parking", True)
        images = [response.urljoin(x) for x in response.xpath("//div[@id='slider']/ul/li/img/@src").extract()]
        if images:
                item_loader.add_value("images", images)
        script_map = response.xpath("//script/text()[contains(.,'google.maps.LatLng(')]").get()
        if script_map:
            latlng = script_map.split("google.maps.LatLng(")[1].split(");")[0]
            item_loader.add_value("latitude", latlng.split(",")[0].strip())
            item_loader.add_value("longitude", latlng.split(",")[1].strip())

        item_loader.add_value("landlord_name", "Indestad")
        item_loader.add_value("landlord_phone", "010 20 66 000")
        item_loader.add_value("landlord_email", "info@indestad.nl")
              
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