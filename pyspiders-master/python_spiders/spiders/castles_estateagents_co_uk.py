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

class MySpider(Spider):
    name = 'castles_estateagents_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en' 
    thousand_separator = ','
    scale_separator = '.'
    start_urls = ["https://www.castles.london/let/property-to-let/"]
    
    custom_setting = {
        "PROXY_ON": "True"
    }
    
    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page",2)
        seen = False
        for item in response.xpath("//div[@class='pstatus' and ./span[.='To Let ']]/../../@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 2 or seen:
            p_url =f"https://www.castles.london/let/property-to-let/page/{page}"
            yield Request(p_url, callback=self.parse, meta={"page":page+1})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)

        if get_p_type_string(response.url):
            item_loader.add_value("property_type", get_p_type_string(response.url))
        else:
            desc = "".join(response.xpath("//h3[.='Property Description']/following-sibling::*/text()").getall())
            if get_p_type_string(desc):
                item_loader.add_value("property_type", get_p_type_string(desc))
            else:
                return
        item_loader.add_value("external_source", "Castles_Estateagents_Co_PySpider_united_kingdom")
         
        item_loader.add_xpath("title", "//div[contains(@class,'content-padding')]//h1//text()[normalize-space()]")
        
        rent = " ".join(response.xpath("//div[contains(@class,'content-padding')]//h2[contains(.,'£')]//text()[normalize-space()]").extract())
        if rent:
            item_loader.add_value("rent_string", rent) 
        
        room_count = response.xpath("//div[img[contains(@src,'bedroom')]]/span/text()").extract_first()
        if room_count:
            item_loader.add_value("room_count", room_count.strip().split(" ")[0])

        address = response.xpath("//div[contains(@class,'content-padding')]//h1//text()[normalize-space()]").extract_first()       
        if address:
            item_loader.add_value("address", address)   
            item_loader.add_value("city", address.split(",")[-2].strip())
            item_loader.add_value("zipcode", address.split(",")[-1].strip())
       
        desc = " ".join(response.xpath("//div[contains(@class,'property-details')]/p/text()[normalize-space()]").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
      
        floor = response.xpath("//div[@class='featureswrapper']/div//text()[contains(.,' floor') or contains(.,' FLOOR') ]").extract_first()
        if floor:
            item_loader.add_value("floor", floor.lower().split("floor")[0].strip().split(" ")[-1])
        bathroom = response.xpath("//div[@class='featureswrapper']/div//text()[contains(.,' bathroom')]").extract_first()
        if bathroom:
            bathroom = bathroom.split("bathroom")[0].strip().split(" ")[0]
            if bathroom.isdigit():
                item_loader.add_value("bathroom_count", bathroom)
        else:
            bathroom_count = "".join(response.xpath("//div[@class='row meta-row meta-row-one']/div//span[contains(.,'BATHROOM')]/text() | //div[@class='featureswrapper']/div//text()[contains(.,'BATHROOM')]").getall())
            if bathroom_count:
                if bathroom_count.strip().lower() == "bathroom":
                    item_loader.add_value("bathroom_count", "1")
                else:
                    item_loader.add_value("bathroom_count", bathroom_count.strip().split(" ")[0])
                    
        parking = response.xpath("//div[@class='featureswrapper']/div//text()[contains(.,'parking') or contains(.,'Parking') or contains(.,'PARKING')]").extract_first()
        if parking:
            item_loader.add_value("parking", True)
            
        terrace = response.xpath("//div[@class='featureswrapper']/div//text()[contains(.,'terrace') or contains(.,'Terrace') ]").extract_first()
        if terrace:
            item_loader.add_value("terrace", True)
            
        balcony = response.xpath("//div[@class='featureswrapper']/div//text()[contains(.,'Balcony') or contains(.,'balcony') or contains(.,'BALCONY') ] | //h3[contains(.,'Features')]/following-sibling::div/div[contains(.,'Balcony')]").extract_first()
        if balcony:
            item_loader.add_value("balcony", True)
            
        elevator = response.xpath("//div[@class='featureswrapper']/div//text()[contains(.,'LIFT') or contains(.,'lift') or contains(.,'Lift') or contains(.,'Elevator')]").extract_first()
        if elevator:
            item_loader.add_value("elevator", True)
            
        furnished = response.xpath("//div[@class='featureswrapper']/div//text()[contains(.,'furnished') or contains(.,'Furnished') or contains(.,'FURNISHED') ]").extract_first()
        if furnished:
            if "unfurnished" in furnished.lower():
                item_loader.add_value("furnished", False)
            else:
                item_loader.add_value("furnished", True)
        
        available_date = response.xpath("//div[@class='featureswrapper']/div//text()[contains(.,'AVAILABLE') or contains(.,' Available')]").extract_first()
        if available_date:
            date_parsed = dateparser.parse(available_date.lower().split("available")[1], languages=['en'])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
     
        script_map = response.xpath("//script[contains(.,'initMap([') and contains(.,'lng')]/text()").get()
        if script_map:
            latlng = script_map.split("initMap([")[1].split("]")[0]
            item_loader.add_value("latitude", latlng.split('"lat": "')[1].split('"')[0].strip())
            item_loader.add_value("longitude", latlng.split('"lng": "')[1].split('"')[0].strip())
          
        images = [x for x in response.xpath("//div[contains(@class,'imageviewerPartial')]/div/@data-image-src").extract()]
        if images:
            item_loader.add_value("images", images)    

        floor_plan_images  = [x for x in response.xpath("//a[contains(@href,'floorplan')]/img/@src").extract()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images )    

        item_loader.add_value("landlord_phone", "020 8245 2624")
        item_loader.add_value("landlord_email", "headoffice@castles.london")
        item_loader.add_value("landlord_name", "Castles London")       

        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and "house" in p_type_string.lower():
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    else:
        return None
