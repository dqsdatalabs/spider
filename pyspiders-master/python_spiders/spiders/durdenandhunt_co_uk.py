# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader 
import json
from word2number import w2n
import re

class MySpider(Spider):
    name = 'durdenandhunt_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en' 
    thousand_separator = ','
    scale_separator = '.'
    start_urls = ["https://www.durdenandhunt.co.uk/Search?listingType=6&category=1&areainformation=&radius=&bedrooms=&minprice=&maxprice=&statusids=1&obc=Price&obd=Descending&perpage=30"]
    external_source = "Durdenandhunt_Co_PySpider_united_kingdom"

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2) 

        seen = False
        for item in response.xpath("//div[@class='status' and .='To Let']/../a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 2 or seen:
            p_url = response.url.split("&page")[0] + f"&page={page}"
            yield Request(p_url, callback=self.parse, meta={"page":page+1})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("/")[-1])

        desc = "".join(response.xpath("//h2[contains(.,'Full Description')]/../text()").getall())
        if get_p_type_string(desc):
            item_loader.add_value("property_type", get_p_type_string(desc))
        else:
            return
        item_loader.add_value("external_source", self.external_source)
         
        item_loader.add_xpath("title", "//h1//text()")
        
        rent = response.xpath("//div/h2/div[@data-bind='with: $root.modal']//text()[contains(.,'£')]").extract_first()
        if rent:
            item_loader.add_value("rent_string", rent) 
        
        room_count = response.xpath("//div[i[@class='i-bedrooms']]/text()[normalize-space()]").extract_first()        
        if room_count:
            room_count = room_count.strip()
            item_loader.add_value("room_count", room_count)        
        elif not room_count:
            room_count = response.xpath("//div[@class='featuresBox']//ul/li[contains(.,'Bedroom')]//text()").extract_first()
            if room_count: 
                try:     
                    room_count = room_count.split("Bed")[0].strip()     
                    item_loader.add_value("room_count", w2n.word_to_num(room_count))
                except:
                    pass
            elif "studio" in get_p_type_string(desc):
                item_loader.add_value("room_count","1")
        room1=response.xpath("//li/span[contains(text(),'bedrooms')]/text()").get()      
        if room1:
            if room1:
                room_count1=re.findall("\d+",room1)
                item_loader.add_value("room_count",room_count1)

            
        bathroom_count = response.xpath("//div[@class='featuresBox']//ul/li[contains(.,'Bathroom')]//text()").extract_first()
        if bathroom_count:      
            try:
                bathroom_count = bathroom_count.split("Bath")[0].strip()     
                numara = w2n.word_to_num(bathroom_count)
                item_loader.add_value("bathroom_count", numara)
            except:
                pass
        else:
            bathroom_count = response.xpath("//div[@class='detailBottom']//text()[contains(.,'bathroom') or contains(.,'Bathroom')]").get()
            if bathroom_count:      
                try:
                    bathroom_count = bathroom_count.lower().split("bath")[0].strip().split(" ")[-1].strip()     
                    numara = w2n.word_to_num(bathroom_count)
                    item_loader.add_value("bathroom_count", numara)
                except:
                    pass 
        
        floor_list = ['first','second','third','fourth', 'fifth', 'sixth', 'seventh', 'eighth', 'ninth', 'tenth']
        floor = response.xpath("//div[@class='detailBottom']//text()[contains(.,'floor')]").get()
        if floor:
            try:
                floor = floor.split("floor")[0].strip().split(" ")[-1].strip()
                floor = floor_list.index(floor) + 1
                item_loader.add_value("floor", floor)
            except:
                pass

        address = response.xpath("//h1//text()").extract_first()       
        if address:
            item_loader.add_value("address", address)   
            zipcode = address.split(",")[-1].strip()
            city = address.split(",")[-3].strip()
            item_loader.add_value("zipcode", zipcode)   
            item_loader.add_value("city", city)   

        deposit = response.xpath("//div[@class='featuresBox']//ul/li[contains(.,'Deposit') or contains(.,'deposit') ]//text()").extract_first()
        if deposit:
            item_loader.add_value("deposit", deposit.lower().split("depo")[0].replace("£", "").strip())
   
        parking = response.xpath("//div[@class='featuresBox']//ul/li[contains(.,'parking') or contains(.,'Parking')]//text()").extract_first()
        if parking:
            item_loader.add_value("parking", True)
        elevator = response.xpath("//div[@class='featuresBox']/ul/li[contains(.,'Lift') or contains(.,'lift')  ]//text()").extract_first()
        if elevator:
            item_loader.add_value("elevator", True)
        balcony = response.xpath("//div[@class='featuresBox']//ul/li[contains(.,'Balcony') or contains(.,'balcony')]//text()").extract_first()
        if balcony:
            item_loader.add_value("balcony", True)


        furnished = response.xpath("//div[@class='featuresBox']//ul/li[contains(.,'Furnished') or contains(.,'furnished')]//text()").extract_first()
        if furnished:
            if "unfurnished" in furnished.lower():
                item_loader.add_value("furnished", False)
            else:
                item_loader.add_value("furnished", True)
        else:
            furnished = response.xpath("//div[@class='detailBottom']//text()[contains(.,'Furnished') or contains(.,'furnished')]").get()
            if furnished:
                if "unfurnished" in furnished.lower():
                    item_loader.add_value("furnished", False)
                else:
                    item_loader.add_value("furnished", True)
       
        desc = " ".join(response.xpath("//h2[contains(.,'Full Description')]/../text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
        images = [x for x in response.xpath("//div[@id='property-photos-device1']//img/@src").extract()]
        if images:
            item_loader.add_value("images", images)    

        item_loader.add_value("landlord_phone", "02030260160")
        item_loader.add_value("landlord_email", "lettings@durdenandhunt.co.uk")
        item_loader.add_value("landlord_name", "Durden and Hunt")     

        map_iframe_no = response.url.split("/")[-1].strip()
        map_url = f"https://www.durdenandhunt.co.uk/Map-Property-Search-Results?references={map_iframe_no}"
        if map_url:
            yield Request(
                map_url,
                callback=self.get_map,
                meta={
                    "item_loader" : item_loader 
                }
            )
    
    def get_map(self, response):
        item_loader = response.meta.get("item_loader")
        map_json = json.loads(response.body)
       
        for data in map_json["items"]:
            lat = data["lat"]
            lng = data["lng"]
            if lat and lng:
                item_loader.add_value("latitude", str(lat))
                item_loader.add_value("longitude", str(lng))
   
        yield item_loader.load_item()
     

def get_p_type_string(p_type_string):
    if p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and "house" in p_type_string.lower():
        return "house"
    elif p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    else:
        return None
