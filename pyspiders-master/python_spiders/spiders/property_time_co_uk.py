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
    name = 'property_time_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    thousand_separator = ','
    scale_separator = '.'
    start_urls = ["https://www.property-time.co.uk/Search?listingType=6&statusids=1&obc=Price&obd=Descending&areadata=&areaname=&radius=&bedrooms=&minprice=&maxprice="]

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//a[@class='fdLink']/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
        
        next_page = response.xpath("//a[.='»']/@href").get()
        if next_page:
            yield Request(response.urljoin(next_page), callback=self.parse)
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)


        status = response.xpath("//div[@class='status']/text()").get()
        if status and "available" not in status.lower().strip():
            return
        item_loader.add_value("external_source", "Property_Time_Co_PySpider_united_kingdom")
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id",response.url.split("/")[-1])

        summary = "".join(response.xpath("//h2[contains(.,'Summary')]/following-sibling::div/p/text()").getall())
        if get_p_type_string(summary):
            item_loader.add_value("property_type", get_p_type_string(summary))
        else:
            return
        room = response.xpath("//div[@class='detailRoomsIcon']/i[@class='i-bedrooms']/following-sibling::text()[normalize-space()]").extract_first()
        if room and room.strip() !='0':
            item_loader.add_value("room_count", room.strip())
        elif "studio" in item_loader.get_collected_values("property_type"):
            item_loader.add_value("room_count", "1")

        bathroom = response.xpath("//div[@class='detailRoomsIcon']/i[@class='i-bathrooms']/following-sibling::text()").extract_first()
        if bathroom and bathroom.strip() !='0':
            item_loader.add_value("bathroom_count", bathroom.strip())

        address = response.xpath("//div/h1/text()").extract_first()
        if address:
            item_loader.add_value("title", address.strip())
            item_loader.add_value("address", address.strip())
            item_loader.add_value("zipcode", address.split(",")[-1].strip())
            item_loader.add_value("city", address.split(",")[-2].strip())
        
        rent = response.xpath("//div/h2/div[contains(.,'£')]//text()").extract_first()
        if rent: 
            if "pw" in rent.lower():
                rent = rent.split("£")[1].strip().split(" ")[0].replace(",","")
                rent = int(float(rent))*4
            else:
                rent = rent.split("£")[1].strip().split(" ")[0].replace(",","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "GBP")
                            
        desc = " ".join(response.xpath("//h2[contains(.,'Summary')]/following-sibling::div/p/text()").extract())
        if desc:
            item_loader.add_value("description",desc.strip())      
            if "No Pets" in desc:
                item_loader.add_value("pets_allowed",False)  
            if "available from" in desc.lower():
                try:
                    date_parsed = dateparser.parse(desc.lower().split("available from")[1].split(".")[0], languages=['en'])
                    if date_parsed:
                        date2 = date_parsed.strftime("%Y-%m-%d")
                        item_loader.add_value("available_date", date2)      
                except:
                    pass
            unit_pattern = re.findall(r"[+-]? *((?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?)\s*(Sq. Ft.|sq. ft.|Sq. ft.|sq. Ft.|sq|Sq)",desc.replace(",","").replace("(",""))
            if unit_pattern:
                square_title=unit_pattern[0][0]
                sqm = str(int(float(square_title) * 0.09290304))
                item_loader.add_value("square_meters", sqm)
      
        images = [response.urljoin(x) for x in response.xpath("//div[@id='property-photos-device1']/a/@href").extract()]
        if images:
            item_loader.add_value("images", images)   

        floor_plan_images = [response.urljoin(x) for x in response.xpath("//div/a[contains(@href,'floorplan')]/@href").extract()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)
    
        floor = response.xpath("//div[contains(@class,'featuresBox')]/ul/li[contains(.,'Floor')]/text()[not(contains(.,'Floors'))]").extract_first()
        if floor: 
            item_loader.add_value("floor", floor.split("Floor")[0].strip().split(" ")[-1]) 
        terrace = response.xpath("//div[contains(@class,'featuresBox')]/ul/li/text()[contains(.,'Terrace') or contains(.,'terrace')]").extract_first()
        if terrace: 
            item_loader.add_value("terrace", True) 
        balcony = response.xpath("//div[contains(@class,'featuresBox')]/ul/li/text()[contains(.,'Terrace') or contains(.,'terrace')]").extract_first()
        if balcony: 
            item_loader.add_value("balcony", True) 
            
        parking = response.xpath("//div[contains(@class,'featuresBox')]/ul/li/text()[contains(.,'Garage') or contains(.,'Parking') or contains(.,'parking') ]").extract_first()
        if parking: 
            item_loader.add_value("parking", True) 

        elevator = response.xpath("//div[contains(@class,'featuresBox')]/ul/li/text()[contains(.,'Lift')]").extract_first()
        if elevator: 
            item_loader.add_value("elevator", True) 

        furnished = response.xpath("//div[contains(@class,'featuresBox')]/ul/li/text()[contains(.,'Furnished') or contains(.,'furnished')]").extract_first()
        if furnished: 
            if "furnished or unfurnished" in furnished.lower():
                pass
            elif "unfurnished" in furnished.lower():
                item_loader.add_value("furnished", False)  
            elif "furnished" in furnished.lower():
                item_loader.add_value("furnished", True) 
      
        item_loader.add_value("landlord_email", "info@property-time.co.uk")
        item_loader.add_value("landlord_phone", "020 7794 2008")
        item_loader.add_value("landlord_name", "PROPERTY TIME")

        map_id = response.url.split("/")[-1]
        if map_id:
            map_url = f"https://www.property-time.co.uk/Map-Property-Search-Results?references{map_id}&category=1"
            yield Request(map_url, callback=self.parse_map,meta={"item":item_loader})
        else:
            yield item_loader.load_item()

    def parse_map(self, response):
        item_loader = response.meta.get('item')
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
