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
class MySpider(Spider):
    name = 'stonehouse_estates_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en' 
    thousand_separator = ','
    scale_separator = '.'
    start_urls = ["https://www.stonehouse-estates.co.uk/properties/?department=residential-lettings&address_keyword=&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=&minimum_bedrooms=&view=&pgp="]
    external_source = "Stonehouse_Estates_Co_PySpider_united_kingdom"
    custom_settings = {
        "RETRY_HTTP_CODES": [500, 503, 504, 400, 401, 403, 405, 407, 408, 416, 456, 502, 429, 307],
        "PROXY_TR_ON": True,
        "HTTPCACHE_ENABLED": False
    }
    
    headers = {
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        "accept-language": "tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7",
        "user-agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Mobile Safari/537.36",
        "accept-encoding": "gzip, deflate, br",
        "upgrade-insecure-requests":" 1"
    }
    
    def start_requests(self):
        yield Request(self.start_urls[0], headers=self.headers, callback=self.parse)
    
    
    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//div[@class='thumbnail']/a"):
            status = item.xpath("./div[@class='flag']/text()").get()
            if status and "agreed" in status.lower().strip():
                continue
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item)
        
        next_page = response.xpath("//a[contains(@class,'next')]/@href").get()
        if next_page:
            yield Request(response.urljoin(next_page), callback=self.parse)
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("external_link", response.url)

        full_text = "".join(response.xpath("//div[@class='details-left']/p/text()").getall())
        if get_p_type_string(full_text):
            item_loader.add_value("property_type", get_p_type_string(full_text))
        else:
            return
        item_loader.add_value("external_source", self.external_source)
        external_id =response.xpath("substring-after(//span[contains(.,'Ref')]//text(),':')").get()
        if external_id:      
            item_loader.add_value("external_id", external_id.strip())
        title = response.xpath("//div[@class='details-left']/h2/text()").extract_first()
        if title:
            item_loader.add_value("title", title.strip())
            item_loader.add_value("address", title.strip())
            try:
                zipcode = title.split(",")[-1].strip()
                city= ""
                if len(zipcode.split(" "))>1:
                    zipcode1 = zipcode
                    zipcode = zipcode1.split(" ")[-1].strip()
                    city = zipcode1.replace(zipcode,"").strip()
       
                if not zipcode.isalpha():
                    item_loader.add_value("zipcode", zipcode)
                if city:
                    item_loader.add_value("city",city.strip())
                if not city:
                    item_loader.add_value("city", title.split(",")[-2].strip())
            except:pass
    
        room_count = response.xpath("//div[@class='details-left']//div[img[@title='Bedrooms']]//text()[.!='0']").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        if "studio" in get_p_type_string(full_text):
            item_loader.add_value("room_count", "1")


        bathroom_count=response.xpath("//div[@class='details-left']//div[img[@title='Bathrooms']]//text()").get()
        if bathroom_count:      
            item_loader.add_value("bathroom_count", bathroom_count)

        rent = response.xpath("//div[@class='details-left']/h4/text()").extract_first()
        if rent:
            if "pw" in rent.lower():
                rent = rent.split('£')[1].lower().split(' pw')[0].strip().replace(',', '').replace('\xa0', '')
                item_loader.add_value("rent", str(int(float(rent)) * 4))
                item_loader.add_value("currency", 'GBP')
            else:
                item_loader.add_value("rent_string", rent)    
        desc = " ".join(response.xpath("//span[contains(.,'Ref')]/following-sibling::p//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
            unit_pattern = re.findall(r"[+-]? *((?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?)\s*(Sq. Ft.|sqft|sq.ft|sq ft|sq. ft.|Sq. ft.|sq. Ft.|sq|Sq)",desc.replace(",",""))
            if unit_pattern:
                square_title=unit_pattern[0][0]
                sqm = str(int(float(square_title) * 0.09290304))
                item_loader.add_value("square_meters", sqm)
        script_map = response.xpath("//script/text()[contains(.,'LatLng')]").get()
        if script_map:
            latlng = script_map.split("google.maps.LatLng(")[1].split(")")[0]
            item_loader.add_value("latitude", latlng.split(",")[0].strip())
            item_loader.add_value("longitude", latlng.split(",")[1].strip())
 
        floor = response.xpath("//div[@class='details-right']/div//text()[contains(.,'FLOOR') and not(contains(.,'FLOORING')) and not(contains(.,'WOOD')) ]").extract_first()
        if floor:
            floor = floor.split("FLOOR")[0].strip().split(" ")[-1]
            item_loader.add_value("floor", floor)
        terrace = response.xpath("//div[@class='details-right']/div//text()[contains(.,'terrace') or contains(.,'Terrace') ]").extract_first()
        if terrace:
            item_loader.add_value("terrace", True)
        elevator = response.xpath("//div[@class='details-right']/div//text()[contains(.,'LIFT') or contains(.,'lift') ]").extract_first()
        if elevator:
            item_loader.add_value("elevator", True)
        balcony = response.xpath("//div[@class='details-right']/div//text()[contains(.,'balcony') or contains(.,'BALCONY') ]").extract_first()
        if balcony:
            item_loader.add_value("balcony", True)
        parking = response.xpath("//div[@class='details-right']/div//text()[contains(.,'PARKING') or contains(.,'Parking')]").extract_first()
        if parking:
            item_loader.add_value("parking", True)

        furnished = response.xpath("//div[@class='details-right']/div//text()[contains(.,'Furnished') or contains(.,'furnished') ]").extract_first()
        if furnished:
            if "furnished or unfurnished" in furnished.lower():
                pass
            elif "unfurnished" in furnished.lower():
                item_loader.add_value("furnished", False)
            else:
                item_loader.add_value("furnished", True)

        images = [x for x in response.xpath("//div[@class='wpb_wrapper']/span/a/img/@src").extract()]
        if images:
            item_loader.add_value("images", images)    

        floor_plan_images  = [x for x in response.xpath("//div/h2[contains(.,'Floorplan')]/following-sibling::img/@src").extract()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images )    

        item_loader.add_value("landlord_phone", "0207 281 7281")
        item_loader.add_value("landlord_email", "info@stonehouse-estates.co.uk")
        item_loader.add_value("landlord_name", "Stonehouse Estates")   
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
