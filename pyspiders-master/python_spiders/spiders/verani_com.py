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
    name = 'verani_com_disabled'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    start_urls = ['https://verani.com/rews-service/map/ajax/search.php?template=gallery&listOffset=0&SetSort=price-hi2lo']  # LEVEL 1

    headers = {
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36",
        "x-requested-with": "XMLHttpRequest"
    }
    
    def start_requests(self):
        yield Request(self.start_urls[0], headers=self.headers)
    
    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get('page', 30)
        data = json.loads(response.body)["list"]
        seen = False
        for item in data:
            external_id = str(item[0])
            url = ""
            for i in item:
                if external_id in str(i):
                    url = i
            url = f"https://verani.com/{url}"
            yield Request(url, callback=self.populate_item)
            seen = True
        
        if page == 30 or seen:
            url = f"https://verani.com/rews-service/map/ajax/search.php?template=gallery&listOffset={page}&SetSort=price-hi2lo"
            yield Request(url, callback=self.parse, meta={"page": page+30})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        prop_type = response.xpath("//b[contains(.,'Type')]/following-sibling::span/text()").get()
        if get_p_type_string(prop_type):
            item_loader.add_value("property_type", get_p_type_string(prop_type))
        else: return
        
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("-")[-1])
        item_loader.add_value("external_source", "Verani_PySpider_united_kingdom")
        title="".join(response.xpath("//h2[@class='responsive-listing-items']//span//text()").getall())
        if title:
            item_loader.add_value("title",title)
        dontallow=response.url
        if dontallow and "parking" in dontallow.lower():
            return
 

        rent = response.xpath("//b[contains(.,'Price')]/following-sibling::div/div/text()").get()
        if rent:
            rent = rent.split(" ")[0].replace(",","").replace("$","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "AUD")
        description="".join(response.xpath("//div[@class='responsive-listing-items']/div/text()").getall())
        if description:
            description=description.replace("\t","").replace("\n","").replace("  ","")
            item_loader.add_value("description",description) 
        
        room_count = response.xpath("//b[contains(.,'Bed')]/following-sibling::span/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        
        bathroom_count = response.xpath("//b[contains(.,'Total Bath')]/following-sibling::span/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split(" ")[0])
        
        square_meters = response.xpath("//span[contains(.,'sqft')]/text()").get()
        if square_meters:
            square_meters = square_meters.split("s")[0].replace(",","").strip()
            item_loader.add_value("square_meters", str(int(float(square_meters)* 0.09290304)))
        
        address = item_loader.get_output_value("title")
        if address:
            address = address.split(":")[-1].strip()
            item_loader.add_value("address", address)
        
        city = response.xpath("//b[contains(.,'City')]/following-sibling::span/text()").get()
        item_loader.add_value("city", city)
        
        zipcode = response.xpath("//div[contains(@class,'responsive-listing-items')]/span/span[last()]/text()").get()
        if zipcode and zipcode.isdigit():
            item_loader.add_value("zipcode", zipcode)
        images = [x for x in response.xpath("//img[@itemprop='photo']/@src").getall()]
        if images:
                item_loader.add_value("images", images)
        
        dishwasher = response.xpath("//span[contains(.,'Dishwasher')]").get()
        if dishwasher:
            item_loader.add_value("dishwasher", True)


        
        washing = response.xpath("//span[contains(.,'Washing')]").get()
        if washing:
            item_loader.add_value("washing_machine", True)
            
        swimming_pool = response.xpath("//span[contains(.,'Pool')]").get()
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)
        
        balcony = response.xpath("//span[contains(.,'Balcony')]").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        parking = response.xpath("//b[contains(.,'Parking')]/following-sibling::span/text()[not(contains(.,'No'))] | //b[contains(.,'Garage')]/following-sibling::span/text()[not(contains(.,'No'))]").get()
        if parking:
            item_loader.add_value("parking", True)
        
        latitude_longitude = response.xpath("//script[contains(.,'LatLng(') and not(contains(.,'LatLng( this'))]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('LatLng(')[1].split(',')[0].strip()
            longitude = latitude_longitude.split('LatLng(')[1].split(',')[1].split(')')[0].strip()
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
        
        landlord_name = response.xpath("//span[@class='agentName']/text()").get()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name.strip())
        else: item_loader.add_value("landlord_name", "Verani Realty")
        
        landlord_phone = response.xpath("//span[@itemprop='telephone']//text()").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone.strip())
        else: item_loader.add_value("landlord_phone", "8887230306")
        
        sale = "".join(response.xpath("//h2[contains(.,'Sale')]//text()").getall())
        if not sale:
            yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower() or "unit" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "home" in p_type_string.lower() or "family" in p_type_string.lower() or "villa" in p_type_string.lower() or "detached" in p_type_string.lower()):
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    else:
        return None