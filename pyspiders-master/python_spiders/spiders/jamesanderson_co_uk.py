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
    name = 'jamesanderson_co_uk'
    start_urls = ['https://jamesanderson.co.uk/property-search']  # LEVEL 1
    
    headers = {
        "accept" : "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        "referer": "https://jamesanderson.co.uk/property-search",
        "upgrade-insecure-requests": "1",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36",
    }
    
    def start_requests(self):
        
        formdata = {
            "listing_type": "rent",
            "local": "",
            "bedrooms": "",
            "min_price": "none",
            "max_price": "none",
            "order": "DESC",
            "exclude_let_by": "exclude_let_by",
        }
        
        yield FormRequest(
            url=self.start_urls[0],
            dont_filter=True,
            formdata=formdata,
            headers=self.headers,
            callback=self.parse
        )

    # 1. FOLLOWING
    def parse(self, response):
        script = response.xpath("//div/@data-props").get()
        data = json.loads(script)
        for item in data:
            follow_url = f"https://jamesanderson.co.uk/?p={item['id']}"
            yield Request(follow_url, callback=self.populate_item, meta={"item":item})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Jamesanderson_Co_PySpider_united_kingdom")
        desc = "".join(response.xpath("//p[@class='description']//text()").getall())
        if get_p_type_string(desc):
            item_loader.add_value("property_type", get_p_type_string(desc))
        else:
            if get_p_type_string(response.url):
                item_loader.add_value("property_type", get_p_type_string(response.url))
            elif "bedroom" in response.url: item_loader.add_value("property_type", "house")
            else: return
        
        address = response.xpath("//h2[@class='title']/text()").get()
        if address:
            item_loader.add_value("title", address)
            item_loader.add_value("address", address)
            zipcode = address.split(" ")[-1]
            city = address.split(zipcode)[0].strip().strip(",").split(",")[-1].strip()
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode)
        
        room_count = response.xpath("//h4[contains(@class,'subtitle')]/text()").get()
        if room_count and "Bedroom" in room_count:
            room_count = room_count.split("Bedroom")[0].strip()
            if room_count !='0':
                item_loader.add_value("room_count", room_count)
            else:
                room_count = response.xpath("//ul[@class='features']/li[contains(.,'Bedroom')]/text()[2]").get()
                if room_count and "One" in room_count:
                    item_loader.add_value("room_count", "1")
                
        rent = response.xpath("//h4[contains(@class,'price')]/text()").get()
        if rent:
            rent = rent.split("p")[0].split("£")[1].replace(",","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "GBP")
        
        furnished = response.xpath("//ul[@class='features']/li[contains(.,'Furnished')]/text()[2]").get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        if desc:
            item_loader.add_value("description", desc)
        
        energy_label = response.xpath("//ul[@class='features']/li[contains(.,'EPC')]/text()[2]").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.strip().split(" ")[-1])
        
        floor_plan_images = [x for x in response.xpath("//li[@id='floorplansSlide']//@data-src").getall()]
        item_loader.add_value("floor_plan_images", floor_plan_images)
        
        parking = response.xpath("//ul[@class='features']/li[contains(.,'Parking')]/text()[2]").get()
        if parking:
            item_loader.add_value("parking", True)
        
        elevator = response.xpath("//ul[@class='features']/li[contains(.,'Lift')]/text()[2]").get()
        if elevator:
            item_loader.add_value("elevator", True)
        
        balcony = response.xpath("//ul[@class='features']/li[contains(.,'Balcony')]/text()[2]").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        images = [x for x in response.xpath("//ul[@class='slides']//@data-src").getall()]
        item_loader.add_value("images", images)
        
        data = response.meta.get('item')
        item_loader.add_value("latitude", str(data["lat"]))
        item_loader.add_value("longitude", str(data["lng"]))
        
        item_loader.add_value("landlord_name", "James Anderson Estate Agents")
        item_loader.add_value("landlord_phone", "020 8876 7222")
        item_loader.add_value("landlord_email", "info@jamesanderson.co.uk")
        
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