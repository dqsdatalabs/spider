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
    name = 'sabestate_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    def start_requests(self):
        
        start_url = "https://sabestates.com/properties-for-sale-and-to-let/?tab=available-to-let"
        
        yield Request(url=start_url,
                        callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get('page', 2)
        
        seen = False
        for url in response.xpath("//a[contains(.,'Details')]/@href").extract():
            yield Request(url, callback=self.populate_item)
            seen = True
        
        if page == 1 or seen:
            url = f"https://sabestates.com/properties-for-sale-and-to-let/page/{page}/?tab=available-to-let"
            yield Request(url, callback=self.parse, meta={"page": page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        prop_type = response.xpath("//li[contains(.,'Property Type')]/text()").get()
        desc = "".join(response.xpath("//div[@id='description']//text()").getall())
        if get_p_type_string(prop_type): item_loader.add_value("property_type", get_p_type_string(prop_type))
        else:
            if get_p_type_string(desc): item_loader.add_value("property_type", get_p_type_string(desc))
            else: return
        
        item_loader.add_xpath("title", "//h1/text()")
        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", "Sabestate_PySpider_united_kingdom")

        rent = response.xpath("//span[contains(@class,'item-price ')]/text()").extract_first()
        if rent:
            item_loader.add_value("rent", rent.split("£")[1].replace(",","."))
        item_loader.add_value("currency", "GBP")
        
        room = response.xpath("//li[contains(.,'Bedroom')]/text()").extract_first()
        if room:
            room = room.strip().split(" ")[0].replace(",","")
            item_loader.add_value("room_count", room)

        bathroom_count = response.xpath("//li[contains(.,'Bedroom')]/text()").extract_first()
        if bathroom_count:
            bathroom_count = bathroom_count.strip().split(" ")[0].replace(",","")
            item_loader.add_value("bathroom_count", bathroom_count)

        address = " ".join(response.xpath("//div[@class='header-left']/text()[3]").extract())
        if address:
            if " at " in address: address = address.split(" at ")[1].strip()
            if " in " in address: address = address.split(" in ")[1].strip()
            item_loader.add_value("address", address.strip())
            if "," in address:
                city = address.split(",")[1].strip()
                item_loader.add_value("city", city)
                
                zipcode = address.split(",")[-1].strip()
                if " " not in zipcode and not zipcode.isalpha():
                    item_loader.add_value("zipcode", zipcode)
                if zipcode.count(" ")==1:
                    item_loader.add_value("zipcode", zipcode)

        if desc:
            item_loader.add_value("description", desc.strip())

        images = [
            response.urljoin(x)
            for x in response.xpath("//div[@class='gallery-inner']//@src").extract()
        ]
        item_loader.add_value("images", images)

        terrace = response.xpath("//li[contains(.,'Terrace') or contains(.,'terrace') or contains(.,'TERRACE')]//text()").get()
        if terrace:
            item_loader.add_value("terrace", True)
        
        parking = response.xpath("//li[contains(.,'PARKING') or contains(.,'Parking') or contains(.,'parking')]//text()").get()
        if parking:
            item_loader.add_value("parking", True)
        
        furnished = response.xpath("//li[contains(.,'furnished') or contains(.,'Furnished')]//text()").extract_first()
        if furnished:
            item_loader.add_value("furnished", True) 

        balcony = " ".join(response.xpath("//li[contains(.,'BALCON') or contains(.,'Balcon') or contains(.,'balcon')]//text()").extract())
        if balcony:
            item_loader.add_value("balcony", True) 

        elevator = " ".join(response.xpath("//li[contains(.,'Lift') or contains(.,'LIFT') or contains(.,'lift')]//text()").extract())
        if elevator:
            item_loader.add_value("elevator", True)

        item_loader.add_value("landlord_name", "Sabestate")
        item_loader.add_value("landlord_phone", "020-8992-9922")
        item_loader.add_value("landlord_email", "info@sabestate.com")
            
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