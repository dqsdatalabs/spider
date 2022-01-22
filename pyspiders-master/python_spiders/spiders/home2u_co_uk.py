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
    name = 'home2u_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    external_source = "Home2u_Co_PySpider_united_kingdom"
    start_urls = ['https://www.home2u.co.uk/Search?listingType=6&statusids=1&obc=Price&obd=Descending&category=1&areadata=&areaname=&minprice=&maxprice=&radius=&bedrooms=']  # LEVEL 1
    
    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//div[@class='status']//img[contains(@alt,'Available')]/parent::div/following-sibling::a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        description = "".join(response.xpath("//h3[contains(.,'Detail')]/following-sibling::text()").getall())
        if get_p_type_string(description):
            item_loader.add_value("property_type", get_p_type_string(description))
        else: return
        item_loader.add_value("external_source", self.external_source)
        
        title = response.xpath("//title/text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        
        address = response.xpath("//h1/text()").get()
        if address:
            item_loader.add_value("address", address)
            zipcode = address.split(",")[-1].strip()
            if " " not in zipcode:
                item_loader.add_value("zipcode", zipcode)
                item_loader.add_value("city", address.split(",")[-2].strip())
            else:
                item_loader.add_value("city", zipcode)
        
        description = " ".join(response.xpath("//div[@class='scroll']//text()").getall())
        if description:
            description = description.split("Details:")[1].split("Viewing & Disclaimer")[0].strip()
            item_loader.add_value("description", description)
        
        room_count = response.xpath("//i[contains(@class,'bed')]/following-sibling::span[1]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        elif "studio" in description.lower():
            item_loader.add_value("room_count", "1")
        elif response.xpath("//strong/span[contains(.,'Bedroom')]/text()").get():
            room = response.xpath("//strong/span[contains(.,'Bedroom')]/text()").get().split(" ")[1]
            item_loader.add_value("room_count", room)
        elif description.count("Bedroom")>0:
            item_loader.add_value("room_count", description.count("Bedroom"))
        
        room_count = response.xpath("//i[contains(@class,'bath')]/following-sibling::span[1]/text()").get()
        if room_count:
            item_loader.add_value("bathroom_count", room_count)
        
        rent = response.xpath("//h3/div[contains(@data-bind,'root')]//text()[normalize-space()][not(contains(.,'Click for price options'))]").get()
        if rent:
             rent = rent.split("£")[1].strip().split(" ")[0].replace(",","")
             item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "GBP")
        
        images = [x for x in response.xpath("//div[@id='gallery']//@href").getall()]
        if images:
            item_loader.add_value("images", images)
        
        item_loader.add_value("landlord_name","Home2U")

        parking = response.xpath("//text()[contains(.,'Garage')]").get()
        if parking: item_loader.add_value("parking", True)
        

        item_loader.add_value("landlord_phone", "02086905000")
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "villa" in p_type_string.lower() or "home" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "maisonette" in p_type_string.lower() or "terrace" in p_type_string.lower()):
        return "house"
    elif p_type_string and "room" in p_type_string.lower():
        return "room"
    else:
        return None