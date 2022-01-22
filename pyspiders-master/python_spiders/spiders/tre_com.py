# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import re
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json

class MySpider(Spider):
    name = 'tre_com'
    external_source = "Tre_PySpider_australia"
    execution_type='testing'
    country='australia'
    locale='en'
    start_urls = ['https://tre.com.au/renting/for-rent/']  # LEVEL 1
    custom_settings = {
        "HTTPCACHE_ENABLED": False,
    }
    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get('page', 2)
        seen = False
        for item in response.xpath("//a[contains(.,'View')]/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
            seen = True
        
        if page == 2 or seen:
            url = f"https://tre.com.au/renting/for-rent/?page={page}"
            yield Request(url, callback=self.parse, meta={"page": page+1, "property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("/")[-1])
        desc = "".join(response.xpath("//div[@id='listing-view-about']//p//text()").getall())
        if get_p_type_string(desc):
            item_loader.add_value("property_type", get_p_type_string(desc))
        else:
            return
        item_loader.add_value("external_source", self.external_source)
        
        item_loader.add_xpath("title", "//title/text()")


        
        address = response.xpath("//h1/text()").get()
        if address:
            item_loader.add_value("address", address)
        city=item_loader.get_output_value("address")
        if city:
            item_loader.add_value("city",city.split(" ")[-2:])
        
        room_count = response.xpath("//span[@class='beds']/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip().split("bed")[0].strip().split(" ")[-1])
        
        bathroom_count = response.xpath("//span[@class='baths']/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip().split("bath")[0].strip().split(" ")[-1])
        
        rent = response.xpath("//span[@class='listing-price']/text()").get().strip()
        if rent and "$" in rent:
            if "week" in rent:
                rent = int(rent.split(" ")[0].replace("$",""))*4
            else: 
                rent = rent.split(" ")[0].replace("$","")
        
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "AUD")
        rentcheck=item_loader.get_output_value("rent")
        if not rentcheck:
            rent1=response.xpath("//span[@class='listing-price']/text()").get().strip()
            if rent1 and "week" in rent1:
                rent1=int(rent1.split("per")[0])*4
                if rent1:
                    item_loader.add_value("rent",rent1)
        
        if desc:
            item_loader.add_value("description", re.sub('\s{2,}', ' ', desc.strip()))
        parking=response.xpath("//span[@class='listing-amenities']//span[@class='parking']/text()").get()
        if parking:
            item_loader.add_value("parking",True)

        images = [x for x in response.xpath("//span[@class='swappr-slide']//@data-swappr-src").getall()]
        item_loader.add_value("images", images)
        
        lat_lng = response.xpath("//div/@data-latlon").get()
        if lat_lng:
            lat = lat_lng.split(",")[0]
            lng = lat_lng.split(",")[1]
            item_loader.add_value("latitude", lat)
            item_loader.add_value("longitude", lng)
            
        item_loader.add_value("landlord_name", "Thomson IP")
        item_loader.add_value("landlord_phone", "03 9509 8244")
        email=response.xpath("//p[@class='agent-options']//a[contains(@href,'mailto')]/@href").get()
        if email:
            item_loader.add_value("landlord_email",email.split(":")[-1])
        
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "flatshare" in p_type_string.lower():
        return "room"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "unit" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "maisonette" in p_type_string.lower() or "home" in p_type_string.lower() or "cottage" in p_type_string.lower()):
        return "house"
    elif p_type_string and "single room" in p_type_string.lower():
        return "room"
    elif p_type_string and "bedroom" in p_type_string.lower():
        return "house"
    else:
        return None