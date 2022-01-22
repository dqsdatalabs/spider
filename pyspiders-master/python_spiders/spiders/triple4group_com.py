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
    name = 'triple4group_com'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    start_urls = ["https://www.triple4group.com/properties/?page=1&propind=L&orderBy=PriceSearchAmount&orderDirection=DESC&hideProps=1&businessCategoryId=1&searchType=grid&sortBy=highestPrice"]

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//div[@class='photo']/a"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 2 or seen:
            p_url = f"https://www.triple4group.com/properties/?page={page}&pageSize=12&orderBy=PriceSearchAmount&orderDirection=DESC&propind=L&businessCategoryId=1&searchType=grid&hideProps=1&stateValues=1"
            yield Request(
                p_url,
                callback=self.parse,
                meta={"page":page+1})
        
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Triple4group_PySpider_united_kingdom")
        item_loader.add_value("external_link", response.url)

        f_text = " ".join(response.xpath("//div[@class='description']//text()").getall())
        if get_p_type_string(f_text):
            item_loader.add_value("property_type", get_p_type_string(f_text))
        else:
            f_text = " ".join(response.xpath("//div[@class='bedswithtype']//text()").getall())
            if get_p_type_string(f_text):
                item_loader.add_value("property_type", get_p_type_string(f_text))
            else:
                return

        item_loader.add_value("external_id", response.url.split('property/')[1].split('/')[0].strip())
        
        title = "".join(response.xpath("//div[@class='bedswithtype']//text()").getall())
        if title:
            item_loader.add_value("title", title.strip())
        
        address = response.xpath("//div[@class='address']/text()").get()
        if address:
            item_loader.add_value("address", address)
            item_loader.add_value("city", address.split(" ")[-1])
            
        zipcode = response.xpath("//div[@class='summary']/text()").get()
        if zipcode:
            if "," in zipcode:
                zipcode = zipcode.split(",")[-1].strip()
                if not zipcode.isalpha():
                    item_loader.add_value("zipcode", zipcode.replace(".",""))
        
        rent = response.xpath("//span[@class='displayprice']//text()").get()
        if rent:
            price = rent.split("£")[1].replace(",","").strip()
            item_loader.add_value("rent", price)
            item_loader.add_value("currency", "GBP")
        
        room_count = "".join(response.xpath("//span[@class='beds']//text()").getall())
        if room_count:
            if room_count.strip() != "0":
                item_loader.add_value("room_count", room_count.strip())
            else:
                room_count = "".join(response.xpath("//span[@class='receptions']//text()").getall())
                item_loader.add_value("room_count", room_count.strip())
                
        
        bathroom_count = "".join(response.xpath("//span[@class='bathrooms']//text()").getall())
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())
        
        desc = " ".join(response.xpath("//div[@class='description']//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        images = [x for x in response.xpath("//div[@class='propertyimagelist']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)

        latitude_longitude = response.xpath("//a[contains(@href,'lat=')]/@href").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('lat=')[1].split('&')[0]
            longitude = latitude_longitude.split('lng=')[1].split('&')[0]     
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
        
        parking = response.xpath("//div[@class='description']//text()[contains(.,'Parking')]").get()
        if parking:
            item_loader.add_value("parking", True)
                
        item_loader.add_value("landlord_name", "TRIPLE4GROUP PROPERTY MANAGEMENT")
        
        phone = response.xpath("//a[contains(@href,'tel')]/@href").get()
        if phone:
            item_loader.add_value("landlord_phone", phone.split(":")[1].strip())
        
        item_loader.add_value("landlord_email","email@triple4group.com")
        
        
        yield item_loader.load_item()


def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "etage" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "woning" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "house"
    else:
        return None