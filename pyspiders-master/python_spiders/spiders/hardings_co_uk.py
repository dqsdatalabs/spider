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
import dateparser

class MySpider(Spider):
    name = 'hardings_co_uk'
    execution_type = 'testing' 
    country = 'united_kingdom'
    locale ='en'
    start_urls = ['https://hardings.co.uk/search?&p_department=RL']  # LEVEL 1

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//div[@class='property']"):
            follow_url = response.urljoin(item.xpath(".//@href").get())
            status = item.xpath(".//div[contains(@class,'availability')]/text()").get()
            if not status:
                yield Request(follow_url, callback=self.populate_item)

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("/")[-1])
        property_type = response.xpath("//span[@class='type']/text()").get()
        if get_p_type_string(property_type):
            item_loader.add_value("property_type", get_p_type_string(property_type))
        else: return
        
        item_loader.add_value("external_source", "Hardings_PySpider_united_kingdom")
        item_loader.add_value("external_id", response.url.split("/")[-1])

        item_loader.add_xpath("title", "//title/text()")
        
        rent = response.xpath("//span[@class='price']/text()").get()
        if rent:
            rent = rent.replace("pcm", "").replace("£","").replace(",","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "GBP")
        
        address = response.xpath("//h1/text()").get()
        if address:
            item_loader.add_value("address", address)
        item_loader.add_value("city", "Windsor")
        desc = "".join(response.xpath("//div[@class='full_description_large']//text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())
        
        room_count = response.xpath("//li[contains(.,'bedroom')]/text()").get()
        if room_count:
            room_count = room_count.split("bedroom")[0].lower().replace("large", "").replace("double","").strip().split(" ")[-1]
            if room_count.isdigit():
                item_loader.add_value("room_count", room_count)
        date=response.xpath("//li[contains(.,'Available')]/text()").get()
        if date:
            date=date.split("Available from")[-1].split("Available")[-1].strip()
            date_parsed = dateparser.parse(date, date_formats=["%d %m %Y"])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))
        
        dishwasher = response.xpath("//li[contains(.,'dishwasher')]/text()").get()
        if dishwasher:
            item_loader.add_value("dishwasher", True)
        
        pets_allowed = response.xpath("//li[contains(.,'Pet friendly')]/text()").get()
        if pets_allowed:
            item_loader.add_value("pets_allowed", True)
        
        furnished = response.xpath("//li[contains(.,'furnished') or contains(.,'Furnished')]/text()[not(contains(.,'Unfurnished'))]").get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        images = response.xpath("//script[contains(.,'thumbnail_images')]/text()").get()
        if images:
            images = images.split('"thumbnail_images",')[1].split(', ["')[0].strip()
            data = json.loads(images)
            for d in data:
                item_loader.add_value("images", d["image"])
        
        latitude = response.xpath("//@data-location").get()
        if latitude:
            item_loader.add_value("latitude", latitude.split(",")[0].strip())
            item_loader.add_value("longitude", latitude.split(",")[1].strip())
        
        
        floor_plan_images = [x.split("'")[1] for x in response.xpath("//a[p[contains(.,'Floorplan')]]/@onclick").getall()]
        item_loader.add_value("floor_plan_images", floor_plan_images)
        
        item_loader.add_value("landlord_name", "Hardings Estate Agents")
        item_loader.add_value("landlord_phone", "01753 833118")
        item_loader.add_value("landlord_email", "lettings@hardings.co.uk")
        
        status = response.xpath("//span[contains(@class,'availability')]//text()[contains(.,'Let')]").get()
        if not status:
            yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    else:
        return None