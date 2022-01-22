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
    name = 'listings_nl'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'
    start_urls = ["https://www.listings.nl/property/"]

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//a[@class='card']"):
            status = item.xpath(".//div[contains(@class,'status')]/text()").get()
            if status and "rented" in status.lower():
                continue
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item)
          
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Listings_PySpider_netherlands")
        item_loader.add_xpath("title", "//title/text()")

        f_text = "".join(response.xpath("//article[contains(@class,'col-lg-8')]/p/text()").getall())
        if get_p_type_string(f_text):
            item_loader.add_value("property_type", get_p_type_string(f_text))
        else:
            return
        desc = "".join(response.xpath("//article/p/text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())

        rent = "".join(response.xpath("//div[contains(@class,'bg-secondary')]/ul/li/text()[contains(.,'€')]").getall())
        if rent:
            price = rent.split(",")[0].strip().replace(",","")
            item_loader.add_value("rent_string",price.strip())

        address = "".join(response.xpath("//h1/text()").getall())
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", address.split(",")[1].strip())

        meters = "".join(response.xpath("//div[contains(@class,'bg-secondary')]/ul/li[1]/text()[not(contains(.,'Bedroom'))]").getall())
        if meters:
            item_loader.add_value("square_meters",meters.split("m")[0].strip())

        room_count = response.xpath("//div[contains(@class,'bg-secondary')]/ul/li/text()[contains(.,'Bedroom')]").get()
        if room_count:
            room = room_count.strip().split(" ")[0]
            if room !="0":
                item_loader.add_value("room_count",room.strip())

        LatLng = "".join(response.xpath("substring-before(substring-after(//script[contains(.,'LatLng')]/text(),'LatLng('),',')").getall())
        if LatLng:
            item_loader.add_value("latitude",LatLng.strip())
            item_loader.add_xpath("longitude","substring-before(substring-after(//script[contains(.,'LatLng')]/text(),','),')')")

        images = [x for x in response.xpath("//div[@class='row no-gutters']/a/@href").extract()]
        if images is not None:
            item_loader.add_value("images", images)  

        furnished = response.xpath("//div[contains(@class,'bg-secondary')]/ul/li/text()[contains(.,'furnished') or contains(.,'Furnished') ]").get()
        if furnished:
            if "Unfurnished" in furnished:
                item_loader.add_value("furnished",False)
            elif "Furnished" in furnished:
                item_loader.add_value("furnished",True)

        item_loader.add_value("landlord_phone", "+31614499787")
        item_loader.add_value("landlord_name", "Listings")
        item_loader.add_value("landlord_email", "info@listings.nl")
        
        yield item_loader.load_item()


def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "appartement" in p_type_string.lower() or "bovenwoning" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "cottage" in p_type_string.lower() or "terrace" in p_type_string.lower()):
        return "house"
    else:
        return None