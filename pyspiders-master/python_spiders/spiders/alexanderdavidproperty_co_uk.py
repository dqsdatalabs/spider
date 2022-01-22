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
    name = 'alexanderdavidproperty_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'

    def start_requests(self):
        yield Request(
            "https://alexanderdavidproperty.co.uk/results.php?property-status=Residential+Lettings&location=&search-minbeds=0&search-maxbeds=6&search-minprice=0&search-maxprice=8500",
            callback=self.parse,
            )

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@id='all']/div/div//h3/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item)
            
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        property_type = " ".join(response.xpath("//aside[h3[contains(.,'Description')]]/p//text()").getall())
        if get_p_type_string(property_type): item_loader.add_value("property_type", get_p_type_string(property_type))
        else: return
        item_loader.add_value("external_source", "Alexanderdavidproperty_Co_PySpider_united_kingdom")
        item_loader.add_value("external_id", response.url.split("reference=")[1])

        title = " ".join(response.xpath("//div[contains(@class,'page-top-in')]//h2//text()").getall())
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        address = "".join(response.xpath("//script[contains(.,'fullAddress')]//text()").getall())
        if address:
            address = address.split('fullAddress = "')[1].split('"')[0].replace(", UK","")
            zipcode = address.split(",")[-1]
            city = address.split(",")[-3]
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode)

        rent = response.xpath("//h3//span[contains(@class,'price')]//text()").get()
        if rent:
            if "week" in rent.lower():
                price = rent.strip().split("£")[1].split(" ")[0].replace(",","")
                price = int(price)*4
            else:
                price = rent.strip().split("£")[1].split(" ")[0].replace(",","")
            if rent > "0":
                item_loader.add_value("rent", price)
        item_loader.add_value("currency", "GBP")

        desc = " ".join(response.xpath("//h3[contains(.,'Description')]//following-sibling::p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        room_count = response.xpath("//i[contains(@class,'bedroom')]//parent::span//text()").get()
        if room_count:
            room_count = room_count.strip()
            item_loader.add_value("room_count", room_count)

        bathroom_count = response.xpath("//i[contains(@class,'bathroom')]//parent::span//text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.strip()
            item_loader.add_value("bathroom_count", bathroom_count)
        
        images = [x for x in response.xpath("//ul[contains(@class,'slides')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)

        parking = response.xpath("//li[contains(.,'Parking')]//text()").get()
        if parking:
            item_loader.add_value("parking", True)

        balcony = response.xpath("//li[contains(.,'Balcony')]//text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        terrace = response.xpath("//li[contains(.,'Terrace')]//text()").get()
        if terrace:
            item_loader.add_value("terrace", True)

        furnished = response.xpath("//li[contains(.,'Furnished')]//text()").get()
        if furnished:
            item_loader.add_value("furnished", True)

        floor = response.xpath("//li[contains(.,'Floor')]//text()").get()
        if floor:
            floor = floor.replace("New","").strip().split(" ")[0]
            item_loader.add_value("floor", floor.strip())

        item_loader.add_value("landlord_name", "Alexander David Property")
        item_loader.add_value("landlord_phone", "020 8980 3480")
        item_loader.add_value("landlord_email", "info@alexanderdavidproperty.co.uk")
        
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "woning" in p_type_string.lower() or "bungalow" in p_type_string.lower()):
        return "house"
    else:
        return None