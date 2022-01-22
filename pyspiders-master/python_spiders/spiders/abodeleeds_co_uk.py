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
    name = 'abodeleeds_co_uk'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    start_urls = ["https://abodeleeds.co.uk/list/?filter=true&area=&bedrooms=&search=Search&view=list"]

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//div[@class='list-item__main']/a"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 2 or seen:
            p_url = f"https://abodeleeds.co.uk/list/page/{page}?filter=true&area=&bedrooms=&search=Search&view=list"
            yield Request(
                p_url,
                callback=self.parse,
                meta={"page":page+1})
        
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Abodeleeds_Co_PySpider_united_kingdom")
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", "student_apartment")

        ext_id = response.url.split("_")[-1].strip().strip("/")
        if ext_id:
            item_loader.add_value("external_id", ext_id)
        
        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", re.sub('\s{2,}', ' ', title.strip()))
        
        address = response.xpath("//h1/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", address.strip().split(",")[-1])
        
        features = response.xpath("//div[@class='property__meta']//text()").get()
        if features:
            room_count = features.split("Bed")[0].split("|")[-1].strip()
            item_loader.add_value("room_count", room_count)

            rent = features.split("Week:")[1].split(".")[0].replace("£","").strip()
            item_loader.add_value("rent", int(rent)*4)
            item_loader.add_value("currency", "GBP")
            
            deposit = features.split("Deposit:")[1].split("|")[0].replace("£","").strip()
            item_loader.add_value("deposit", deposit)
        
        bathroom_count = response.xpath("//li[contains(.,'Bathroom')]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split(" ")[0])
        
        desc = " ".join(response.xpath("//div[@class='property__main']//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        if "floor" in desc:
            item_loader.add_value("floor", desc.split("floor")[0].strip().split(" ")[-1])
        
        latitude_longitude = response.xpath("//div/@data-latlng").get()
        if latitude_longitude:
            item_loader.add_value("latitude", latitude_longitude.split(",")[0])
            item_loader.add_value("longitude", latitude_longitude.split(",")[1])
        
        import dateparser
        available_date = response.xpath("//time/@datetime").get()
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        furnished = response.xpath("//li[contains(.,'Furnished') or contains(.,' furnished')]/text()").get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        images = [x for x in response.xpath("//div[@class='gallery-item']//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        item_loader.add_value("landlord_name", "ABODE")
        item_loader.add_value("landlord_phone", "0113 274 8142")
        
        yield item_loader.load_item()

