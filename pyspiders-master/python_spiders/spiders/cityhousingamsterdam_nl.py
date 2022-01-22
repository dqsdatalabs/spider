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
import dateparser
from datetime import datetime

class MySpider(Spider):
    name = 'cityhousingamsterdam_nl'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'en'
    external_source='Cityhousingamsterdam_PySpider_netherlands'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.cityhousingamsterdam.nl/rent-listings?city=&house_type=Apartment&min_price=&max_price=&range=&surface=&bedrooms=&interior=&location=&available_at_date-datepicker=&available_at_date=",
                    "https://www.cityhousingamsterdam.nl/rent-listings?city=&house_type=Groundfloor+apartment&min_price=&max_price=&range=&surface=&bedrooms=&interior=&location=&available_at_date-datepicker=&available_at_date=",
                    "https://www.cityhousingamsterdam.nl/rent-listings?city=&house_type=Upstairs+apartment&min_price=&max_price=&range=&surface=&bedrooms=&interior=&location=&available_at_date-datepicker=&available_at_date=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.cityhousingamsterdam.nl/rent-listings?city=&house_type=Tussenwoning&min_price=&max_price=&range=&surface=&bedrooms=&interior=&location=&available_at_date-datepicker=&available_at_date=",
                ],
                "property_type" : "house",
            },
            {
                "url" : [
                    "https://www.cityhousingamsterdam.nl/rent-listings?city=&house_type=Studio&min_price=&max_price=&range=&surface=&bedrooms=&interior=&location=&available_at_date-datepicker=&available_at_date=",
                ],
                "property_type" : "studio",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//a[@class='grey-border']"):
            status = item.xpath(".//p[@class='label']/text()").get()
            if status and "verhuurd" in status.lower():
                continue
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

        next_page = response.xpath("//li[contains(@class,'next ')]/a/@href").get()
        if next_page:
            yield Request(
                response.urljoin(next_page),
                callback=self.parse,
                meta={"property_type":response.meta["property_type"]}
            )    
        
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Cityhousingamsterdam_PySpider_netherlands")
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        
        title = response.xpath("//title//text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)   
        
        address = "".join( response.xpath("//div[contains(@class,'left-column')]//following-sibling::h1//text()").getall())
        if address:
            address = re.sub('\s{2,}', ' ', address.strip())
            item_loader.add_value("address", address)
            item_loader.add_value("city", address.split(" ")[-1])
        
        rent = response.xpath("//li/span[contains(.,'Prij')]/following-sibling::span/text()").get()
        price = ""
        if rent:
            price = rent.split(",-")[0].split("€")[1].strip().replace(".","")
            item_loader.add_value("rent", price)
            item_loader.add_value("currency", "EUR")
        
        square_meters = response.xpath("//h2[contains(.,'m²')]/text()").get()
        if square_meters:
            square_meters = square_meters.split("m²")[0].split("•")[1].strip()
            item_loader.add_value("square_meters", square_meters)
        
        room_count = response.xpath("//li/span[contains(.,'Slaapkamer')]/following-sibling::span/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        
        desc = " ".join(response.xpath("//p[contains(@class,'description')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        if "floor" in desc:
            floor = desc.split("floor")[0].strip().split(" ")[-1]
            if "Laminate" not in floor:
                item_loader.add_value("floor", floor)
            
        if "deposit" in desc:
            deposit = desc.split("deposit")[0].strip().split("&")[-1].strip().split(" ")[0]
            if price:
                deposit = int(deposit)*int(price)
                item_loader.add_value("deposit", deposit)
        
        images = [x for x in response.xpath("//div[contains(@class,'slide')]/@data-image").getall()]
        if images:
            item_loader.add_value("images", images)
        
        available_date = response.xpath("//li/span[contains(.,'Beschik')]/following-sibling::span/text()").get()
        if available_date:
            if "Direct" in available_date:
                item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
        
        furnished = response.xpath("//li/span[contains(.,'Interieur')]/following-sibling::span/text()").get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        latitude = response.xpath("//div[contains(@class,'pane')]//@data-lat").get()
        if latitude:
            item_loader.add_value("latitude", latitude)
        
        longitude = response.xpath("//div[contains(@class,'pane')]//@data-lng").get()
        if longitude:
            item_loader.add_value("longitude", longitude)
        
        item_loader.add_value("external_id", response.url.split("-")[-1])
        
        item_loader.add_value("landlord_name", "City Housing Amsterdam")
        item_loader.add_value("landlord_phone", "020- 2600019")
        item_loader.add_value("landlord_email", "info@cityhousingamsterdam.nl")
              
        yield item_loader.load_item()