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
from datetime import datetime

class MySpider(Spider):
    name = 'bricksnstones_be'
    execution_type='testing'
    country='belgium'
    locale='nl'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.bricksnstones.be/api/estates",
                ],
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item, callback=self.parse)


    # 1. FOLLOWING
    def parse(self, response):
        data = json.loads(response.body)
        for item in data["data"]:
            if "group" in item and "koop" in item["group"]:
                continue
            if "statusLabel" in item and "verhuurd" in item["statusLabel"]:
                continue
            if "category" in item and item["category"] == "appartement":
                prop_type = "apartment"
            elif "category" in item and item["category"] == "huis":
                prop_type = "house"
            else:
                continue
            items = item
            follow_url = item["url"]
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":prop_type, "items": items})
        
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Bricksnstones_PySpider_belgium")
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        items = response.meta.get("items")
        
        item_loader.add_value("title", items['title'])
        item_loader.add_value("room_count", items['bedrooms'])
        item_loader.add_value("bathroom_count", items['bathrooms'])
        item_loader.add_value("external_id", items['reference'])
        item_loader.add_value("latitude", items['latitude'])
        item_loader.add_value("longitude", items['longitude'])
        item_loader.add_value("rent", items['price'].split("€")[1].strip())
        item_loader.add_value("currency", "EUR")
        item_loader.add_value("images", items['images'])
        
        address = items["title"]
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", address.strip().split(" ")[-1].strip())
            item_loader.add_value("zipcode", address.strip().split(" ")[0].strip())

        square_meters = response.xpath("//div[contains(@class,'column')][contains(.,'Woonoppervlakte')]/following-sibling::div/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split("m")[0].strip())
        
        available_date = response.xpath("//div[contains(@class,'column')][contains(.,'Beschikbaarheid')]/following-sibling::div/text()").get()
        if available_date:
            if "onmiddellijk" in available_date:
                item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
        
        floor = response.xpath("//div[contains(@class,'column')][contains(.,'Beschikbaarheid')]/following-sibling::div/text()").get()
        if floor:
            item_loader.add_value("floor", floor.strip())
        
        terrace = response.xpath("//div[contains(@class,'column')][contains(.,'Terras')]/following-sibling::div/text()").get()
        if terrace:
            if "nee" in terrace.lower():
                item_loader.add_value("terrace", False)
            elif "ja" in terrace.lower():
                item_loader.add_value("terrace", True)
            
        elevator = response.xpath("//div[contains(@class,'column')][contains(.,'Lift')]/following-sibling::div/text()").get()
        if elevator:
            if "nee" in elevator.lower():
                item_loader.add_value("elevator", False)
            elif "ja" in elevator.lower():
                item_loader.add_value("elevator", True)
        
        desc = " ".join(response.xpath("//div[@class='text']//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc)
            item_loader.add_value("description", desc)
        
        energy_label = response.xpath("//div[contains(@class,'column')][contains(.,'EPC waarde')]/following-sibling::div/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.strip())
        
        item_loader.add_value("landlord_name", "Bricks 'n Stones")
        item_loader.add_value("landlord_phone", "32 3 203 00 33")

        landlord_email = response.xpath("//a[@class='estate-sidebar__representative-social-link']/@href").get()
        if landlord_email: item_loader.add_value("landlord_email", landlord_email.split(':')[-1].strip())
      
        yield item_loader.load_item()