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
    name = 'bradleyhall_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.bradleyhall.co.uk/residential-properties/?property_use_id=2&sale_type_id=1&location_text=&radius=&location_id=&property_type_id=2&min_price=&max_price=&min_bedrooms=&max_bedrooms=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.bradleyhall.co.uk/residential-properties/?property_use_id=2&sale_type_id=1&location_text=&radius=&location_id=&property_type_id=1&min_price=&max_price=&min_bedrooms=&max_bedrooms=",
                    "https://www.bradleyhall.co.uk/residential-properties/?property_use_id=2&sale_type_id=1&location_text=&radius=&location_id=&property_type_id=4&min_price=&max_price=&min_bedrooms=&max_bedrooms=",
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})
    
    def parse(self, response):

        for item in response.xpath("//div[contains(@class,'main-property-card')]"):
            follow_url = response.urljoin(item.xpath(".//a/@href").get())
            let_available = item.xpath(".//span[contains(@class,'available')]").get()
            if let_available: yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta["property_type"]})

        next_button = response.xpath("//a[contains(.,'Next')]/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse, meta={"property_type": response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_source", "Bradleyhall_Co_PySpider_united_kingdom")

        title = " ".join(response.xpath("//h1//text()").getall())
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        address = response.xpath("//h1//text()").get()
        if address:
            city = address.strip().split(" ")[-1]
            item_loader.add_value("address", address)
            item_loader.add_value("city", city)

        rent = response.xpath("//strong[contains(.,'£')]//text()").get()
        if rent:
            rent = rent.strip().split("£")[1].replace(",","").strip().split(" ")[0]
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "GBP")

        desc = " ".join(response.xpath("//div[contains(@class,'wysiwyg')]//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        room_count = response.xpath("//i[contains(@class,'bed')]//parent::span/strong//text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)

        bathroom_count = response.xpath("//i[contains(@class,'bath')]//parent::span/strong//text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        
        images = [x for x in response.xpath("//div[contains(@id,'product-gallery-feature-image')]//@href").getall()]
        if images:
            item_loader.add_value("images", images)
         
        floor_plan_images = response.xpath("//div[contains(@id,'property-floor-plans')]//@href").get()
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)

        parking = response.xpath("//span[contains(.,'Garage')]//text()").get()
        if parking:
            item_loader.add_value("parking", True)

        furnished = response.xpath("//span[contains(.,'furnished') or contains(.,'Furnished')]//text()").get()
        if furnished:
            item_loader.add_value("furnished", True)

        floor = response.xpath("//div[contains(@class,'custom-bullets')]//span[contains(.,'Floor')]//text()").get()
        if floor:
            floor = floor.strip().split(" ")[0]
            item_loader.add_value("floor", floor)
        
        energy_label = response.xpath("//img[contains(@class,'epc-rating-image')]//@src").get()
        if energy_label:
            energy_label = energy_label.split("currentenergy=")[1].split("&")[0]
            item_loader.add_value("energy_label", energy_label)

        latitude = response.xpath("//input[contains(@class,'latitude')]//@value").get()
        if latitude:     
            item_loader.add_value("latitude", latitude)

        longitude = response.xpath("//input[contains(@class,'longitude')]//@value").get()
        if longitude:     
            item_loader.add_value("longitude", longitude)

        landlord_name = response.xpath("//h2[contains(.,'Arrange a viewing')]//following-sibling::div/p//text()").get()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name)

        landlord_email = response.xpath("//h2[contains(.,'Arrange a viewing')]//following-sibling::div//a[contains(@href,'mailto')]//@href").get()
        if landlord_email:
            landlord_email = landlord_email.split(":")[1].split("?")[0]
            item_loader.add_value("landlord_email", landlord_email)
        
        landlord_phone = response.xpath("//h2[contains(.,'Arrange a viewing')]//following-sibling::div//a[contains(@href,'tel')]//@href").get()
        if landlord_phone:
            landlord_phone = landlord_phone.split(":")[1]
            item_loader.add_value("landlord_phone", landlord_phone)

        yield item_loader.load_item()