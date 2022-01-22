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
    name = 'michielstolker_nl'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'en'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.michielstolker.nl/?widget_id=2&sf_unit_price=150&sf_select_property_type=6",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.michielstolker.nl/?widget_id=2&sf_unit_price=150&sf_select_property_type=15",
                    "https://www.michielstolker.nl/?widget_id=2&sf_unit_price=150&sf_select_property_type=16",
                    "https://www.michielstolker.nl/?widget_id=2&sf_unit_price=150&sf_select_property_type=7",
                ],
                "property_type" : "house",
            },
            {
                "url" : [
                    "https://www.michielstolker.nl/?widget_id=2&sf_unit_price=150&sf_select_property_type=17",
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
        for item in response.xpath("//a[@class='view_detail' and @itemprop]"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

        next_page = response.xpath("//li[@class='next']/a/@href").get()
        if next_page:
            yield Request(
                response.urljoin(next_page),
                callback=self.parse,
                meta={"property_type":response.meta["property_type"]}
            )    
        
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Michielstolker_PySpider_netherlands")
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        
        title = response.xpath("//title/text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        
        street = response.xpath("//div[contains(.,'Street :')]/span/text()").get()
        city = response.xpath("//div[contains(.,'city :')]/span/text()").get()
        if city or street:
            if street and city:
                item_loader.add_value("address", street+" "+city)
            elif street:
                item_loader.add_value("address", street)
            else:
                item_loader.add_value("address", city)
            item_loader.add_value("city", city)
        
        zipcode = response.xpath("//div[contains(.,'Postal')]/span/text()").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode)
        
        rent = response.xpath("//div[contains(.,'Price :')]/span/text()").get()
        price = ""
        if rent:
            price = rent.split("Per")[0].strip().split("€")[1].strip().replace(",","")
            item_loader.add_value("rent", price)
            item_loader.add_value("currency","EUR")
        
        room_count = response.xpath("//div[contains(.,'Bedrooms :')]/span/text()").get()
        if room_count and room_count!="0":
            item_loader.add_value("room_count", room_count)
        elif response.meta.get('property_type') =="studio":
            item_loader.add_value("room_count","1")
            
        bathroom_count = response.xpath("//div[contains(.,'Bathrooms :')][not(contains(.,'Half'))]/span/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        
        square_meters = response.xpath("//div[contains(.,'Built Up')]/span/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split("m")[0].strip())
        
        external_id = response.xpath("//div[contains(.,'ID :')]/span/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id)
        
        floor = response.xpath("//div[contains(.,'Floor Number :')]/span/text()").get()
        if floor:
            item_loader.add_value("floor", floor)
        
        latitude = response.xpath("//div[contains(.,'Latitude :')]/span/text()").get()
        if latitude:
            item_loader.add_value("latitude", latitude)
        
        longitude = response.xpath("//div[contains(.,'Longitude :')]/span/text()").get()
        if longitude:
            item_loader.add_value("longitude", longitude)
        
        desc = " ".join(response.xpath("//div[@itemprop='description']//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        images = [x for x in response.xpath("//ul[@class='bxslider']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        
        pets_allowed = response.xpath("//div[contains(.,'Pet Policy')]/span/text()").get()
        if pets_allowed:
            if "Not" in pets_allowed:
                item_loader.add_value("pets_allowed", False)
            elif "Allowed" in pets_allowed:
                item_loader.add_value("pets_allowed", True)
        
        furnished = response.xpath("//div[contains(.,'Furnished :')]/span/text()").get()
        if furnished:
            if "Not" in furnished:
                item_loader.add_value("furnished", False)
            elif "Furnished" in furnished:
                item_loader.add_value("furnished", True)
        
        if "Deposit:" in desc:
            deposit = desc.split("Deposit:")[1].strip().split(" ")[0]
            if deposit.isdigit():
                deposit = int(deposit)*int(price)
                item_loader.add_value("deposit", deposit)
        
        if "available immediately" in desc:
            item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
        
        dishwasher = response.xpath("//div[contains(.,'Dishwasher')]//text()").get()
        if dishwasher:
            item_loader.add_value("dishwasher", True)
        
        washing_machine = response.xpath("//div[contains(.,'Washing Machine')]//text()").get()
        if washing_machine:
            item_loader.add_value("washing_machine", True)
        
        balcony = response.xpath("//div[contains(.,'Balcon')]//text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        terrace = response.xpath("//div[contains(.,'Roof Deck')]//text()").get()
        if terrace:
            item_loader.add_value("terrace", True)
        
        item_loader.add_xpath("landlord_name", "//li[@class='name']//text()")
        item_loader.add_xpath("landlord_phone", "//li[@itemprop='telephone']//text()")
        item_loader.add_value("landlord_email", "info@michielstolker.nl")
        
        yield item_loader.load_item()