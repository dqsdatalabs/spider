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
    name = 'estate_agents_portsmouth_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://estate-agents-portsmouth.co.uk/lettings/properties?branch=&type=flat&min-rent=&max-rent=&min-bedrooms=&max-bedrooms=",
                ],
                "property_type" : "apartment"
            },
            {
                "url" : [
                    "https://estate-agents-portsmouth.co.uk/lettings/properties?branch=&type=house&min-rent=&max-rent=&min-bedrooms=&max-bedrooms=",
                ],
                "property_type" : "house"
            },   
            {
                "url" : [
                    "https://estate-agents-portsmouth.co.uk/lettings/properties?branch=&type=studio&min-rent=&max-rent=&min-bedrooms=&max-bedrooms=",
                ],
                "property_type" : "studio"
            },
            {
                "url" : [
                    "https://estate-agents-portsmouth.co.uk/lettings/properties?branch=&type=room&min-rent=&max-rent=&min-bedrooms=&max-bedrooms=",
                ],
                "property_type" : "room"
            },   
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@id='properties']//h1/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={'property_type': response.meta["property_type"]})
        
        next_button = response.xpath("//a[contains(.,'Next ›')]/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse, meta={'property_type': response.meta["property_type"]})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_source", "Estate_Agents_Portsmouth_Co_PySpider_united_kingdom")
        
        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", re.sub('\s{2,}', ' ', title.strip()))

        external_id = response.xpath("//a[contains(.,'reference')]/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split("reference")[1].strip())

        address = response.xpath("//h1/small/text()").get()
        if address:
            item_loader.add_value("address", address)
            zipcode = address.split(" ")[-1]
            item_loader.add_value("zipcode",zipcode)
            
            city = address.split(zipcode)[0].strip().split(" ")[-1]
            if "street" in city.lower() or "end" in city.lower():
                item_loader.add_value("city", f"{address.split(zipcode)[0].strip().split(' ')[-1]} {address.split(zipcode)[0].strip().split(' ')[-2]}")
            else:
                item_loader.add_value("city", city)
        
        rent = response.xpath("//span[@class='price']/text()").get()
        if rent:
            rent = rent.strip().split(" ")[0].replace(",","").replace("£","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "GBP")
        
        room_count = response.xpath("//li[@class='bedroom-count']/text()").get()
        if room_count:
            room_count = room_count.split(":")[1].strip()
            item_loader.add_value("room_count", room_count)
        else:
            room_count = response.xpath("//li[@class='reception-room-count']/text()").get()
            if room_count:
                room_count = room_count.split(":")[1].strip()
                item_loader.add_value("room_count", room_count)
        
        bathroom_count = response.xpath("//li[@class='bathroom-count']/text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.split(":")[1].strip()
            item_loader.add_value("bathroom_count", room_count)
        
        floor = response.xpath("//li[contains(.,'Floor')]//text()").get()
        if floor:
            floor = floor.split("Floor")[0].strip().split(" ")[-1]
            if floor:
                item_loader.add_value("floor", floor.split("Floor")[0].strip().split(" ")[-1])

        furnished = response.xpath("//li[contains(.,'Furnished') or contains(.,' furnished')]//text()").get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        elevator = response.xpath("//li[contains(.,'Lift')]//text()").get()
        if elevator:
            item_loader.add_value("elevator", True)
        
        balcony = response.xpath("//li[contains(.,'Balcon')]//text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        parking = response.xpath("//li[contains(.,'Parking')]//text()").get()
        if parking:
            item_loader.add_value("parking", True)
        
        import dateparser
        available_date = response.xpath("//li[contains(.,'Available:')]/time/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        description = " ".join(response.xpath("//div[@id='property-details']//p//text()").getall())
        if description:
            item_loader.add_value("description", re.sub('\s{2,}', ' ', description.strip()))
        
        images = [x.split("(")[1].split(")")[0] for x in response.xpath("//div[@id='property-images']//@style[contains(.,'url')]").getall()]
        if images:
            item_loader.add_value("images", images)
        
        energy_label = response.xpath("//img[contains(@alt,'Energy')]/@src").get()
        if energy_label:
            if "currentenergy" in energy_label:
                energy_label = energy_label.split("currentenergy=")[1].split("&")[0]
                item_loader.add_value("energy_label", energy_label)
        
        item_loader.add_value("landlord_name", "Letting Agents Portsmouth")
        item_loader.add_value("landlord_phone", "02392 658044")

        yield item_loader.load_item()