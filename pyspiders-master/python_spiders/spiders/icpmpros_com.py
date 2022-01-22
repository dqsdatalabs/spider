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

class MySpider(Spider):
    name = 'icpmpros_com'
    execution_type = 'testing'
    country = 'canada'
    locale = 'en'
    external_source = 'Icpmpros_PySpider_canada'
    
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://icpmpros.managebuilding.com/Resident/public/rentals?hidenav=true",
                ],
                "property_type": "house"
            },     
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                yield Request(
                    url=item,
                    callback=self.parse,
                    meta={'property_type': url.get('property_type')}
                )

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//a[contains(@class,'featured-listing')]"):
            base_url = "https://icpmpros.managebuilding.com"
            follow_url = item.xpath("./@href").get()
            follow_url = base_url + follow_url

            rent = item.xpath("./@data-rent").get()
            room = item.xpath("./@data-bedrooms").get()
            bath = item.xpath("./@data-bathrooms").get()
            cityzip = item.xpath("./@data-location").get()
   
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type'), "rent":rent, "room":room, "bath":bath, "cityzip":cityzip})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
  
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_link", response.url)
        property_type = response.meta.get('property_type')
        item_loader.add_value("property_type", property_type)
        
        external_id = response.url
        if external_id:
            item_loader.add_value("external_id", external_id.split("?")[0].split("/")[-1])

        title = response.xpath("//h1[contains(@class,'title')]/text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title.strip())
        
        rent = response.meta.get('rent')
        if rent:
            item_loader.add_value("rent", rent.strip())
        item_loader.add_value("currency", "USD")
        
        room_count = response.meta.get('room')
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        
        bathroom_count = response.meta.get('bath')
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())  
        
        address = response.xpath("//h1[contains(@class,'title')]/text()").get()
        if address:
            address = re.sub('\s{2,}', ' ', address.strip())
            item_loader.add_value("address", address.strip())
           
        cityzip = response.meta.get('cityzip')
        if cityzip:
            city = cityzip.split(",")[0].strip()
            item_loader.add_value("city", city)
            zipcode = cityzip.split("|")[1].strip()
            if zipcode:
                item_loader.add_value("zipcode", zipcode)

        available_date = response.xpath("//div[contains(@class,'available')]/text()").get()
        if available_date:
            available_date = available_date.split("Available")[1].strip()
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))

        desc = " ".join(response.xpath("//p[@class='unit-detail__description']/text()").getall())
        if desc:
            description = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", description.strip())
            if description and "parking" in description.lower():
                item_loader.add_value("parking", True)
            if description and "dishwasher" in description.lower():
                item_loader.add_value("dishwasher", True)
        
        pets_allowed = response.xpath("//li/text()[contains(.,'Pet friendly')]").get()
        if pets_allowed:
            item_loader.add_value("pets_allowed", True)

        washing_machine = response.xpath("//li/text()[contains(.,'washer')]").get()
        if washing_machine:
            item_loader.add_value("washing_machine", True)

        dishwasher = response.xpath("//li/text()[contains(.,'Dishwasher')]").get()
        if dishwasher:
            item_loader.add_value("dishwasher", True)

        images = [x for x in response.xpath("//ul[@class='js-gallery unseen']/li/@data-mfp-src").getall()]
        if images:
            item_loader.add_value("images", images)

        item_loader.add_value("landlord_name", "Investors Choice Property Management")
        item_loader.add_value("landlord_phone", "(+1) 888-641-8889")
        item_loader.add_value("landlord_email", "info@icpmpros.com")
        
        yield item_loader.load_item()