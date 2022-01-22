# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser
import re
class MySpider(Spider):
    name = 'buckleyrealestate_ie'
    execution_type='testing'
    country='ireland'
    locale='en'
    external_source = "Buckleyrealestate_PySpider_ireland"
 
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://buckleyrealestate.ie/daft-residential-lettings-listings",
                ],
            }
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                )

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[contains(@class,'daft-listings')]/div[contains(@data-org,'6')]"):
            follow_url = item.xpath(".//h4/a/@href").get()

            if item.xpath(".//@data-bedrooms").get():
                room_count = item.xpath(".//@data-bedrooms").get()
            if item.xpath(".//@data-bathrooms").get():
                bathroom_count = item.xpath(".//@data-bathrooms").get()
            if item.xpath(".//@data-city").get():
                city = item.xpath(".//@data-city").get()

            yield Request(follow_url, callback=self.populate_item, meta={"room_count":room_count, "bathroom_count":bathroom_count, "city":city})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)

        prop_type = response.xpath("//p[@data-testid='property-type']/text()").get()
        if prop_type and "apartment" in prop_type.lower():
            item_loader.add_value("property_type", "apartment")
        elif prop_type and "house" in prop_type.lower():
            item_loader.add_value("property_type", "house")
        else:
            return

        title = response.xpath("//h1/text()").get()
        if title:
            item_loader.add_value("title", title)
        
        external_id = response.url
        if external_id:
            item_loader.add_value("external_id", external_id.split("/")[-1])

        rent = response.xpath("//span/text()[contains(.,'€')]").get()
        if rent and "per month" in rent.lower():
            item_loader.add_value("rent", rent.split(" per")[0].replace("€", "").replace(",", ""))
        item_loader.add_value("currency", "EUR")
        
        bedroom = response.meta.get("room_count")
        if bedroom:
            item_loader.add_value("room_count", bedroom)
        
        bathroom = response.meta.get("bathroom_count")
        if bathroom:
            item_loader.add_value("bathroom_count", bathroom)

        city = response.meta.get("city")
        if city:
            item_loader.add_value("city", city)

        address = response.xpath("//h1/text()").get()
        if address:
            item_loader.add_value("address", address)

        description = "".join(response.xpath("//div[@data-testid='description']/text()").getall())
        if description:
            description = re.sub('\s{2,}', ' ', description.replace("\r","").replace("\n","").strip())
            item_loader.add_value("description", description)

        furnished = response.xpath("(//span[contains(.,'Furnished')]/following-sibling::text())[2]").get()
        if furnished and "yes" in furnished.lower():
            item_loader.add_value("furnished", True)
        
        parking = response.xpath("//li[contains(@class,'PropertyDetailsList')]/text()[contains(.,'Parking')]").get()
        if parking:
            item_loader.add_value("parking", True)
        
        balcony = response.xpath("//li[contains(@class,'PropertyDetailsList')]/text()[contains(.,'Balcony')]").get()
        if parking:
            item_loader.add_value("balcony", True)

        washing_machine = response.xpath("//li[contains(@class,'PropertyDetailsList')]/text()[contains(.,'Washing')]").get()
        if washing_machine:
            item_loader.add_value("washing_machine", True)

        dishwasher = response.xpath("//li[contains(@class,'PropertyDetailsList')]/text()[contains(.,'Dishwasher')]").get()
        if dishwasher:
            item_loader.add_value("dishwasher", True)

        images_script = response.xpath("//script[contains(text(),'size1200x1200')]").get()
        if images_script:
            images = re.findall('"size1200x1200":"([^,]+)","', images_script)
            item_loader.add_value("images",images)

        position = response.xpath("//a[@aria-label='Satellite View']/@href").get()
        if position:
            lat = position.split("+")[0].split("loc:")[-1]
            long = position.split("+")[-1]
            item_loader.add_value("latitude",lat)
            item_loader.add_value("longitude",long)
        
        item_loader.add_value("landlord_name", "Buckley Real Estate")
        item_loader.add_value("landlord_phone", "+353 1 9633333")
        item_loader.add_value("landlord_email", "info@buckleyrealestate.ie")
        
        yield item_loader.load_item()