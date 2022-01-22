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
    name = 'bhhslondonproperties_co'
    execution_type='testing'
    country='united_kingdom'
    locale='en'

    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.bhhslondonproperties.com/property-lettings/apartments-available-to-rent-in-london",
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "https://www.bhhslondonproperties.com/property-lettings/houses-available-to-rent-in-london"
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
        
        for item in response.xpath("//h4/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
        
        next_page = response.xpath("//a[.='Next']/@href").get()
        if next_page:
            yield Request(response.urljoin(next_page), callback=self.parse, meta={"property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", "Bhhslondonproperties_PySpider_united_kingdom")
        item_loader.add_value("external_id", response.url.split("/")[-1])

        title = response.xpath("//h1//text()").get()
        if title:
            item_loader.add_value("title", title)

        address = response.xpath("//h1//text()").get()
        if address:
            address = address.split(" in ")[1]
            city = address.split(",")[-2].strip()
            zipcode = address.split(",")[-1].strip()
            item_loader.add_value("address", address)
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode)

        rent = response.xpath("//div[contains(@class,'address-wrapper')]//span[contains(.,'month')]//span//text()").get()
        if rent:
            rent = rent.strip().replace("£","").replace(",",".")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "GBP")

        desc = " ".join(response.xpath("//div[contains(@class,'overview-content')]//h4//following-sibling::p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        else:
            desc = " ".join(response.xpath("//div[contains(@class,'overview-content')]//h4//text()").getall())
            if desc:
                desc = re.sub('\s{2,}', ' ', desc.strip())
                item_loader.add_value("description", desc)

        room_count = response.xpath("//h1//text()[contains(.,'bedroom')]").get()
        if room_count:
            room_count = room_count.strip().split(" ")[0]
            item_loader.add_value("room_count", room_count)
        else:
            room_count = response.xpath("//h1//text()[contains(.,'Studio')]").get()
            if room_count:
                item_loader.add_value("room_count", "1")
        
        images = [x for x in response.xpath("//ul[contains(@class,'slides')]//li//@src").getall()]
        if images:
            item_loader.add_value("images", images)
         
        floor_plan_images = response.xpath("//div[@id='floorplan']//@src").get()
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)

        parking = response.xpath("//ul[contains(@class,'attributes')]//li[contains(@class,'Garage')]//text()").get()
        if parking:
            item_loader.add_value("parking", True)
        else:
            parking = response.xpath("//li[contains(@class,'Parking')]//text()").get()
            if parking:
                item_loader.add_value("parking", True)

        balcony = response.xpath("//li[contains(@class,'Balcony')]//text()").get()
        if balcony:
            item_loader.add_value("balcony", True)

        elevator = response.xpath("//li[contains(@class,'Lift')]//text()").get()
        if elevator:
            item_loader.add_value("elevator", True)

        floor = response.xpath("//li[contains(@class,'Floor')]//text()").get()
        if floor and not "upper" in floor.lower():
            floor = floor.strip().split(" ")[0]
            item_loader.add_value("floor", floor.strip())

        latitude_longitude = response.xpath('//script[contains(.,"lat\':")]//text()').get()
        if latitude_longitude:
            latitude = latitude_longitude.split("lat':")[1].split(',')[0].strip() 
            longitude = latitude_longitude.split("lng':")[1].split(',')[0].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        item_loader.add_value("landlord_name", "BERKSHIRE HATHAWAY HOMESERVICES KAY & CO")
        
        landlord_phone = response.xpath("//p[contains(@class,'office contacts')]//a//@href").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone.split(":")[1])

        yield item_loader.load_item()