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
    name = 'hupropertygroup_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    external_source = "Hupropertygroup_Com_PySpider_australia"
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "http://www.hupropertygroup.com.au/rent?search=&listing_type=rent&property_type=Unit&bedrooms=&bathrooms=&min_rent=0&min_price=0&max_rent=&max_price=",
                    "http://www.hupropertygroup.com.au/rent?search=&listing_type=rent&property_type=Apartment&bedrooms=&bathrooms=&min_rent=0&min_price=0&max_rent=&max_price=",
                    "http://www.hupropertygroup.com.au/rent?search=&listing_type=rent&property_type=Duplex&bedrooms=&bathrooms=&min_rent=0&min_price=0&max_rent=&max_price=",
                    "http://www.hupropertygroup.com.au/rent?search=&listing_type=rent&property_type=Flat&bedrooms=&bathrooms=&min_rent=0&min_price=0&max_rent=&max_price=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "http://www.hupropertygroup.com.au/rent?search=&listing_type=rent&property_type=House&bedrooms=&bathrooms=&min_rent=0&min_price=0&max_rent=&max_price=",
                    "http://www.hupropertygroup.com.au/rent?search=&listing_type=rent&property_type=Townhouse&bedrooms=&bathrooms=&min_rent=0&min_price=0&max_rent=&max_price=",
                    "http://www.hupropertygroup.com.au/rent?search=&listing_type=rent&property_type=Semi-detached&bedrooms=&bathrooms=&min_rent=0&min_price=0&max_rent=&max_price=",
                    "http://www.hupropertygroup.com.au/rent?search=&listing_type=rent&property_type=Terrace&bedrooms=&bathrooms=&min_rent=0&min_price=0&max_rent=&max_price=",
                    "http://www.hupropertygroup.com.au/rent?search=&listing_type=rent&property_type=Villa&bedrooms=&bathrooms=&min_rent=0&min_price=0&max_rent=&max_price=",
                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "http://www.hupropertygroup.com.au/rent?search=&listing_type=rent&property_type=Studio&bedrooms=&bathrooms=&min_rent=0&min_price=0&max_rent=&max_price=",
                ],
                "property_type" : "studio"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//a[@class='card']/@href").getall():           
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_id", response.url.split("id=")[1])
        
        title = response.xpath("//title//text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.split("|")[0].strip())
            item_loader.add_value("title", title)
        
        address = response.xpath("//div[@class='summaryItem']/div[@class='address']/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", address.split(",")[-1].strip())
        
        room_count = response.xpath("//li/div[contains(.,'Bedroom')]/text()").get()
        if room_count:
            room_count = room_count.split(" ")[0]
            item_loader.add_value("room_count", room_count)
        
        bathroom_count = response.xpath("//li/div[contains(.,'Bathroom')]/text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.split(" ")[0]
            item_loader.add_value("bathroom_count", bathroom_count)
        
        rent = response.xpath("//div[@class='listPrice']/text()[contains(.,'$')]").get()
        if rent:
            rent = rent.split("$")[1].split("p")[0]
            item_loader.add_value("rent", int(float(rent))*4)
        item_loader.add_value("currency", "AUD")
        
        parking = response.xpath("//li/div[contains(.,'Car')]/text()[not(contains(.,'0'))] | //span[contains(@class,'check')]/following-sibling::text()[contains(.,'Parking') or contains(.,'Garage')]").get()
        if parking:
            item_loader.add_value("parking", True)
        
        swimming_pool = response.xpath("//span[contains(@class,'check')]/following-sibling::text()[contains(.,'Dishwasher')]").get()
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)
        
        description = " ".join(response.xpath("//div[@class='description']//p//text()").getall())
        if description:
            item_loader.add_value("description", description.strip())
        
        images = [x for x in response.xpath("//div[@class='carousel-inner']//@href").getall()]
        if images:
            item_loader.add_value("images", images)
        
        latitude_longitude = response.xpath("//script[contains(.,'lng\"')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('lat":')[1].split(',')[0]
            longitude = latitude_longitude.split('lng":')[1].split('}')[0].strip()
            if latitude !="null":
                item_loader.add_value("longitude", longitude)
                item_loader.add_value("latitude", latitude)
        
        item_loader.add_value("landlord_name", "H&U Property Group")
        item_loader.add_value("landlord_phone", "07 3883 4906")
        item_loader.add_value("landlord_email", "reception@hupropertygroup.com.au")
        
        yield item_loader.load_item()