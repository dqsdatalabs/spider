# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import re

class MySpider(Spider):
    name = 'fntamworth_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
 
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.fntamworth.com.au/rent?search=&listing_type=rent&property_type=Apartment&bedrooms=&bathrooms=&min_rent=0&min_price=0&max_rent=&max_price=",
                    "https://www.fntamworth.com.au/rent?search=&listing_type=rent&property_type=Duplex&bedrooms=&bathrooms=&min_rent=0&min_price=0&max_rent=&max_price=",
                    "https://www.fntamworth.com.au/rent?search=&listing_type=rent&property_type=Flat&bedrooms=&bathrooms=&min_rent=0&min_price=0&max_rent=&max_price=",

                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.fntamworth.com.au/rent?search=&listing_type=rent&property_type=House&bedrooms=&bathrooms=&min_rent=0&min_price=0&max_rent=&max_price=",
                    "https://www.fntamworth.com.au/rent?search=&listing_type=rent&property_type=Townhouse&bedrooms=&bathrooms=&min_rent=0&min_price=0&max_rent=&max_price=",
                    "https://www.fntamworth.com.au/rent?search=&listing_type=rent&property_type=Semi-detached&bedrooms=&bathrooms=&min_rent=0&min_price=0&max_rent=&max_price=",
                    "https://www.fntamworth.com.au/rent?search=&listing_type=rent&property_type=Terrace&bedrooms=&bathrooms=&min_rent=0&min_price=0&max_rent=&max_price=",
                    "https://www.fntamworth.com.au/rent?search=&listing_type=rent&property_type=Villa&bedrooms=&bathrooms=&min_rent=0&min_price=0&max_rent=&max_price=",
                    "https://www.fntamworth.com.au/rent?search=&listing_type=rent&property_type=Unit&bedrooms=&bathrooms=&min_rent=0&min_price=0&max_rent=&max_price="
                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "https://www.fntamworth.com.au/rent?search=&listing_type=rent&property_type=Studio&bedrooms=&bathrooms=&min_rent=0&min_price=0&max_rent=&max_price=",
                    
                ],
                "property_type" : "studio"
            }
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        for url in response.xpath("//li[@class='col-lg-4 col-md-6']/div/a/@href").getall():
            yield Request(response.urljoin(url), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
   
    # 2. SCRAPING level 2
    def populate_item(self, response): 
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Fntamworth_PySpider_australia")
        item_loader.add_value("external_id", response.url.split("_id=")[-1])

        title =response.xpath("//title//text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        address = response.xpath("//div[contains(@class,'property-address')]//text()").get()
        if address:
            city = address.split(",")[-1].strip()
            item_loader.add_value("address", address)
            item_loader.add_value("city", city)

        rent = response.xpath("//div[contains(@class,'property-header')]//div[contains(@class,'meta')]//span//text()").get()
        if rent:
            if "pw" in rent.lower() or "week" in rent.lower():
                if "-" in rent:
                    rent = rent.split("-")[0].split("$")[1].split(".")[0]
                else:
                    rent = rent.replace("pw","").split("$")[1]
                rent = int(rent)*4
            else:
                rent = rent.split("$")[1].split(".")[0]
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "AUD")

        desc = " ".join(response.xpath("//div[contains(@id,'main-content')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        room_count = response.xpath("//i[contains(@class,'bed')]//parent::div//following-sibling::div//text()").get()
        if room_count:
            room_count = room_count.strip().split(" ")[0]
            item_loader.add_value("room_count", room_count)

        bathroom_count = response.xpath("//i[contains(@class,'tint')]//parent::div//following-sibling::div//text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.strip().split(" ")[0]
            item_loader.add_value("bathroom_count", bathroom_count)
        
        images = [x for x in response.xpath("//ul[contains(@class,'slides')]//@data-image").getall()]
        if images:
            item_loader.add_value("images", images)
         
        floor_plan_images = response.xpath("//div[contains(@id,'floor-plan')]//@src").get()
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)
        
        from datetime import datetime
        import dateparser
        available_date = "".join(response.xpath("//div[contains(@id,'main-content')]//text()[contains(.,'Available')]").getall())
        if available_date:
            available_date = available_date.split("Available")[1].replace(":","").strip()
            if not "now" in available_date.lower():
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)

        parking = response.xpath("//i[contains(@class,'car')]//parent::div//following-sibling::div//text()").get()
        if parking:
            item_loader.add_value("parking", True)

        balcony = response.xpath("//div[contains(@id,'main-content')]//text()[contains(.,'Balcony')]").get()
        if balcony:
            item_loader.add_value("balcony", True)
            
        dishwasher = response.xpath("//section[contains(@id,'property-features')]//li[contains(.,'Dishwasher')]//text()").get()
        if dishwasher:
            item_loader.add_value("dishwasher", True)

        latitude_longitude = response.xpath("//script[contains(.,'L.marker')]//text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('L.marker([')[1].split(',')[0]
            longitude = latitude_longitude.split('L.marker([')[1].split(",")[1].split(']')[0].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        item_loader.add_value("landlord_name", "First National Real Estate | Tamworth")
        item_loader.add_value("landlord_email", "reception@fntamworth.com.au")
        item_loader.add_value("landlord_phone", "02 6766 6122")

        yield item_loader.load_item()