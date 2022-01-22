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
    name = 'cassidyandtate_co_uk' 
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    external_source="Cassidyandtate_Co_PySpider_united_kingdom"
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://cassidyandtate.co.uk/properties-search/?property_view=rent&src_add_or_post=&min_bedrooms=empty&max_bedrooms=empty&property_type%5B%5D=Apartment&min_price=empty&max_price=empty&property_view1=empty&order_result=empty&street=empty&added_in=empty&submit=Search",
                  
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "https://cassidyandtate.co.uk/properties-search/?property_view=rent&src_add_or_post=&min_bedrooms=empty&max_bedrooms=empty&property_type%5B%5D=House+-+detached&property_type%5B%5D=House+-+semi-detached&property_type%5B%5D=House+-+end+terrace&property_type%5B%5D=House+-+terraced&min_price=empty&max_price=empty&property_view1=empty&order_result=empty&street=empty&added_in=empty&submit=Search",
                    "https://cassidyandtate.co.uk/properties-search/?property_view=rent&src_add_or_post=&min_bedrooms=empty&max_bedrooms=empty&property_type%5B%5D=Bungalow&min_price=empty&max_price=empty&property_view1=empty&order_result=empty&street=empty&added_in=empty&submit=Search",
                    "https://cassidyandtate.co.uk/properties-search/?property_view=rent&src_add_or_post=&min_bedrooms=empty&max_bedrooms=empty&property_type%5B%5D=New+Homes&min_price=empty&max_price=empty&property_view1=empty&order_result=empty&street=empty&added_in=empty&submit=Search"

                ],
                "property_type": "house"
            }
            
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
        
        for item in response.xpath("//div[contains(@class,'panel-desc')]"): 
            url = item.xpath(".//h3/a/@href").get()
            status = item.xpath(".//p/small[contains(.,'Let Agreed')]/text()").get()
            if status:
                continue
            yield Request(url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
        
        next_page = response.xpath("//div[@class='pagination']//a[.='Next']/@href").get()
        if next_page: 
            yield Request(response.urljoin(next_page), callback=self.parse, meta={"property_type": response.meta.get('property_type')})

# 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)        
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_id", response.url.split("propId=")[1])

        title = response.xpath("//h3[contains(@class,'property_title')]//text()").get()
        if title:
            item_loader.add_value("title", title)

        address = response.xpath("//h3[contains(@class,'property_title')]//text()").get()
        if address:
            city = address.split(",")[-1].split("-")[0].strip()
            zipcode = address.split("-")[-1].strip()
            item_loader.add_value("address", address)
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode)

        rent = response.xpath("//div[contains(@class,'prop_price')]//text()").get()
        if rent:
            rent = rent.split(" ")[0].replace(",","").replace("£","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "GBP")

        desc = " ".join(response.xpath("//h3[contains(@class,'property_description')]//following-sibling::p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        room_count = response.xpath("//span[contains(@class,'bedrooms_count')]//text()").get()
        if room_count:
            room_count = room_count.strip().split(" ")[0]
            item_loader.add_value("room_count", room_count)

        bathroom_count = response.xpath("//span[contains(@class,'bathrooms_count')]//text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.strip().split(" ")[0]
            item_loader.add_value("bathroom_count", bathroom_count)
        
        images = [x for x in response.xpath("//ul[contains(@id,'image-gallery')]//@src[not(contains(.,'.pdf'))]").getall()]
        if images:
            item_loader.add_value("images", images)
         
        floor_plan_images = response.xpath("//a[contains(@title,'Floor Plan')]//@href").get()
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)
        
        from datetime import datetime
        import dateparser
        available_date = response.xpath("//li[contains(.,'Available')]//text()").get()
        if available_date:
            available_date = available_date.split("Available")[1].replace("Beginning","").replace("mid","").strip()
            if not "now" in available_date.lower():
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)

        parking = response.xpath("//li[contains(.,'Parking')]//text()").get()
        if parking:
            item_loader.add_value("parking", True)

        balcony = response.xpath("//li[contains(.,'Balcony')]//text()").get()
        if balcony:
            item_loader.add_value("balcony", True)

        furnished = response.xpath("//li[contains(.,'Furnished') or contains(.,'furnished')]//text()[not(contains(.,'Unfurnished'))]").get()
        if furnished:
            item_loader.add_value("furnished", True)

        floor = response.xpath("//li[contains(.,'Floor')]//text()").get()
        if floor:
            floor = floor.split("Floor")[0].strip()
            item_loader.add_value("floor", floor.strip())

        latitude_longitude = response.xpath("//script[contains(.,'lat =')]//text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('lat =')[1].split(';')[0].strip() 
            longitude = latitude_longitude.split('lon =')[1].split(';')[0].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        item_loader.add_value("landlord_name", "Cassidy & Tate")
        item_loader.add_value("landlord_phone", "01727 228428")
        item_loader.add_value("landlord_email", "stalbans@cassidyandtate.co.uk")
        
        yield item_loader.load_item()