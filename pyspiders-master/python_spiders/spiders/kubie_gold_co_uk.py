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
import dateparser


class MySpider(Spider):
    name = 'kubie_gold_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en' 
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.kubie-gold.co.uk/properties/?address_keyword=&department=residential-lettings&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=&minimum_bedrooms=&property_type=24",
                ],
                "property_type" : "apartment"
            },
            {
                "url" : [
                    "https://www.kubie-gold.co.uk/properties/?address_keyword=&department=residential-lettings&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=&minimum_bedrooms=&property_type=11",
                ],
                "property_type" : "house"
            },   
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[contains(@class,'feature-info')]/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
        
        next_page = response.xpath("//a[contains(@class,'next')]/@href").get()
        if next_page:
            yield Request(response.urljoin(next_page), callback=self.parse, meta={'property_type': response.meta.get('property_type')})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Kubiegold_PySpider_"+ self.country + "_" + self.locale)

        prop_type = response.xpath("//li[contains(.,'STUDIO')]/text()").get()
        if prop_type:
            item_loader.add_value("property_type", "studio")
        else:
            item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        title = response.xpath("//title/text()").get()
        item_loader.add_value("title", title)
        
        address = response.xpath("//span[contains(@class,'sub-title')]/text()").get()
        count = address.count(",")
        if address and count > 1:
            item_loader.add_value("address", address)
            item_loader.add_value("city", address.split(",")[1])
            item_loader.add_value("zipcode", address.split(",")[-1])
        elif address and count == 1:
            item_loader.add_value("address", address)
            item_loader.add_value("city", address.split(",")[0].strip())
            item_loader.add_value("zipcode", address.split(",")[-1])
        else:
            item_loader.add_value("address", title)
            item_loader.add_value("zipcode", address)
        
        rent = response.xpath("//h2[@class='title-price']/text()").get()
        if rent and "pw" in rent:
            price = rent.split("pw")[0].split("£")[1].replace("\t","").replace(",","").strip()
            item_loader.add_value("rent",str(int(price)*4))
        item_loader.add_value("currency", "GBP")
        
        room_count = response.xpath("//li/span[@class='bed']/parent::li/span/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        elif prop_type:
            item_loader.add_value("room_count", "1")
        bathroom_count = response.xpath("//li/span[@class='bathrooms']/parent::li/span/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        
        floor_plan_images = response.xpath("//li/a[contains(@id,'floor')]/@href").get()
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)
        
        desc = response.xpath("//div[contains(@class,'content')]/p/text()").get()
        if desc:
            item_loader.add_value("description", desc)
        
        # if "sqft" in desc:
        #     square_meters = desc.split("sqft")[0].split("(")[1].strip()
        #     item_loader.add_value("square_meters", square_meters)
        
        lat_lng = response.xpath("//script[contains(.,'Lat')]/text()").get()
        if lat_lng:
            lat = lat_lng.split("LatLng(")[1].split(",")[0]
            lng = lat_lng.split("LatLng(")[1].split(",")[1].split(")")[0]
            item_loader.add_value("latitude", lat)
            item_loader.add_value("longitude", lng)
        
        images = [x for x in response.xpath("//div[@class='images']/a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
            
        elevator = response.xpath("//div[contains(@class,'content')]//ul/li[contains(.,'LIFT')]/text()").get()
        if elevator:
            item_loader.add_value("elevator", True)
            if "FLOOR" in elevator:
                floor = elevator.split("FLOOR")[0]
                item_loader.add_value("floor", floor)
        
        parking = response.xpath("//div[contains(@class,'content')]//ul/li[contains(.,'PARKING')]/text()").get()
        if parking:
            item_loader.add_value("parking", True)
            
        swimming_pool = response.xpath("//div[contains(@class,'content')]//ul/li[contains(.,'POOL')]/text()").get()
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)
        
        furnished = response.xpath("//div[contains(@class,'content')]//ul/li[contains(.,'FURNISHED')]/text()").get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        available_date = response.xpath("//div[contains(@class,'content')]//ul/li[contains(.,'AVAILAB')]/text()").get()
        if available_date and "IMMEDIATELY" not in available_date:
            available_d = available_date.replace("AVAILABLE","").replace("AVAILABALE","").replace("END","").strip()
            date = "{} {}".format(available_d, datetime.now().year)
            try:
                date_parsed = dateparser.parse(date, date_formats=["%d-%m-%Y"])
            except:
                date_parsed = dateparser.parse(date, date_formats=["%m-%Y"])
            if date_parsed:
                date3 = date_parsed.strftime("%Y-%m-%d")
                current_date = str(datetime.now())
                if current_date > date3:
                    date = datetime.now().year + 1
                    parsed = date3.replace("2020",str(date))
                    item_loader.add_value("available_date", parsed)
                else:
                    item_loader.add_value("available_date", date3)
    
        
        item_loader.add_value("landlord_name", "KUBIE GOLD ASSOCIATES")
        item_loader.add_value("landlord_phone", "020 3936 8047")
        item_loader.add_value("landlord_email", "info@kubie-gold.co.uk")
        
        
        
        yield item_loader.load_item()
