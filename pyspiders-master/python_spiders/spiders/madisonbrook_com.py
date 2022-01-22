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
from datetime import datetime
import re

class MySpider(Spider):
    name = 'madisonbrook_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en' 
    external_source="Madisonbrook_PySpider_united_kingdom"
    start_urls = ["https://madisonbrook.com/property-search/?department=residential-lettings&address_keyword=&price_range=&rent_range=&property_type=&minimum_bedrooms=&minimum_bathrooms="]

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//div[contains(@class,'prop-box-content')]"):
            status = item.xpath("./div[@class='prop-box-info']/p/text()").get()
            if status and "agreed" in status.lower().strip():
                continue
            follow_url = response.urljoin(item.xpath("./a/@href").get())
            yield Request(follow_url, callback=self.populate_item)
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source",self.external_source)

        item_loader.add_value("external_link", response.url)

        features = " ".join(response.xpath("//div[@class='features']/ul/li/text()").getall())
        if get_p_type_string(features):
            item_loader.add_value("property_type", get_p_type_string(features))
        else:
            return

        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", re.sub('\s{2,}', ' ', title.strip()))
        
        address = response.xpath("//div[contains(@class,'sub-title')]/div[contains(@class,'icon-house')]/parent::div/h2/text()").get()
        if address:
            
            if "," in address:
                zipcode = address.split(",")[-1].replace("Lewisham","").strip()
                if "Western" not in zipcode and "Yeo" not in zipcode:
                    city = address.split(zipcode)[0].strip().strip(",").split(",")[-1]
                    item_loader.add_value("zipcode", zipcode)
                    item_loader.add_value("city", city.strip())
            else:
                zipcode = address.split(" ")[-1]
                if not zipcode.isalpha():
                    item_loader.add_value("zipcode", zipcode)
                    item_loader.add_value("city", address.split(zipcode)[0].strip())
            item_loader.add_value("address", address)

        rent = response.xpath("//div[contains(@class,'prop-title-value')]/h2/text()").get()
        if rent:
            price = rent.split("pcm")[0].split("£")[1].strip().replace(",","")
            item_loader.add_value("rent", price)
        item_loader.add_value("currency", "GBP")
        
        room_count = response.xpath("//li/span[contains(.,'Bedroom')]/parent::li/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        else:
            room_count = response.xpath("//li/span[contains(.,'Reception')]/parent::li/text()").get()
            item_loader.add_value("room_count", room_count.strip())
            
        bathroom_count = response.xpath("//li/span[contains(.,'Bathroom')]/parent::li/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())
        
        square_meters = response.xpath("//div[@class='features']//li[contains(.,'Sq')]/text()").get()
        if square_meters:
            square_meters = square_meters.split(" ")[0].replace(",","")
            if square_meters.isdigit():
                sqm = str(int(int(square_meters)* 0.09290304))
                item_loader.add_value("square_meters", sqm)        
        
        external_id = response.xpath("//li/span[contains(.,'Ref')]/parent::li/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.strip())
            
        available_date = response.xpath("//div[@class='features']//li[contains(.,'Available')]/text()").get()
        if available_date:
            if "immediately" in available_date.lower() or "Now" in available_date:
                available_date = datetime.now()
                item_loader.add_value("available_date", available_date.strftime("%Y-%m-%d"))
            else:
                available_date = available_date.split("Available")[1].replace(":","").replace("from","").strip()
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
        
        floor = response.xpath("//div[@class='features']//li[contains(.,'Floor')]/text()").get()
        if floor:
            floor = floor.split("Floor")[0].strip()
            # if "Ground" not in floor:
            item_loader.add_value("floor", floor)
        
        desc = "".join(response.xpath("//div[@class='summary-contents']//text()").getall())
        if desc:
            item_loader.add_value("description", re.sub('\s{2,}', ' ', desc))
        
        images = [ x for x in response.xpath("//a[@rel='gallery']/@href").getall()]
        if images:
            item_loader.add_value("images", images)
        
        latitude_longitude = response.xpath("//script[contains(.,'LatLng')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split("LatLng(")[1].split(",")[0]
            longitude = latitude_longitude.split("LatLng(")[1].split(",")[1].split(")")[0].strip()
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)
        
        elevator = response.xpath("//div[@class='features']//li[contains(.,'Lift') or contains(.,'lift')]/text()").get()
        if elevator:
            item_loader.add_value("elevator", True)

        furnished = response.xpath("//div[@class='features']/ul/li[contains(.,'Furnished') or contains(.,'furnished')]/text()").get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        terrace = response.xpath("//div[@class='features']//li[contains(.,'Terrace') or contains(.,'terrace')]/text()").get()
        if terrace:
            item_loader.add_value("terrace", True)
        
        parking = response.xpath(
            "//div[@class='features']//li[contains(.,'Parking') or contains(.,'parking') or contains(.,'Garage') or contains(.,'garage')]/text()"
            ).get()
        if parking:
            item_loader.add_value("parking", True)

        item_loader.add_value("landlord_name", "MADISON BROOK INTERNATIONAL")
        item_loader.add_value("landlord_phone", "+44 (0)20 3946 6100")
        item_loader.add_value("landlord_email", "info@madisonbrook.com")
        
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and "house" in p_type_string.lower():
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    else:
        return None
