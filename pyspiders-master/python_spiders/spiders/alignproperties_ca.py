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
    name = 'alignproperties_ca'
    execution_type='testing'
    country='canada'
    locale='en'
    external_source="Alignproperties_PySpider_canada"
    def start_requests(self):
        start_urls = "https://api.theliftsystem.com/v2/search?locale=en&client_id=463&auth_token=sswpREkUtyeYjeoahA2i&city_id=2619&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=0&max_rate=1500&min_sqft=0&max_sqft=10000&show_custom_fields=true&show_promotions=true&local_url_only=true&region=&keyword=false&property_types=apartments%2Chouses&ownership_types=&exclude_ownership_types=&custom_field_key=&custom_field_values=&order=min_rate+ASC&limit=66&neighbourhood=&amenities=&promotions=&city_ids=408%2C845%2C2619&pet_friendly=&offset=0&count=false"

        yield Request(start_urls, callback=self.parse)
    def parse(self, response):
        jresp = json.loads(response.body)
        for item in jresp:
            prop_type = item["property_type"]
            url = item["permalink"]
            
            if get_p_type_string(prop_type):
                p_type = get_p_type_string(prop_type)
                yield Request(url, callback=self.populate_item, meta={"property_type": p_type, "item": item})

     
    # 2. SCRAPING level 2
    def populate_item(self, response):
        
        item = response.meta.get("item")
        ext_id = item["id"]
        title = item["name"]
        available_date = item["min_availability_date"]
        address = item["address"]["address"]
        city = item["address"]["city"]
        postal_code = item["address"]["postal_code"]
        province_code = item["address"]["province_code"]
        pet_friendly = item["pet_friendly"] #true
        details = item["details"]["overview"]
        latitude = item["geocode"]["latitude"]
        longitude = item["geocode"]["longitude"]
        bedrooms = item["statistics"]["suites"]["bedrooms"]["min"]
        bathrooms = item["statistics"]["suites"]["bathrooms"]["min"]
        rates = item["statistics"]["suites"]["rates"]["min"]
        square_feet = item["statistics"]["suites"]["square_feet"]["min"]

        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", str(ext_id))
        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("title",title)
        item_loader.add_value("address", "{}, {}, {}".format(address,city,province_code))
        item_loader.add_value("city", city)
        item_loader.add_value("zipcode", postal_code)
        item_loader.add_value("rent", int(float(rates)))
        item_loader.add_value("currency", "USD")
        item_loader.add_value("room_count", int(float(bedrooms)))
        item_loader.add_value("bathroom_count", int(float(bathrooms)))

        if square_feet:
            sqm=str(int(float(square_feet)* 0.09290304))
            item_loader.add_value("square_meters", sqm)

        if available_date:
            date_parsed = dateparser.parse( available_date, date_formats=["%m-%d-%Y"] )
            if dateparser:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))

        images = [x for x in response.xpath("//section[@class='gallery loading']//a[@rel='property']/@href").getall()]
        if images:
            item_loader.add_value("images", images)

        item_loader.add_xpath("latitude", latitude)
        item_loader.add_xpath("longitude", longitude)

        if details:
            item_loader.add_value("description",details.strip())

        if pet_friendly:
            item_loader.add_value("pets_allowed",True)

        parking = "".join(response.xpath("//div[@class='amenity-holder']//text()[contains(.,'Garage') or contains(.,'garage') or contains(.,'parking')]").extract())
        if parking:
            item_loader.add_value("parking",True)

        elevator = "".join(response.xpath("//div[@class='amenity-holder']//text()[contains(.,'Elevator') or contains(.,'elevator')]").extract())
        if elevator:
            item_loader.add_value("elevator",True)
        balcony = "".join(response.xpath("//div[@class='amenity-holder']//text()[contains(.,'balcony') or contains(.,'Balcony')]").extract())
        if balcony:
            item_loader.add_value("balcony",True)

        dishwasher = "".join(response.xpath("//div[@class='amenity-holder']//text()[contains(.,'Dishwasher') or contains(.,'dishwasher')]").extract())
        if dishwasher:
            item_loader.add_value("dishwasher",True)

        item_loader.add_value("landlord_name", "ALIGN PROPERTY MANAGEMENT")
        item_loader.add_value("landlord_phone", "(844) 832-7668")
        
        yield item_loader.load_item()
def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("apartment" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower()):
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    else:
        return None