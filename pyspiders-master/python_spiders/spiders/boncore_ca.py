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
    name = 'boncore_ca'
    execution_type = 'testing' 
    country='canada'
    locale='en'
    external_source = 'Boncore_PySpider_canada'
    post_urls = "https://api.theliftsystem.com/v2/search?locale=en&client_id=611&auth_token=sswpREkUtyeYjeoahA2i&city_id=1979&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=0&max_rate=2200&min_sqft=0&max_sqft=10000&show_custom_fields=true&show_promotions=true&region=&keyword=false&property_types=apartments%2C+houses%2C+commercial&ownership_types=&exclude_ownership_types=&custom_field_key=&custom_field_values=&order=min_rate+ASC&limit=66&neighbourhood=&amenities=&promotions=&city_ids=3107%2C1978%2C2870%2C1979&pet_friendly=&offset=0&count=false"  
    
    headers = {    
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "tr-TR,tr;q=0.9,en-GB;q=0.8,en;q=0.7,en-US;q=0.6",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36",
    }
    payload = {
        "locale": "en",
        "client_id": "611",
        "auth_token": "sswpREkUtyeYjeoahA2i",
        "city_id": "1979",
        "geocode": "",
        "min_bed": "-1",
        "max_bed": "100",
        "min_bath": "0",
        "max_bath": "10",
        "min_rate": "0",
        "max_rate": "2200",
        "min_sqft": "0",
        "max_sqft": "10000",
        "show_custom_fields": "true",
        "show_promotions": "true",
        "region":"",
        "keyword": "false",
        "property_types": "apartments, houses, commercial",
        "ownership_types": "",
        "exclude_ownership_types": "",
        "custom_field_key": "",
        "custom_field_values": "",
        "order": "min_rate ASC",
        "limit": "66",
        "neighbourhood": "",
        "amenities": "",
        "promotions": "",
        "city_ids": "3107,1978,2870,1979",
        "pet_friendly": "",
        "offset": "0",
        "count": "false",
    }
    
    def start_requests(self): 

        yield Request(
            self.post_urls,
            callback=self.parse,
            body=json.dumps(self.payload),
            headers=self.headers,
        )
    
    # 1. FOLLOWING
    def parse(self, response):

        data = json.loads(response.body)
        for item in data:
            follow_url = item["permalink"]
            id_values = str(int(item["id"]))
            address = item["address"]["address"]
            city = item["address"]["city"]
            zipcode = item["address"]["postal_code"]
            property_type = item["property_type"]
            name = item["name"]
            latitude = item["geocode"]["latitude"]
            longitude = item["geocode"]["longitude"]
            phone =item["contact"]["phone"]
            email = item["contact"]["email"]
            landlord_name = item["contact"]["name"]
            yield Request(
                        follow_url, 
                        callback=self.populate_item, 
                        meta={
                            "item":item,
                            "id_values":id_values,
                            "address":address,
                            "city":city,
                            "zipcode":zipcode,
                            "property_type":property_type,
                            "name":name,
                            "latitude":latitude,
                            "longitude":longitude,
                            "phone":phone,
                            "email":email,
                            "landlord_name":landlord_name,
                        })

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item = response.meta.get("item")

        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_link", response.url)
        
        property_type = response.meta.get("property_type")
        if property_type and "apartment" in property_type.lower():
            item_loader.add_value("property_type","apartment")
        else:
            item_loader.add_value("property_type","house")

        id_values = response.meta.get("id_values")
        if id_values:
            item_loader.add_value("external_id", id_values)

        title = response.meta.get("name")
        if title:
            item_loader.add_value("title", title)

        address = response.meta.get("address")
        if address:
            item_loader.add_value("address", address)

        city = response.meta.get("city")
        if city:
            item_loader.add_value("city", city)

        zipcode = response.meta.get("zipcode")
        if zipcode:
            item_loader.add_value("zipcode", zipcode)

        description = response.xpath("//div[contains(@class,'main')]/p/text()").getall()
        if description:
            item_loader.add_value("description",description)

        price = response.xpath("//span[contains(.,'Rent')]/following-sibling::span/text()").get()
        if price:
            item_loader.add_value("rent", price)
        item_loader.add_value("currency", "USD")

        room_count = response.xpath("//span[contains(.,'Bedrooms')]/following-sibling::span/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)

        bathroom_count = response.xpath("//span[contains(.,'Bathrooms')]/following-sibling::span/text()").get()
        if bathroom_count and "." in bathroom_count:
            bathroom_count = bathroom_count.split(".")[0]
        item_loader.add_value("bathroom_count",bathroom_count)
        
        latitude = response.meta.get("latitude")
        if latitude:
            item_loader.add_value("latitude", latitude)

        longitude = response.meta.get("longitude")
        if longitude:
            item_loader.add_value("longitude", longitude)

        images = [x for x in response.xpath("//a[@rel='property']/@href").getall()]
        if images:
            item_loader.add_value("images", images)  

        landlord_name = response.meta.get("landlord_name")
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name)

        phone = response.meta.get("phone")
        if phone:
            item_loader.add_value("landlord_phone", phone)
   
        email = response.meta.get("email")
        if email:
            item_loader.add_value("landlord_email", email)
        yield item_loader.load_item()
