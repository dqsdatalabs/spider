# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json

class MySpider(Spider):

    name = 'coronis_com_au'
    execution_type='testing'
    country='australia'
    locale='en' 
    external_source="Coronis_Com_PySpider_australia"
    custom_settings={
        "PROXY_AU_ON":True,
        "HTTPCACHE_ENABLED":True
    }
    headers ={
        "Accept": "*/*",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36",
    }
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.coronis.com.au/wp-json/api/listings/all?type=rental&status=current&paged=1&limit=12",
                ]
            },
	
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                yield Request(
                    url=item,
                    callback=self.parse,
                    headers=self.headers,
                )
                
    # 1. FOLLOWING
    def parse(self, response):

        item_loader = ListingLoader(response=response)

        data = json.loads(response.body)

        item_loader.add_value("external_source", self.external_source)

        json_data = data["result"][0]
        for item in json_data:
            detail_url= item["slug"]
            if detail_url:
                item_loader.add_value("external_link", detail_url)

            property_type= item["propertyCategory"]
            if property_type:
                item_loader.add_value("property_type", property_type)

            external_id =item["uniqueID"]
            if external_id:
                item_loader.add_value("external_id", external_id)

            title =item["title"]
            if title:
                item_loader.add_value("title", title)

            address =item["fullAddress"]
            if address:
                item_loader.add_value("address", address)

            city= item["address"]["suburb"]
            if city:
                item_loader.add_value("city", city)

            zipcode= item["address"]["postcode"]
            if zipcode:
                item_loader.add_value("zipcode", zipcode)
                
            description= item["propertyHeading"]
            if description:
                item_loader.add_value("description", description)
                
            room_count= item["propertyBed"]
            if room_count:
                item_loader.add_value("room_count", room_count)

            bathroom_count= item["propertyBath"]
            if bathroom_count:
                item_loader.add_value("bathroom_count", bathroom_count)

            square_meters= item["landDetails"]["value"]
            if square_meters and square_meters!=0:
                item_loader.add_value("square_meters", square_meters)

            rent= item["propertyPricing"]["value"]
            if rent and "week" in rent.lower():
                rent = rent.split("$")[1].split(" ")[0]
                rent = int(float(rent)*4)
                item_loader.add_value("rent", rent)
            else:
                rent = rent.split("$")[1].split(" ")[0]
                item_loader.add_value("rent", rent)
            item_loader.add_value("currency", "USD")

            longitude_latitude= item["propertyCoords"]
            if longitude_latitude:
                latitude = longitude_latitude.split(",")[0]
                longitude = longitude_latitude.split(",")[1]
                item_loader.add_value("latitude", latitude) 
                item_loader.add_value("longitude", longitude) 

            images= item["propertyImage"]["featured"]
            if images:
                item_loader.add_value("images", images)
                
            landlord_name = item["agent"][0]["name"]
            if landlord_name:
                item_loader.add_value("landlord_name", landlord_name) 

            landlord_phone = item["agent"][0]["phone"]
            if landlord_phone:
                item_loader.add_value("landlord_phone", landlord_phone) 

        yield item_loader.load_item()