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
class MySpider(Spider):
    name = 'annafieldestates_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    thousand_separator = ','
    scale_separator = '.'   
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://annafieldestates.co.uk/search-results/?department=residential-lettings&address_keyword=&radius=&availability=6&minimum_bedrooms=&minimum_bathrooms=&property_type=22&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=",
                    "https://annafieldestates.co.uk/search-results/?department=residential-lettings&address_keyword=&radius=&availability=8&minimum_bedrooms=&minimum_bathrooms=&property_type=22&minimum_price=&maximum_price=&minimum_rent=&maximum_rent="
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://annafieldestates.co.uk/search-results/?department=residential-lettings&address_keyword=&radius=&availability=6&minimum_bedrooms=&minimum_bathrooms=&property_type=18&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=",
                    "https://annafieldestates.co.uk/search-results/?department=residential-lettings&address_keyword=&radius=&availability=6&minimum_bedrooms=&minimum_bathrooms=&property_type=9&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=",
                    "https://annafieldestates.co.uk/search-results/?department=residential-lettings&address_keyword=&radius=&availability=8&minimum_bedrooms=&minimum_bathrooms=&property_type=9&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=",
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
        for url in response.xpath("//div[@class='details']/h3/a/@href").extract():
            yield Request(url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", "Annafieldestates_Co_PySpider_united_kingdom")          
        item_loader.add_xpath("rent_string", "//div[@class='price']/text()")

        address = response.xpath("//div[contains(@class,'element-6e65dd7 ')]//h2/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
            zipcode = address.split(",")[-1].strip()
            if "," in address:
                if zipcode.replace(" ","").isalpha():
                    item_loader.add_value("city", address.split(",")[-1].strip())
                else:
                    item_loader.add_value("zipcode", zipcode)
                    item_loader.add_value("city", address.split(",")[-2].strip())
        room_count = response.xpath("//div[@class='summary']/div//text()[contains(.,'Bedroom')]").get()
        if room_count:
            room_count = room_count.split("Bedroom")[0].replace("Double ","").strip().split(" ")[-1]
            if room_count.isdigit():
                item_loader.add_value("room_count", room_count.strip())
        available_date = response.xpath("//div[@class='summary']/div//text()[contains(.,'Available')]").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.split("Available")[1].split(".")[0].strip(), date_formats=["%d %m %Y"])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))
        description = " ".join(response.xpath("//div[@class='summary']/div//text()").getall()) 
        if description:
            item_loader.add_value("description", description.strip())
     
        images = [x for x in response.xpath("//div[@id='slider']//li/a/@href[.!='#']").getall()]
        if images:
            item_loader.add_value("images", images)

        latitude_longitude = response.xpath("//script[contains(.,' google.maps.LatLng(')]//text()").get()
        if latitude_longitude:
            latitude_longitude = latitude_longitude.split(' google.maps.LatLng(')[1].split(')')[0]
            item_loader.add_value("latitude", latitude_longitude.split(",")[0].strip())
            item_loader.add_value("longitude", latitude_longitude.split(",")[1].strip())
        
        item_loader.add_value("landlord_name", "Annafield Estates")
        item_loader.add_value("landlord_phone", "01480 587 121")
        item_loader.add_value("landlord_email", "enquiries@annafieldestates.co.uk")
        yield item_loader.load_item()

