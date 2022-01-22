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
from python_spiders.helper import ItemClear
import re

class MySpider(Spider):
    name = 'prdnewcastle_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    custom_settings = {"PROXY_TR_ON": True}
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.prdnewcastle.com.au/wp-json/api/listings/all?status=current&type=rental&paged=1&priceRange=&category=Apartment%2CFlat&isProject=false&limit=999&author=&bed=0&bath=0&sort=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.prdnewcastle.com.au/wp-json/api/listings/all?status=current&type=rental&paged=1&priceRange=&category=House%2CUnit%2CTownhouse%2CVilla%2CDuplexSemi-detached&isProject=false&limit=999&author=&bed=0&bath=0&sort=",
                ],
                "property_type" : "house",
            },
            {
                "url" : [
                    "https://www.prdnewcastle.com.au/wp-json/api/listings/all?status=current&type=rental&paged=1&priceRange=&category=Studio&isProject=false&limit=999&author=&bed=0&bath=0&sort=",
                ],
                "property_type" : "studio",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        data = json.loads(response.body)
        if "results" in data:
            for item in data["results"]:
                follow_url = item["slug"]
                yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"],"item":item})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Prdnewcastle_Com_PySpider_australia")   

        item = response.meta.get('item')
        item_loader.add_value("external_id", str(item["uniqueID"])) 
        item_loader.add_value("room_count", str(item["propertyBed"])) 
        item_loader.add_value("bathroom_count", str(item["propertyBath"])) 
        item_loader.add_value("title", item["title"]) 
        item_loader.add_value("city", item["address"]["suburb"]) 
        item_loader.add_value("address", item["title"]) 
        zipcode=response.xpath("//title//text()").get()
        if zipcode:
            zipcode=zipcode.split("-")[0].strip().split(" ")[-2:]
            item_loader.add_value("zipcode",zipcode)
        item_loader.add_value("latitude", item["propertyCoords"].split(",")[0])
        item_loader.add_value("longitude", item["propertyCoords"].split(",")[1].strip())
    
        available_date = response.xpath("//h3[.='Availability']/following-sibling::div[1]/text()").get()
        if available_date:
            if "Available Now" in available_date:
                item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
            else:
                date_parsed = dateparser.parse(available_date.split(" from ")[-1], date_formats=["%d/%m/%Y"])
                if date_parsed:
                    item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))
       
        parking = str(item["propertyParking"])
        if parking:
            if "0" in parking:
                item_loader.add_value("parking", False)
            else:
                item_loader.add_value("parking", True)
    
        rent =  item["propertyPricing"]["propertyRent"]
        if rent:
            item_loader.add_value("rent", int(float(rent))*4)
        item_loader.add_value("currency", "AUD")
        desc = "".join(response.xpath("//div[contains(@class,'listing-single-content')]/p//text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())

        images = [x for x in response.xpath("//div[@class='listing-single-slider']/div/img/@data-lazy").getall()]
        if images:
            item_loader.add_value("images", images)
        features="".join(response.xpath("//p[contains(.,'Features')]/text()").getall())
        if features:
            if "furnished" in features.lower():
                item_loader.add_value("furnished",True)

        item_loader.add_xpath("landlord_name", "//div[@class='agent-container dflex'][last()]//h3/a/text()")
        item_loader.add_xpath("landlord_phone", "//div[@class='agent-container dflex'][last()]//span[@class='btn-phone-label-number']/text()")
        
        yield item_loader.load_item()
