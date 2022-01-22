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
import re

class MySpider(Spider):
    name = 'harcourtssolutions_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    external_source = "Harcourtssolutions_Com_PySpider_australia"
    
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.harcourtssolutions.com.au/wp-json/api/listings/all?priceRange=&category=Apartment%2CUnit&type=rental&status=current&paged=1&limit=16&address=&bed=&bath=&car=&sort=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.harcourtssolutions.com.au/wp-json/api/listings/all?priceRange=&category=House%2CTownhouse%2CVilla&type=rental&status=current&paged=1&limit=16&address=&bed=&bath=&car=&sort="
                ],
                "property_type" : "house"
            },
            {
                "url": [
                    "https://www.harcourtssolutions.com.au/wp-json/api/listings/all?priceRange=&category=Studio&type=rental&status=current&paged=1&limit=16&address=&bed=&bath=&car=&sort="
                ],
                "property_type": "studio"
            }

        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        data = json.loads(response.body)
        if "results" in data:
            for item in data["results"]:
                yield Request(item["slug"], callback=self.populate_item, meta={"property_type":response.meta["property_type"], "data": item})
                seen = True
        
        if page == 2 or seen:
            next_page = response.url.replace(f"paged={page-1}", f"paged={page}")
            yield Request(next_page, callback=self.parse, meta={"property_type":response.meta["property_type"], "page": page+1})
            

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        data = response.meta.get('data')
        item_loader.add_value("external_id", data["uniqueID"])
        item_loader.add_value("external_source", self.external_source)

        item_loader.add_value("title", data["title"])

        rent = data["propertyPricing"]["value"]
        if rent:
            if "$" in rent:
                rent = rent.strip().split(" ")[0].replace("$","").replace(",","")
            item_loader.add_value("rent", int(float(rent))*4)
        item_loader.add_value("currency", "AUD")
        
        
        item_loader.add_value("room_count", data["propertyBed"])
        item_loader.add_value("bathroom_count", data["propertyBath"])
        
        available_date = data["propertyPricing"]["availabilty"]
        if available_date and "from" in available_date:
            available_date = available_date.split("from")[1].strip()
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
    
        parking = data["propertyParking"]
        if parking and parking != 0:
            item_loader.add_value("parking", True)
        
        item_loader.add_value("address", data["address"]["suburb"])
        item_loader.add_value("city", data["address"]["suburb"])

        desc = " ".join(response.xpath("//div[contains(@class,'single-listing__content')]//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        images = data["propertyImage"]["listImg"]
        for i in images:
            item_loader.add_value("images", i)
    
        item_loader.add_value("landlord_name", data["agent"]["name"])

        item_loader.add_value("landlord_phone", data["agent"]["phone"])
        
        item_loader.add_value("landlord_email", "solutions@harcourtssolutions.com.au")

        yield item_loader.load_item()