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
    name = 'wwre_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.wwre.com.au/wp-json/api/listings/all?priceRange=&category=Apartment&type=rental&status=current&limit=16&address=&bed=&bath=&car=&sort=&paged=1",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.wwre.com.au/wp-json/api/listings/all?priceRange=0%2C5000&category=House%2CTownhouse&type=rental&status=current&limit=16&address=&bed=&bath=&car=&sort=&paged=1",
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

        page = response.meta.get("page", 2)
        seen = False

        data = json.loads(response.body)
        if data["status"].upper() == 'SUCCESS':
            seen = True
            for item in data["results"]:           
                yield Request(response.urljoin(item["slug"]), callback=self.populate_item, meta={"property_type":response.meta["property_type"], "item":item})

        if page == 2 or seen: 
            yield Request(response.url.split('&paged=')[0] + f"&paged={page}", callback=self.parse, meta={"property_type":response.meta["property_type"], "page":page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Wwre_Com_PySpider_australia")
        
        item = response.meta.get('item')
        
        item_loader.add_value("title", item["title"])
        item_loader.add_value("address", item["title"]+ " " + item["address"]["suburb"])
        item_loader.add_value("city", item["address"]["suburb"])
        item_loader.add_value("room_count", item["propertyBed"])
        item_loader.add_value("bathroom_count", item["propertyBath"])
        
        rent = item["propertyPricing"]["value"]
        if rent:
            price = rent.split(" ")[0].replace("$","")
            item_loader.add_value("rent", int(price)*4)
        item_loader.add_value("currency", "USD")
        
        from datetime import datetime
        import dateparser
        available = item["propertyPricing"]["availabilty"]
        if available:
            if "now" in available.lower():
                item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
            else:
                date_parsed = dateparser.parse(available.split("from")[1].strip(), date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
                    
        item_loader.add_value("external_id", str(item["id"]))
        
        parking = item["propertyParking"]
        if parking and parking!='0':
            item_loader.add_value("parking", True)

        item_loader.add_value("latitude", item["propertyCoords"].split(",")[0])
        item_loader.add_value("longitude", item["propertyCoords"].split(",")[1])

        images = item["propertyImage"]["listImg"]
        item_loader.add_value("images", images)
        
        desc = " ".join(response.xpath("//div[contains(@class,'listing__content')]//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        if "sqm" in desc:
            square_meters = desc.split("sqm")[0].strip().split(" ")[-1]
            item_loader.add_value("square_meters", square_meters)
        
        if "floor" in desc:
            floor = desc.split("floor")[0].strip().split(" ")[-1]
            item_loader.add_value("floor", floor)
        
        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="dishwasher", input_value="//li[contains(.,'Dishwasher')]/text()", input_type="F_XPATH", tf_item=True)
        
        
        item_loader.add_xpath("landlord_name", "//div[contains(@class,'agent-name')]/text()")
        item_loader.add_xpath("landlord_phone", "//div[contains(@class,'details-contacts')][1]/a[1]/text()")
        item_loader.add_xpath("landlord_email", "//a[contains(@href,'mail')]/@href[contains(.,'@')]")
        
        yield item_loader.load_item()