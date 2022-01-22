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
    name = 'dijones_com_au'   
    execution_type='testing'
    country='australia'
    locale='en' 
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.dijones.com.au/wp-json/api/listings/all?limit=96&type=rental&status=current&paged={}&bed=&bath=&car=&sort=newest&category=apartment%2Cterrace",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.dijones.com.au/wp-json/api/listings/all?limit=96&type=rental&status=current&paged={}&bed=&bath=&car=&sort=newest&category=house%2Cunit%2Ctownhouse",
                ],
                "property_type" : "house",
            },
            {
                "url" : [
                    "https://www.dijones.com.au/wp-json/api/listings/all?limit=96&type=rental&status=current&paged={}&bed=&bath=&car=&sort=newest&category=studio",
                ],
                "property_type" : "studio",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item.format(1),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'), "base":item})

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        data = json.loads(response.body)
        if "results" in data:
            for item in data["results"]:
                follow_url = item["slug"]
                yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"],"item":item})
                seen = True

        if page == 2 or seen:
            base = response.meta["base"]
            p_url = base.format(page)
            yield Request(p_url, callback=self.parse, meta={"property_type":response.meta["property_type"], "base":base, "page":page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Dijones_Com_PySpider_australia")   
        item_loader.add_xpath("title", "//h1//text()")   

        item = response.meta.get('item')
        rent = item["propertyPricing"]["value"]
        if rent:
            if "deposit taken" in rent.lower() or "leased" in rent.lower():
                return
        item_loader.add_value("external_id", str(item["uniqueID"])) 
        room_count = str(item["propertyBed"])
        if room_count !="0":
            item_loader.add_value("room_count", room_count) 
        elif "studio" in response.meta.get('property_type'):
            item_loader.add_value("room_count", "1") 

        item_loader.add_value("bathroom_count", str(item["propertyBath"])) 
        item_loader.add_value("city", item["address"]["suburb"]) 
        if "propertyCoords" in item:
            item_loader.add_value("latitude", item["propertyCoords"].split(",")[0])
            item_loader.add_value("longitude", item["propertyCoords"].split(",")[1].strip())
        
        available_date = item["propertyPricing"]["availabilty"]
        if available_date:
            if "Available Now" in available_date:
                item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
            else:
                date_parsed = dateparser.parse(available_date.split(" from ")[-1], date_formats=["%d %m %Y"])
                if date_parsed:
                    item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))
       
        parking = str(item["propertyParking"])
        if parking:
            if "0" in parking:
                item_loader.add_value("parking", False)
            else:
                item_loader.add_value("parking", True)
        address = response.xpath("//h1//text()").get()
        if address:
            item_loader.add_value("address", address)
            item_loader.add_value("zipcode", address.split(" ")[-1])
        if rent:
            rent = rent.split("$")[-1].strip().split(" ")[0].split("-")[0].replace(",","")
            item_loader.add_value("rent", int(float(rent))*4)
        item_loader.add_value("currency", "AUD")
        desc = "".join(response.xpath("//div[contains(@class,'text-left')]/p//text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())
        
        images = [x for x in response.xpath("//div[@id='listing-single-slider']/a/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            
        floor_plan_images = [x for x in response.xpath("//div[@id='floorplan-slider2']/a/img/@src").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)
        landlord_name = response.xpath("//div[@class='single-agent-slide'][last()]//h3/text()").get()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name)
            item_loader.add_xpath("landlord_phone", "//div[@class='single-agent-slide'][last()]//a[contains(@href,'tel')]/text()[.!='Call']")
            item_loader.add_xpath("landlord_email", "//div[@class='single-agent-slide'][last()]//a[contains(@href,'mail')]/text()[.!='Email']")
        else:
            item_loader.add_value("landlord_name", "DiJones")
            item_loader.add_value("landlord_phone", "+61 (02) 8356 7878")
            item_loader.add_value("landlord_email", "home@dijones.com.au")

        yield item_loader.load_item()
