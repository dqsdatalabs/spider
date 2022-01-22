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
    name = 'andrewgranger_co_uk'
    execution_type='testing'
    country='australia'
    locale='en'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.andrewgranger.co.uk/search.ljson?channel=lettings&fragment=tag-apartment",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.andrewgranger.co.uk/search.ljson?channel=lettings&fragment=tag-detached-house",
                    "https://www.andrewgranger.co.uk/search.ljson?channel=lettings&fragment=tag-attached-house",
                    "https://www.andrewgranger.co.uk/search.ljson?channel=lettings&fragment=tag-bungalow",


                ],
                "property_type" : "house"
            }
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get('page', 2)
        data = json.loads(response.body)
        if data["pagination"]["total_count"] >0:
            for item in data["properties"]:
                url = item["property_url"]
                yield Request(response.urljoin(url), callback=self.populate_item, meta={"item":item,"property_type":response.meta.get('property_type')})
        
        if data["pagination"]["page_count"] >= page:      
            f_url = response.url.split("/page-")[0]+f"/page-{page}"
            yield Request(f_url, callback=self.parse, meta={"page": page+1, 'property_type':response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("properties/")[-1].split("/")[0])
        item_loader.add_value("external_source", "Andrewgranger_Co_PySpider_united_kingdom")
        
        title = "".join(response.xpath("//h1//text()").getall())
        if title:
            item_loader.add_value("title", re.sub('\s{2,}', ' ', title.strip()))
        
        item = response.meta.get('item')
        
        item_loader.add_value("address", item["display_address"])
        item_loader.add_value("city", item["town"])
        rent = item["price"]
        if rent:
            rent = rent.split(" ")[0].replace("£","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "GBP")
        item_loader.add_value("room_count", item["bedrooms"])
        item_loader.add_value("latitude", str(item["lat"]))
        item_loader.add_value("longitude", str(item["lng"]))
        
        images = item["photos"]
        for i in images:
            item_loader.add_value("images", response.urljoin(i))
        
        energy_label = response.xpath("//div[@class='propertyMainDescription']//p//text()[contains(.,'EPC')]").get()
        if energy_label:
            energy_label = energy_label.lower().replace("rating","").replace("of","").split("epc")[1].replace(":","").replace("'","").replace(".","").strip()
            if "," in energy_label: energy_label = energy_label.split(",")[0]
            if len(energy_label) ==1: item_loader.add_value("energy_label", energy_label)
        
        description = " ".join(response.xpath("//div[@class='propertyMainDescriptionText']//p//text()").getall())
        if description:
            item_loader.add_value("description", description.strip())
            
        parking = response.xpath("//div[@class='propertyMainDescription']//p//text()[contains(.,'Parking') or contains(.,'parking') or contains(.,'Garage') or contains(.,'garage')]").get()
        if parking:
            item_loader.add_value("parking", True)
        
        furnished = response.xpath("//div[@class='propertyMainDescription']//p//text()[contains(.,'Furnished') or contains(.,'FURNISHED') or contains(.,' furnished')]").get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        item_loader.add_value("landlord_name","Andrew Granger & Co")
        item_loader.add_value("landlord_phone","01162429922")
        
        yield item_loader.load_item()