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
    name = 'tuckergardner_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en' 
    external_source = 'Tuckergardner_PySpider_united_kingdom'
    thousand_separator = ','
    scale_separator = '.' 
    # custom_settings = {
    #     "PROXY_ON":"True"
    # }

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.tuckergardner.com/search.ljson?channel=lettings&fragment=most-recent-first/status-all/page-1",
                ],
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item.format(1),
                            callback=self.parse)


    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        data = json.loads(response.body)
        for item in data["properties"]:
            if "agreed" in item["status"]:
                continue
            
            base_url = "https://www.tuckergardner.com/"
            follow_url = base_url + item["url"]
            
            yield Request(follow_url, callback=self.populate_item, meta={"item":item})
            seen = True
        
        if data["pagination"]["has_next_page"]:
            if page == 2 or seen:            
                p_url = f"https://www.tuckergardner.com/search.ljson?channel=lettings&fragment=most-recent-first/status-all/page-{page}"
                yield Request(
                    p_url,
                    callback=self.parse,
                    meta={"page":page+1})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_id", response.url.split("properties/")[1].split("/")[0])    
        
        item = response.meta.get('item')
        prop_type = item["type"]
        if prop_type and "apartment" in prop_type.lower():
            prop_type = "apartment"
        elif prop_type and "flat" in prop_type.lower():
            prop_type = "apartment"
        elif prop_type and "house" in prop_type.lower():
            prop_type = "house"
        elif prop_type and "unspecified" in prop_type.lower():
            prop_type = "house"
        elif prop_type and "studio" in prop_type.lower():
            prop_type = "studio"
        else:
            return

        if prop_type:
            item_loader.add_value("property_type", prop_type)
        else:
            return

        title = response.xpath("//title//text()").get()
        if title and "£" in title:
            item_loader.add_value("title", title.split("-")[0].strip()) 
        else:
            item_loader.add_value("title", title) 

        if "price" in item:
            item_loader.add_value("rent_string", item["price"])

        room = item["bedrooms"]
        if room:
            item_loader.add_value("room_count", room)
        bathroom = item["bathrooms"]
        if bathroom:
            item_loader.add_value("bathroom_count", bathroom)
        
        if "displayAddress" in item:
            address = item["displayAddress"]
            item_loader.add_value("address", address)
            if "," in address:
                city = address.split(",")[-1].strip()
                if city:
                    item_loader.add_value("city",city)  
                                        
        
        item_loader.add_value("latitude", str(item["lat"]))
        item_loader.add_value("longitude", str(item["lng"]))
            
        script_desc = " ".join(response.xpath("//script/text()[contains(.,'ga_product')]").extract())
        if script_desc:
            desc = script_desc.split('"description":"')[1].split('"});')[0].replace("\\u003cp\\u003e", "").replace("\\u003c/p\\u003e", " ")
            item_loader.add_value("description", desc)
            if desc and "parking" in desc.lower():
                item_loader.add_value("parking",True)
            if desc and "furnish" in desc.lower():
                item_loader.add_value("furnished",True)
            if desc and "balcon" in desc.lower():
                item_loader.add_value("balcony",True)
        
        script_image = response.xpath("//script/text()[contains(.,'ga_product')]").get()
        if script_image:
            images = re.findall('"propertyPhoto":"([^,]+)"},{', script_image)
            item_loader.add_value("images",images)
        item_loader.add_xpath("landlord_name", "//div[@class='copy__content']/a/h4[@class='details-frame__content-title']/text()")
        item_loader.add_xpath("landlord_phone", "//div[@class='copy__content']//span[contains(@class,'-link-text--phone')]/text()")

        yield item_loader.load_item()