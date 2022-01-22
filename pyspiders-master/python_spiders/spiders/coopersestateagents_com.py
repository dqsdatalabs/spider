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
from datetime import datetime
import dateparser

class MySpider(Spider):
    name = 'coopersestateagents_com'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.coopersestateagents.com/properties/lettings/tag-apartment/status-available",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.coopersestateagents.com/properties/lettings/tag-bungalows/status-available",
                    "https://www.coopersestateagents.com/properties/lettings/tag-house/status-available"
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
        for item in response.xpath("//div[@class='togglable_area']//div[@class='photo_container']/../.."):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
            seen = True

        if page == 2 or seen:
            p_url = f"https://www.coopersestateagents.com/properties/lettings/status-available/page-{page}"
            yield Request(
                p_url,
                callback=self.parse,
                meta={"property_type":response.meta["property_type"], "page":page+1}) 
        
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Coopersestateagents_PySpider_united_kingdom")
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        
        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", re.sub('\s{2,}', ' ', title.strip()))
        
        address = "".join(response.xpath("//div[contains(@class,'prop')]/h2/text()").getall())
        if address:
            item_loader.add_value("address", address.strip())
            address = address.strip(",")
            if "," in address:
                item_loader.add_value("city", address.split(",")[-2].strip())
            else:
                item_loader.add_value("city", address)
        
        rent = response.xpath("//div[contains(@class,'prop')]/h2/span/text()").get()
        if rent:
            if "pw" in rent:
                price = rent.split(" ")[0].split("£")[1]
                item_loader.add_value("rent", int(price)*4)
            else:
                item_loader.add_value("rent", rent.split(" ")[0].split("£")[1])
            item_loader.add_value("currency", "GBP")
        
        room_count = response.xpath("//div[contains(@class,'Detail')]//li[contains(.,'Bedroom')]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split(" ")[0])
        
        bathroom_count = response.xpath("//div[contains(@class,'Detail')]//li[contains(.,'Bathroom')]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split(" ")[0])
        
        desc = " ".join(response.xpath("//div[@id='propertyDetails']//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        images = [x for x in response.xpath("//ul[@class='slides']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        external_id = response.xpath("//div[contains(@class,'Detail')]//li[contains(.,'Ref')]//text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split(":")[1])
        
        available_date = response.xpath("//ul[@id='points']//li[contains(.,'Available')]//text()").get()
        if available_date:
            if "now" in available_date.lower():
                item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
            available_date = available_date.split("Available")[1].replace("from","").replace("Academic Year","").replace("for","").replace(" beginning of","")
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
            else:
                available_date = response.xpath("//ul[@id='points']//li[contains(.,'Available for')]//text()").get()
                if available_date:
                    available_date = available_date.split("for")[1].replace("Academic Year","")
                    date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                    
                    if date_parsed:
                        date2 = date_parsed.strftime("%Y-%m-%d")
                        item_loader.add_value("available_date", date2)

        
        latitude_longitude = response.xpath("//script[contains(.,'LatLng(')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('LatLng(')[1].split(',')[0]
            longitude = latitude_longitude.split('LatLng(')[1].split(',')[1].split(')')[0].strip()    
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        floor = response.xpath("//ul[@id='points']//li[contains(.,'floor') or contains(.,'Floor')]//text()").get()
        if floor:
            floor = floor.lower().split("floor")[0].strip().split(" ")[-1]
            item_loader.add_value("floor", floor)

        furnished = response.xpath("//ul[@id='points']//li[contains(.,'Furnished') or contains(.,'Furnoshed')]//text()").get()
        unfurnished = response.xpath("//ul[@id='points']//li[contains(.,'Unfurnished')]//text()").get()
        if furnished:
            item_loader.add_value("furnished", True)
        elif unfurnished:
            item_loader.add_value("furnished", False)
            
        energy_label = response.xpath("//ul[@id='points']//li[contains(.,'EPC') or contains(.,'Energy Rating')]//text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.split(" ")[-1])
        
        parking = response.xpath("//ul[@id='points']//li[contains(.,'parking')]//text()").get()
        if parking:
            item_loader.add_value("parking", True)
        
        washing_machine = response.xpath("//ul[@id='points']//li[contains(.,'washing machine')]//text()").get()
        if washing_machine:
            item_loader.add_value("washing_machine", True)
        
        
        item_loader.add_value("landlord_name", "COOPERS ESTATE AGENTS")
        phone = response.xpath("//div[@class='mobile-hide']//strong/text()").get()
        if phone:
            item_loader.add_value("landlord_phone", phone.strip())
        item_loader.add_value("landlord_email", "sales@coopers-cov.com")
        
        yield item_loader.load_item()