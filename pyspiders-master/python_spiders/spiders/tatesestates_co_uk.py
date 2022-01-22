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
import dateparser
from datetime import datetime
from word2number import w2n

class MySpider(Spider):
    name = 'tatesestates_co_uk'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.tatesestates.co.uk/properties/lettings/tag-apartments",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.tatesestates.co.uk/properties/lettings/tag-house",
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
        for item in response.xpath("//figure"):
            status = item.xpath("./span[contains(@class,'status-badge')]//text()").get()
            if status and "agreed" in status.lower():
                continue
            follow_url = response.urljoin(item.xpath("./a/@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
            seen = True

        if page == 2 or seen:
            p_url = f"https://www.tatesestates.co.uk/properties/lettings/tag-{response.meta['property_type']}/page-{page}"
            yield Request(
                p_url,
                callback=self.parse,
                meta={"property_type":response.meta["property_type"], "page":page+1}
            )    
        
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Tatesestates_Co_PySpider_united_kingdom")
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("properties/")[1].split("/")[0])

        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", re.sub('\s{2,}', ' ', title.strip()))
        
        address = response.xpath("//h3/text()").get()
        if address:
            count = address.count(",")
            item_loader.add_value("address", address)
            if count ==1:
                item_loader.add_value("city", address.split(",")[1].strip())
            else:
                zipcode = address.split(",")[-1].strip()
                if not zipcode.isalpha():
                    item_loader.add_value("zipcode", zipcode)
                    item_loader.add_value("city", address.split(",")[-2].strip())
                else:
                    item_loader.add_value("city", address.split(",")[-1].strip())
        
        room_count = response.xpath("//p[@class='lead']//text()[contains(.,'bedroom')]").get()
        if room_count:
            room_count = room_count.split("bedroom")[0].strip().split(" ")[-1]
            if room_count.isdigit():
                item_loader.add_value("room_count", room_count)
        
        rent = response.xpath("//p[@class='lead']//text()[contains(.,'£')]").get()
        if rent:
            price = rent.split("£")[1].strip()
            if "pw" in price:
                price = price.split("pw")[0].strip()
                item_loader.add_value("rent", int(price)*4)
            else:
                item_loader.add_value("rent", price.split("pcm")[0].strip())
            item_loader.add_value("currency", "GBP")
        
        bathroom_count = response.xpath("//li[contains(.,'bathroom')]/text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.split("bathroom")[0].strip()
            if bathroom_count.isdigit():
                item_loader.add_value("bathroom_count", bathroom_count)
            elif w2n.word_to_num(bathroom_count):
                item_loader.add_value("bathroom_count", w2n.word_to_num(bathroom_count))
                
        floor = response.xpath("//li[contains(.,'floor')]/text()").get()
        if floor:
            floor = floor.split("floor")[0].strip().split(" ")[-1]
            item_loader.add_value("floor", floor)
        
        desc = " ".join(response.xpath("//div[contains(@class,'description')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        images = [x for x in response.xpath("//ul[contains(@class,'bxslider')]//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        latitude_longitude = response.xpath("//script[contains(.,'LatLng(')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('LatLng(')[1].split(',')[0]
            longitude = latitude_longitude.split('LatLng(')[1].split(',')[1].split(')')[0].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
        
        energy_label = response.xpath("//li[contains(.,'nergy')]/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.lower().split("band")[1].replace(":","").strip().upper())
        
        parking = response.xpath("//li[contains(.,'garage') or contains(.,'parking')]/text()").get()
        if parking:
            item_loader.add_value("parking", True)
        
        unfurnished = response.xpath("//li[contains(.,'Unfurnished')]/text()").get()
        furnished = response.xpath("//li[contains(.,'Furnished') or contains(.,' furnished')]/text()").get()
        if furnished:
            item_loader.add_value("furnished", True)
        elif unfurnished:
            item_loader.add_value("furnished", False)
        
        elevator = response.xpath("//li[contains(.,'lift') or contains(.,'Lift')]/text()").get()
        if elevator:
            item_loader.add_value("elevator", True)
        
        available_date = response.xpath("//li[contains(.,'Available')]/text()").get()
        if available_date:
            if "now" in available_date:
                item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
        
        balcony = response.xpath("//li[contains(.,'Balcon') or contains(.,'balcon')]/text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        item_loader.add_value("landlord_name", "TATES ESTATES")
        item_loader.add_value("landlord_phone", "0207 602 6020")
        item_loader.add_value("landlord_email", "mail@tatesestates.co.uk")
        
        yield item_loader.load_item()