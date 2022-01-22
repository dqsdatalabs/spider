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

 
class MySpider(Spider):
    name = 'danielpaul_co_uk'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.danielpaul.co.uk/properties/lettings/tag-flat/status-available",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.danielpaul.co.uk/properties/lettings/tag-house/status-available",
                    
                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "https://www.danielpaul.co.uk/properties/lettings/tag-studio/status-available",
                    
                ],
                "property_type" : "studio"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'), "base_url":item})


    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//div[@class='propList-inner']/div/a"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
            seen = True
        
        if page == 2 or seen:
            base_url = response.meta["base_url"]
            p_url = base_url + f"/page-{page}"
            yield Request(
                p_url,
                callback=self.parse,
                meta={"page":page+1, "property_type":response.meta["property_type"], "base_url":base_url})
         
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Danielpaul_Co_PySpider_united_kingdom")
        item_loader.add_value("external_link", response.url)

        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", re.sub('\s{2,}', ' ', title.strip()))
        
        address = "".join(response.xpath("//div[contains(@class,'prop')]/h2/text()").getall())
        if address:
            if "Beds" in address:
                address = address.split("2")[0].strip()
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", "London")
        
        rent = response.xpath("//div[contains(@class,'prop')]/h2/span/text()").get()
        if rent:
            if "pw" in rent:
                item_loader.add_value("rent", int("".join(filter(str.isnumeric, rent)))*4)
            else:
                item_loader.add_value("rent", "".join(filter(str.isnumeric, rent)))
            item_loader.add_value("currency", "GBP")
        else:
            rent = response.xpath("//div[@id='propertyDetails']//text()[contains(.,'per month')]").get()
            if rent:
                item_loader.add_value("rent", rent.split('per month')[0].split('£')[-1].replace(',', ''))
                item_loader.add_value("currency", "GBP")
        
        room_count = response.xpath("//div[contains(@class,'Detail')]//li[contains(.,'Bedroom')]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split(" ")[0])
        else:
            room_count = response.xpath("//div[contains(@class,'Detail')]//li[contains(.,'Reception')]/text()").get()
            if room_count:
                item_loader.add_value("room_count", room_count.split(" ")[0])
            
        
        bathroom_count = response.xpath("//div[contains(@class,'Detail')]//li[contains(.,'Bathroom')]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split(" ")[0])
        
        desc = " ".join(response.xpath("//div[@id='propertyDetails']//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        if "studio" in desc.lower():
            item_loader.add_value("property_type", "studio")
        else:
            item_loader.add_value("property_type", response.meta.get('property_type'))
        
        deposit = response.xpath("//div[@id='propertyDetails']//p//text()[contains(.,'deposit') and not(contains(.,'NO'))]").get()
        if deposit:
            price = ""
            if "pw" in rent:
                price = int("".join(filter(str.isnumeric, rent)))
            else:
                price = int("".join(filter(str.isnumeric, rent)))/4
                
            if "£" in deposit:
                deposit = deposit.split("£")[1].strip().split(" ")[0]
                item_loader.add_value("deposit", deposit)
            else:
                deposit = deposit.strip().split(" ")[0]
                if deposit.isdigit():
                    item_loader.add_value("deposit", int(deposit)*price)
        
        if "Service charge:" in desc:
            utilities = desc.split("Service charge:")[1].split("\u00a3")[1].split(".")[0]
            item_loader.add_value("utilities", utilities)
            
        energy_label = response.xpath("//ul[@id='points']//li[contains(.,'EPC') or contains(.,'Energy Rating')]//text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.split(" ")[-1])
        elif "green rating" in desc.lower():
            energy_label = desc.lower().split("green rating")[1].strip().split(" ")[0]
            item_loader.add_value("energy_label", energy_label.upper())
        
        images = [x for x in response.xpath("//ul[@class='slides']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        item_loader.add_value("external_id", response.url.split('/')[-2].strip())
        
        available_date = "".join(response.xpath("//p/strong[contains(.,'available')]/parent::p//text()").getall())
        if available_date:
            date_parsed = dateparser.parse(available_date.split(":")[1].strip(), date_formats=["%d/%m/%Y"])
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
            
        parking = response.xpath("//ul[@id='points']//li[contains(.,'parking')]//text()").get()
        if parking:
            item_loader.add_value("parking", True)
        
        washing_machine = response.xpath("//ul[@id='points']//li[contains(.,'washing machine')]//text()").get()
        if washing_machine:
            item_loader.add_value("washing_machine", True)
        
        item_loader.add_value("landlord_name", "DANIEL PAUL SALES, LETTINGS & MANAGEMENT")
        item_loader.add_value("landlord_phone", "020 8452 1436")
        
        yield item_loader.load_item()