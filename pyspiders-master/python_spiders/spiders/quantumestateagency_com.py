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
from word2number import w2n
import dateparser
from datetime import datetime
from word2number import w2n

class MySpider(Spider):
    name = 'quantumestateagency_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en' 
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.quantumestateagency.com/?s=property&to=rent&order=DESC&postcode=&radius=1&min_price=&max_price=&min_bed=1&type=apartment",
                ],
                "property_type" : "apartment"
            },
            {
                "url" : [
                    "https://www.quantumestateagency.com/?s=property&to=rent&order=DESC&postcode=&radius=1&min_price=&max_price=&min_bed=1&type=house",
                ],
                "property_type" : "house"
            },   
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    total_page = None
    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='grid__overlay']/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("external_source","Quantumestateagency_PySpider_"+ self.country)
        property_type = response.meta.get('property_type')
        prp_type = response.xpath("//li[contains(.,'Studio')]/text()").get()
        if prp_type:
            property_type = "studio"
        item_loader.add_value("property_type", property_type )
        item_loader.add_value("external_link", response.url)
        title = response.xpath("//h1/span/text()").get()
        item_loader.add_value("title", title)
        address = response.xpath("//div[contains(@class,'price')]//span/text()").get()
        if address:
            item_loader.add_value("address", address)
            if "," in address:
                city = address.split(",")[-1].strip()
                if city:
                    item_loader.add_value("city",city)
                else:
                    item_loader.add_value("city", address.split(",")[-2].strip())
            else:
                item_loader.add_value("city", address.split(" ")[-1])
                

        rent = response.xpath("//div[contains(@class,'price')]/h1/text()").get()
        if rent:
            price = rent.strip().split("£")[1]
            item_loader.add_value("rent", price)
        item_loader.add_value("currency", "GBP")
        
        room_count = response.xpath("//ul/li[contains(.,'bedroom') or contains(.,'Bedroom')]/text()").get()
        room = response.xpath("//ul/li[contains(.,'Bed ')]/text()").re_first(r'\d+')
        if room:            
                item_loader.add_value("room_count", room)
        elif room_count:
            room_count = room_count.split(" ")[0]
            if "Double" in room_count:
                item_loader.add_value("room_count", "1")
            elif room_count.isdigit():
                item_loader.add_value("room_count", room_count)
            else:
                if (room_count!="Currently") and (room_count!="Master"):
                    item_loader.add_value("room_count", w2n.word_to_num(room_count))
        
        desc = "".join(response.xpath("//div[contains(@class,'slider')]/section//text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())
        
        images = [x.replace("background-image: url('","").replace("')","") for x in response.xpath("//div[@class='lazyload imageBG']/@style").getall()]
        if images:
            item_loader.add_value("images", images)
        
        latitude = response.xpath("//div/@data-lat").get()
        if latitude:
            item_loader.add_value("latitude", latitude)
        
        longitude = response.xpath("//div/@data-lng").get()
        if longitude:
            item_loader.add_value("longitude", longitude)
        
        available_date = "".join(response.xpath("//div[contains(@class,'summary')]/p/text()[contains(.,'Available')]").getall())
        if available_date:
            available_date = available_date.split("Available")[1].split("*")[0].strip()
            if "now" in available_date.lower():
                available_date = datetime.now()
                date2 = available_date.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
                
        energy_label = "".join(response.xpath("//div[contains(@class,'summary')]/p/text()[contains(.,'EPC')]").getall())
        if energy_label:
            energy_label = energy_label.split("EPC")[1].strip().replace("rating","").replace("***","").strip()
            item_loader.add_value("energy_label", energy_label)
            
        unfurnished = response.xpath("//ul/li[contains(.,'Unfurnished')]/text()").get()
        furnished = response.xpath("//ul/li[contains(.,'furnished') or contains(.,'Furnished')]/text()").get()
        if unfurnished:
            item_loader.add_value("furnished", False)
        elif furnished:
            item_loader.add_value("furnished", True)
        
        
        parking = response.xpath("//ul/li[contains(.,'parking') or contains(.,'Parking') or contains(.,'garage') or contains(.,'Garage')]/text()").get()
        if parking:
            item_loader.add_value("parking", True)
        deposit = response.xpath("//div[contains(@class,'slider')]/section//text()[contains(.,'Deposit:') and not(contains(.,'Holding'))]").get()
        if deposit:
            try:                
                deposit1 = deposit.split("Deposit:")[1].split("Month")[0].strip()
                deposit1 = w2n.word_to_num(deposit1)
                deposit2 = deposit.split("£")[1].split(".")[0].strip().replace(",","")
                if price:
                    deposit = int(price)*int(deposit1)
                    item_loader.add_value("deposit", deposit+int(float(deposit2)))
            except:
                pass
                    
        balcony = response.xpath("//ul/li[contains(.,'balcon') or contains(.,'Balcon')]/text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        terrace = response.xpath("//ul/li[contains(.,'terrace') or contains(.,'Terrace')]/text()").get()
        if terrace:
            item_loader.add_value("terrace", True)
        
        item_loader.add_value("landlord_name", "QUANTUM SALES & LETTINGS")
        item_loader.add_value("landlord_phone", "01904 631631")
        item_loader.add_value("landlord_email", "homes@quantumestateagency.com")
        
        
        yield item_loader.load_item()