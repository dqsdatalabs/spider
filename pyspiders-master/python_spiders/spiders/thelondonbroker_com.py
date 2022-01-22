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
import re

class MySpider(Spider):
    name = 'thelondonbroker_com'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://thelondonbroker.com/search/page/{}/?listing_type=rent&location=&type=Apartment&within=2&price_from=&price_to=&beds_min=na&beds_max=na",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://thelondonbroker.com/search/page/{}/?listing_type=rent&location=&type=House&within=2&price_from=&price_to=&beds_min=na&beds_max=na",
                    
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item.format(""),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'), "base_url":item})


    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//a[contains(@class,'property-link')]"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
            seen = True
        
        if page == 2 or seen:
            base_url = response.meta["base_url"]
            p_url = base_url.format(page)
            yield Request(
                p_url,
                callback=self.parse, 
                meta={"page":page+1, "property_type":response.meta["property_type"], "base_url":base_url})
         
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Thelondonbroker_PySpider_united_kingdom")
        item_loader.add_value("external_link", response.url)
        externalid=response.xpath("//link[@rel='shortlink']/@href").get()
        if externalid:
            item_loader.add_value("external_id",externalid.split("p=")[-1])
        
        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", re.sub('\s{2,}', ' ', title.strip()))
        
        rent = response.xpath("//span[contains(.,'£')]//text()").get()
        if rent:           
            price = int(rent.split('pcm')[0].split('£')[-1].replace(",","").strip())
            item_loader.add_value("rent", price)
            item_loader.add_value("currency","GBP")

        address = response.xpath("//h1//text()").get()
        if address:
            item_loader.add_value("address", address)
            zipcode = address.split(",")[-1].strip().split(" ")[-1]
            city = address.split(",")[-1].split(zipcode)[0].strip()
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode)

        bathroom_count = response.xpath("//span[contains(@class,'baths')]//text()").get()
        item_loader.add_value("bathroom_count", bathroom_count)

        square_meters = response.xpath("//div[@class='property-detail'][contains(.,'sq')]//text()").get()
        if square_meters:
            square_meters = square_meters.split('/')[1].strip().split(' ')[0]
            item_loader.add_value("square_meters", square_meters)
        
        desc = "".join(response.xpath("//div[@class='row row-squeeze']//p//text()").getall())
        if desc:
            item_loader.add_value("description", re.sub('\s{2,}', ' ', desc.strip()))
        
        if "studio" in desc.lower():
            item_loader.add_value("property_type", "studio")
        else:
            item_loader.add_value("property_type", response.meta.get('property_type'))
        
        room_count = response.xpath("//span[contains(@class,'beds')]//text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        elif "studio" in desc.lower():
            item_loader.add_value("room_count", "1")
            
        images = [x for x in response.xpath("//div[@class='slide']/div/@style").getall()]
        if images:
            item_loader.add_value("images", images)
        
        if "Available end of" in desc:
            available_date = desc.split("Available end of")[1].split(".")[0].strip()
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        elif "available now" in desc:
            item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
            
        floor = response.xpath("//ul/li[contains(.,'floor') or contains(.,'Floor')]/text()").get()
        if floor:
            floor = floor.lower().split("floor")[0].strip().split(" ")[-1]
            item_loader.add_value("floor", floor.capitalize())
        
        elevator = response.xpath("//ul/li[contains(.,'lift') or contains(.,'Lift')]/text()").get()
        no_elevator = response.xpath("//ul/li[contains(.,'no lift')]/text()").get()
        if no_elevator:
            item_loader.add_value("elevator", False)
        elif elevator:
            item_loader.add_value("elevator", True)
        
        balcony = response.xpath("//ul/li[contains(.,'balcon') or contains(.,'Balcon')]/text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        parking = response.xpath("//ul/li[contains(.,'parking') or contains(.,'Parking')]/text()").get()
        if parking:
            item_loader.add_value("parking", True)
        
        terrace = response.xpath("//ul/li[contains(.,'terrace') or contains(.,'Terrace')]/text()").get()
        if terrace:
            item_loader.add_value("terrace", True)
        
        furnished = response.xpath("//ul/li[contains(.,'Furnished') or contains(.,' furnished')]/text()").get()
        if furnished:
            item_loader.add_value("furnished", True)
        latlng=response.xpath("//script[contains(.,'Location(')]/text()").get()
        if latlng:
            item_loader.add_value("longitude",latlng.split("Location(")[-1].split(",")[0])
        lati=response.xpath("//script[contains(.,'Location')]/text()").get()
        if lati:
            item_loader.add_value("latitude",lati.split("Location(")[-1].split(")")[0].split(",")[-1])
        
        item_loader.add_value("landlord_name", "THE LONDON BROKER")
        
        phone = response.xpath("//a[@class='broker-phone']/@href").get()
        if phone:
            item_loader.add_value("landlord_phone", phone.split("+")[1])
            
        item_loader.add_value("landlord_email", "enquiries@thelondonbroker.com")
        
       
        yield item_loader.load_item()