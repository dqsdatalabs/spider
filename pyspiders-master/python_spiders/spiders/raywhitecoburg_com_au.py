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
    name = 'raywhitecoburg_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    custom_settings = {
        "PROXY_ON":"True",
    }
    
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://raywhitecoburg.com.au/properties/residential-for-rent?category=APT%7CUNT&keywords=&minBaths=0&minBeds=0&minCars=0&rentPrice=&sort=updatedAt+desc&suburbPostCode=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://raywhitecoburg.com.au/properties/residential-for-rent?category=HSE%7CTCE%7CTHS&keywords=&minBaths=0&minBeds=0&minCars=0&rentPrice=&sort=updatedAt+desc&suburbPostCode=",
                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "https://raywhitecoburg.com.au/properties/residential-for-rent?category=STD&keywords=&minBaths=0&minBeds=0&minCars=0&rentPrice=&sort=updatedAt+desc&suburbPostCode=",
                ],
                "property_type" : "studio"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='proplist_item']/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("/")[-1])

        item_loader.add_value("external_source", "Raywhitecoburg_Com_PySpider_australia")     
        title = " ".join(response.xpath("//h1//text()").getall())
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
    
        room_count = "".join(response.xpath("//div[contains(@class,'pdp_header')]//li[contains(@class,'bed')]//text()").getall())
        if room_count:
            room_count = room_count.strip().split(" ")[0]
            item_loader.add_value("room_count", room_count)

        bathroom_count = "".join(response.xpath("//div[contains(@class,'pdp_header')]//li[contains(@class,'bath')]//text()").getall())
        if bathroom_count:
            bathroom_count = bathroom_count.strip().split(" ")[0]
            item_loader.add_value("bathroom_count", bathroom_count)
        address = " ".join(response.xpath("//h1//text()").getall())
        if address:
            address = re.sub('\s{2,}', ' ', address.strip())
            item_loader.add_value("address", address)
        city_zipcode = response.xpath("//h1//span//text()").get()
        if city_zipcode:
            city = city_zipcode.split(",")[0].strip()
            zipcode = city_zipcode.split("VIC")[1].strip()
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode)
        
        desc = " ".join(response.xpath("//div[contains(@class,'pdp_description_content')]/p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        price = response.xpath("//span[contains(@class,'pdp_price')]//text()").get()
        if price:
            rent = price.split("Weekly")[0].split("$")[1].strip().split(" ")[0]
            if "Bond" in price:
                deposit = price.split("/")[1].split("$")[1].split("Bond")[0].strip().replace(",","")
                item_loader.add_value("deposit",deposit)
            item_loader.add_value("rent", int(float(rent))*4)
            
        item_loader.add_value("currency", "AUD")

        from datetime import datetime
        import dateparser
        available = "".join(response.xpath("//div[contains(@class,'event_heading')][contains(.,'Available')]//parent::div//div[contains(@class,'event_date_wrap')]//span//text()").getall())
        if available:
            day = response.xpath("//div[contains(@class,'event_heading')][contains(.,'Available')]//parent::div//span[contains(@class,'event_date')]//text()").get()
            month = "".join(response.xpath("//div[contains(@class,'event_heading')][contains(.,'Available')]//parent::div//span[contains(@class,'event_month')]//text()").getall())
            available_date = day+" "+month
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)

        latitude_longitude = response.xpath("//script[contains(.,'geo')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('"latitude":')[1].split(',')[0]
            longitude = latitude_longitude.split('longitude":')[1].split('}')[0].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        balcony = response.xpath("//li[contains(.,'Balcon')]//text()").get()
        if balcony:
            item_loader.add_value("balcony",True)
        terrace = response.xpath("//li[contains(.,'Terrace')]//text()").get()
        if terrace:
            item_loader.add_value("terrace",True)
        parking = "".join(response.xpath("//div[contains(@class,'pdp_header')]//li[contains(@class,'car')]//text()").getall())
        if parking:
            item_loader.add_value("parking",True)
        
        images = [x for x in response.xpath("//noscript//@src").getall()]
        if images:
            item_loader.add_value("images", images)
       
        item_loader.add_value("landlord_name", "Ray White Coburg")
        item_loader.add_value("landlord_phone", "+61 (3) 9383 3555")
        item_loader.add_value("landlord_email", "	coburg.vic@raywhite.com")
        
        yield item_loader.load_item()