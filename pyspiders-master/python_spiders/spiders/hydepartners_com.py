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
class MySpider(Spider):
    name = 'hydepartners_com'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.hydepartners.com/search/?showstc=off&instruction_type=Letting&address_keyword=&property_type=Apartment&n=25",
                    "https://www.hydepartners.com/search/?showstc=off&instruction_type=Letting&address_keyword=&property_type=Terraced&n=25",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.hydepartners.com/search/?showstc=off&instruction_type=Letting&address_keyword=&property_type=Detached&n=25",
                    "https://www.hydepartners.com/search/?showstc=off&instruction_type=Letting&address_keyword=&property_type=Semi-Detached&n=25",
                    
                ],
                "property_type" : "house"
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
        for item in response.xpath("//div[contains(@class,'panel-default')]"):
            status = item.xpath(".//div[@class='property-grid-image']/img/@src").get()
            if status and "agreed" in status.lower():
                continue
            follow_url = response.urljoin(item.xpath(".//a[contains(.,'Details')]/@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
            seen = True
        
        if page == 2 or seen:
            base_url = response.meta["base_url"]
            p_url = f"https://www.hydepartners.com/search/{page}.html?" + base_url.split("?")[1]
            yield Request(
                p_url,
                callback=self.parse,
                meta={"page":page+1, "property_type":response.meta["property_type"], "base_url":base_url})
         
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("-details/")[1].split("/")[0])
        item_loader.add_value("property_type", response.meta.get('property_type'))  
        item_loader.add_value("external_source", "Hydepartners_PySpider_united_kingdom")     
        item_loader.add_xpath("title","//div/h1/span/text()") 
        item_loader.add_xpath("room_count","//span[@class='property-bedrooms']//text()") 
        item_loader.add_xpath("bathroom_count","//span[@class='property-bathrooms']//text()")
        
        rent = " ".join(response.xpath("//span[@itemprop='price']//text()").extract())
        if rent:
            item_loader.add_value("rent_string",rent.replace(",","") )  
        address = response.xpath("//div/h1/span/text()").extract_first()
        if address:
            item_loader.add_value("address",address.strip())
            if len(address.split(",")) > 2:       
                zipcode = address.split(",")[-1].strip()
                city = address.split(",")[-2].strip() 
                if zipcode.isalpha():
                    zipcode = address.split(",")[-2] 
                    city = address.split(",")[-3]                       
                item_loader.add_value("city",city.strip())  
                item_loader.add_value("zipcode",zipcode.strip())  
        deposit = response.xpath("//p[contains(.,'Deposit')]/strong/text()").extract_first()
        if deposit:
            item_loader.add_value("deposit",deposit.replace(",",""))
        available_date = " ".join(response.xpath("//p[contains(.,'Available')]/strong/text()").extract())
        if available_date:  
            date_parsed = dateparser.parse(available_date, date_formats=["%d-%m-%Y"], languages=['en'])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        desc = " ".join(response.xpath("//div[@class='property-details-box']/p//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
    
        images = [response.urljoin(x) for x in response.xpath("//div[@class='carousel-inner']/div/img/@src").extract()]
        if images:
            item_loader.add_value("images", images)

        latitude_longitude = response.xpath("//script[contains(.,'&q=')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split("&q=")[1].split("%2C")[0]
            longitude = latitude_longitude.split("&q=")[1].split("%2C")[1].split('"')[0]
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)
            
        item_loader.add_value("landlord_name", "Hyde Partners")
        item_loader.add_value("landlord_phone", "0161 773 4583")
        item_loader.add_value("landlord_email", "sales@hydepartners.com")  
        

        yield item_loader.load_item()