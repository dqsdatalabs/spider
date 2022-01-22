# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from urllib.parse import urljoin
import dateparser
from datetime import datetime
import re

class MySpider(Spider):
    name = 'prospect_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    
    def start_requests(self):

        start_urls = [
            {
                "url" : "https://www.prospect.co.uk/search/?showstc=on&showsold=off&instruction_type=Letting&orderby=price+desc&n=12&address_keyword=&minprice=&maxprice=&property_type=Detached&showstc=off",
                "property_type" : "house"
            },
            {
                "url" : "https://www.prospect.co.uk/search/?showstc=on&showsold=off&instruction_type=Letting&orderby=price+desc&n=12&address_keyword=&minprice=&maxprice=&property_type=Semi-Detached&showstc=off",
                "property_type" : "house"
            },
            {
                "url" : "https://www.prospect.co.uk/search/?showstc=on&showsold=off&instruction_type=Letting&orderby=price+desc&n=12&address_keyword=&minprice=&maxprice=&property_type=Terraced&showstc=off",
                "property_type" : "house"
            },
            {
                "url" : "https://www.prospect.co.uk/search/?showstc=on&showsold=off&instruction_type=Letting&orderby=price+desc&n=12&address_keyword=&minprice=&maxprice=&property_type=Apartment&showstc=off",
                "property_type" : "apartment"
            },
            {
                "url" : "https://www.prospect.co.uk/search/?showstc=on&showsold=off&instruction_type=Letting&orderby=price+desc&n=12&address_keyword=&minprice=&maxprice=&property_type=Bungalow&showstc=off",
                "property_type" : "house"
            },
            {
                "url" : "https://www.prospect.co.uk/search/?showstc=on&showsold=off&instruction_type=Letting&orderby=price+desc&n=12&address_keyword=&minprice=&maxprice=&property_type=Detached+Bungalow&showstc=off",
                "property_type" : "house"
            },
            {
                "url" : "https://www.prospect.co.uk/search/?showstc=on&showsold=off&instruction_type=Letting&orderby=price+desc&n=12&address_keyword=&minprice=&maxprice=&property_type=Town+House&showstc=off",
                "property_type" : "house"
            },
            {
                "url" : "https://www.prospect.co.uk/search/?showstc=on&showsold=off&instruction_type=Letting&orderby=price+desc&n=12&address_keyword=&minprice=&maxprice=&property_type=Maisonette&showstc=off",
                "property_type" : "house"
            },
            {
                "url" : "https://www.prospect.co.uk/search/?showstc=on&showsold=off&instruction_type=Letting&orderby=price+desc&n=12&address_keyword=&minprice=&maxprice=&property_type=End+of+Terrace&showstc=off",
                "property_type" : "house"
            },
        ] #LEVEL-1

        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//a[.='More Details']/@href").extract():
            f_url = response.urljoin(item)
            yield Request(
                f_url, 
                callback=self.populate_item, 
                meta={"property_type" : response.meta.get("property_type")},
            )
            
        next_page = response.xpath("//a[@class='next']/@href").get()
        if next_page:
            yield Request(
                url=response.urljoin(next_page),
                callback=self.parse,
                meta={"property_type" : response.meta.get("property_type")}
            )
        

    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Prospect_PySpider_" + self.country + "_" + self.locale)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("/")[-3])
        
        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", re.sub('\s{2,}', ' ', title.strip()))
        
        rent="".join(response.xpath("//div/h2[contains(.,'£')]/text()").getall())
        if rent:
            price=rent.split("£")[1].split("PCM")[0].strip()
            item_loader.add_value("rent_string", price.replace(",","")+"£")
        
        
        room_count= response.xpath("//div/ul[@class='rooms']/li[contains(@class,'bed')]/following-sibling::strong/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        
        bathroom_count = response.xpath("//div/ul[@class='rooms']/li[contains(@class,'bath')]/following-sibling::strong/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())
        
        address = response.xpath("//div/h3/parent::div/h4/text()").get()
        if address:
            zipcode = address.split(",")[-1].strip()
            city = address.split(zipcode)[0].strip().strip(",").split(",")[-1].strip()
            item_loader.add_value("address", address)
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode)
        
        latitude_longitude = response.xpath("//script[contains(.,'LatLng')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split("LatLng(")[1].split(",")[0].strip()
            longitude = latitude_longitude.split("LatLng(")[1].split(",")[1].split(")")[0].strip()
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
            
        desc="".join(response.xpath("//div[@id='1a']/div/p/text()").getall())
        if "sq" in desc.lower():
            sqmt=desc.lower().split("sq")[0].strip().split(" ")[-1]
            if sqmt.isdigit():
                sqm = str(int(int(sqmt)* 0.09290304))
                item_loader.add_value("square_meters", sqm)
        if desc:
            item_loader.add_value("description", re.sub('\s{2,}', ' ', desc.strip()))
        if "dishwasher" in desc.lower():
            item_loader.add_value("dishwasher", True)
        if "washing machine" in desc.lower():
            item_loader.add_value("washing_machine", True)
        if "balcony" in desc.lower():
            item_loader.add_value("balcony", True)
        if "terrace" in desc.lower():
            item_loader.add_value("terrace", True)
        if "lift" in desc.lower():
            item_loader.add_value("elevator", True)
        if "parking" in desc.lower():
            item_loader.add_value("parking", True)  
        
        images=[x for x in response.xpath("//div[contains(@class,'item')]/img/@data-src").getall()]
        if images:
            item_loader.add_value("images", images)
        else:
            images = [response.urljoin(x) for x in response.xpath("//img[@class='img-responsive ']/@src").getall()]
            if images:
                item_loader.add_value("images", images)
        parking=response.xpath("//ul/li[contains(.,'Parking')]/text()").get()
        if parking:
            item_loader.add_value("parking", True)
        elevator=response.xpath("//ul/li[contains(.,'Lift')]/text()").get()
        if elevator:
            item_loader.add_value("elevator", True)
        furnished=response.xpath("//ul/li[contains(.,'Furnished')]/text()").get()
        furnished1 = response.xpath("//div[@id='1a']/div/p/text()[contains(.,' FURNISHED')]").get()
        if furnished:
            item_loader.add_value("furnished", True)
        elif furnished1:
            item_loader.add_value("furnished", True)
        
        available_date = response.xpath("//div[@id='1a']/div/p/text()[contains(.,'AVAILABLE')]").get()
        if available_date:
            if "NOW" in available_date or "IMMEDIATELY" in available_date:
                available_date = datetime.now()
                item_loader.add_value("available_date", available_date.strftime("%Y-%m-%d"))
            else:
                try:
                    available_date = available_date.split("-")[0].split("AVAILABLE")[1]
                    if "," in available_date:
                        date_parsed = dateparser.parse(
                            available_date, date_formats=["%d/%m/%Y"]
                        )
                        if date_parsed:
                            date2 = date_parsed.strftime("%Y-%m-%d")
                            item_loader.add_value("available_date", date2)
                    else:
                        date_parsed = dateparser.parse(
                            available_date, date_formats=["%d/%m/%Y"]
                        )
                        if date_parsed:
                            date2 = date_parsed.strftime("%Y-%m-%d")
                            item_loader.add_value("available_date", date2)
                except: pass
                
        item_loader.add_value("landlord_name","PROSPECT")
        item_loader.add_value("landlord_phone","0118 955 9747")
        item_loader.add_value("landlord_email","propertymanagement@prospect.co.uk")
            
            
        yield item_loader.load_item()

