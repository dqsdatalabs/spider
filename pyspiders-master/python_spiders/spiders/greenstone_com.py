# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags 
from python_spiders.loaders import ListingLoader
import json


class MySpider(Spider):
    name = 'greenstone_com' 
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    
    def start_requests(self):
        start_urls = [
            {"url": "https://www.greenstone.com/property/?property_category=residential&listings=rentals&search=&min-price=0&max-price=999999999&min-beds=&max-beds="}
            
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,)

    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get('page', 2)
        
        seen = False
        for item in response.xpath("//div[@class='item']/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 2 or seen:
            url = f"https://www.greenstone.com/property/page/{page}/?property_category=residential&listings=rentals&search&min-price=0&max-price=999999999&min-beds&max-beds"
            yield Request(url, callback=self.parse, meta={"page": page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Greenstone_PySpider_"+ self.country + "_" + self.locale)

        item_loader.add_value("external_link", response.url)
        title=response.xpath("//h2//text()").get()
        if title:
            item_loader.add_value("title", title)
        desc=" ".join(response.xpath("//div[contains(@class,'description')]/p//text()").getall())
        if "apartment" in desc:
            item_loader.add_value("property_type", "apartment")
        elif "house" in desc:
            item_loader.add_value("property_type", "house")
        else: return
        
        item_loader.add_value("description", desc)
        
        rent=response.xpath("//ul[contains(@class,'details')]/li[contains(.,'Price')]/text()").get()
        if rent:
            if "PCM" in rent:
                price=rent.split("(")[1].split(".")[0]
                item_loader.add_value("rent_string", price.replace(",",""))
            
        
        room_count=response.xpath("//ul[contains(@class,'details')]/li[contains(.,'Bedroom')]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        bathroom_count=response.xpath("//ul[contains(@class,'details')]/li[contains(.,'Bathroom')]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count) 
         
        
        square_meters=response.xpath("//ul[contains(@class,'details')]/li[contains(.,'Floor')]/text()").get()
        if square_meters:
            square_meters=square_meters.split("ft²")[0].strip()
            sqm = str(int(int(square_meters)* 0.09290304))
            item_loader.add_value("square_meters", sqm)
        
        address = response.xpath("//h2/text()").get()
        if address:
            count = address.count(",")
            if "," in address:
                item_loader.add_value("address", address)
                zipcode = address.split(" ")[-1]
                if "London" not in zipcode and "Road" not in zipcode:
                    item_loader.add_value("zipcode", zipcode)
            
                if zipcode:
                    city = address.split(zipcode)[0].strip().strip(",").split(",")[-1].strip()
                    item_loader.add_value("city", city)
                else:
                    city = address.split(",")[-2].strip()
                    item_loader.add_value("city", city)
        zipcodecheck=item_loader.get_output_value("zipcode")
        if not zipcodecheck:
            zipcode=response.url
            item_loader.add_value("zipcode",zipcode.split("-")[-1].replace("/","").upper())
        
        latitude=response.xpath("//div[@id='locations']/div/@data-lat").get()
        longitude=response.xpath("//div[@id='locations']/div/@data-long").get()
        if latitude and longitude:
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
          
            
        if "EPC –" in desc:
            energy_label=desc.split("EPC –")[1].replace(" ",",")
            item_loader.add_value("energy_label", energy_label.split(",")[1])

        images=[x for x in response.xpath("//div[contains(@class,'slideshow-slide')]/@href").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))
        
        item_loader.add_value("landlord_name", "Green Stone")
        item_loader.add_value("landlord_phone", "0207 625 1000")
        item_loader.add_value("landlord_email", "info@greenstone.com")

        yield item_loader.load_item()