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
    name = 'urbanandrural_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en' 
    thousand_separator = ','
    scale_separator = '.'    
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.urbanandrural.com/search?limit=20&includeDisplayAddress=Yes&active=&auto-lat=&auto-lng=&p_department=RL&propertyAge=&location=&propertyType=8%2C11%2C28&minimumRent=&maximumRent=&minimumBedrooms=0&maximumBedrooms=0&searchRadius=&orderBy=price%2Bdesc&availability=0%2C1",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.urbanandrural.com/search?limit=20&includeDisplayAddress=Yes&active=&auto-lat=&auto-lng=&p_department=RL&propertyAge=&location=&propertyType=12%2C13%2C14%2C15&minimumRent=&maximumRent=&minimumBedrooms=0&maximumBedrooms=0&searchRadius=&orderBy=price%2Bdesc&availability=0%2C1",
                    "https://www.urbanandrural.com/search?limit=20&includeDisplayAddress=Yes&active=&auto-lat=&auto-lng=&p_department=RL&propertyAge=&location=&propertyType=1%2C2%2C3%2C4%2C5%2C6%2C21%2C26&minimumRent=&maximumRent=&minimumBedrooms=0&maximumBedrooms=0&searchRadius=&orderBy=price%2Bdesc&availability=0%2C1"
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
        for item in response.xpath("//div[contains(@class,'imageHolder')]/.."):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

        next_page = response.xpath("//a[.='›']/@href").get()
        if next_page:
            yield Request(
                response.urljoin(next_page),
                callback=self.parse,
                meta={"property_type":response.meta["property_type"]})      
        
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id",response.url.split("/")[-1])    

        item_loader.add_value("external_source", "Urbanandrural_PySpider_united_kingdom")
        item_loader.add_xpath("title", "//div[@class='image_container container']//h2/text()")        
        item_loader.add_xpath("room_count", "//div[img[contains(@src,'bedroom')]]/span/text()")        
        item_loader.add_xpath("bathroom_count", "//div[img[contains(@src,'bathroom')]]/span/text()[.!='0']")
                
        address = response.xpath("//div[@class='image_container container']//h2/text()").extract_first()
        if address:
            item_loader.add_value("address", address.strip())
            city = address.split(",")[-1].strip()
            if city.replace(" ","").isalpha():
                item_loader.add_value("city", city)
            else:
                item_loader.add_value("zipcode", address.split(",")[-1].strip())
                item_loader.add_value("city", address.split(",")[-2].strip())

        rent = " ".join(response.xpath("//p[@class='price']/text()").extract())
        if rent:
            item_loader.add_value("rent_string",rent)
       
     
        furnished =response.xpath("//ul/li[contains(.,'furnished') or contains(.,'Furnished')]//text()").extract_first()    
        if furnished:
            if "unfurnished" in furnished.lower():
                item_loader.add_value("furnished", False)
            elif "furnished" in furnished.lower():
                item_loader.add_value("furnished", True)
                
        pets_allowed =response.xpath("//ul/li[contains(.,'Pets')]//text()").extract_first()    
        if pets_allowed:
            if "no " in pets_allowed.lower():
                item_loader.add_value("pets_allowed", False)
            else:
                item_loader.add_value("pets_allowed", True)
 
        terrace =response.xpath("//ul/li[contains(.,'Terrace')]//text()").extract_first()    
        if terrace:
            item_loader.add_value("terrace", True)

        images = [x for x in response.xpath("//div[@id='thumbnail_images']/div/a/@href").extract()]
        if images:
                item_loader.add_value("images", images)
        floor_plan_images = [x for x in response.xpath("//div[@id='floorplan0']/div//a/@href").extract()]
        if floor_plan_images:
                item_loader.add_value("floor_plan_images", floor_plan_images)          
        map_location = response.xpath("//div[@class='google-map-embed']/@data-location").get()
        if map_location:
            item_loader.add_value("latitude", map_location.split(",")[0].strip())
            item_loader.add_value("longitude", map_location.split(",")[1].strip())

        parking =response.xpath("//ul/li[contains(.,'Garage') or contains(.,'Parking') or contains(.,'PARKING') or contains(.,'GARAGE') ]//text()").extract_first()    
        if parking:
            item_loader.add_value("parking", True)
      
        desc = " ".join(response.xpath("//div[contains(@class,'full_description_large')]//text()[not(contains(.,'Read less'))]").extract())
        if desc:
            item_loader.add_value("description", desc.strip())

        item_loader.add_value("landlord_name", "Urban & Rural")

        phone = response.xpath("//a[contains(@href,'tel')]/@href").get()
        if phone:
            phone = phone.split(":")[1].strip()
            item_loader.add_value("landlord_phone", phone)
        
        email = response.xpath("//li[contains(.,'@urbanandrural.com')]/text()").get()
        if email:
            item_loader.add_value("landlord_email", email)   
        yield item_loader.load_item()