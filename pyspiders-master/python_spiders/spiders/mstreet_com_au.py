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
    name = 'mstreet_com_au' 
    execution_type='testing'
    country='australia'
    locale='en'
    def start_requests(self): 
        start_urls = [
            {
                "url" : [
                    "http://mstreet.com.au/listings/page/1/?keyword=&min=&max=&listing-category=for-rent&location=&listing-type=apartment&bedrooms=&bathrooms=",
                    "http://mstreet.com.au/listings/page/1/?keyword=&min=&max=&listing-category=for-rent&location=&listing-type=unit&bedrooms=&bathrooms=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [ 
                    "http://mstreet.com.au/listings/page/1/?keyword=&min=&max=&listing-category=for-rent&location=&listing-type=house&bedrooms=&bathrooms=",
                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "http://mstreet.com.au/listings/page/1/?keyword=&min=&max=&listing-category=for-rent&location=&listing-type=studio&bedrooms=&bathrooms=",
                ],
                "property_type" : "studio"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item, callback=self.parse, meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get("page", 2)
        seen = False

        for item in response.xpath("//div[@class='wpsight-listings']/div/div//div[@class='wpsight-listing-image']/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

        if page == 2 or seen:
            follow_url = response.url.replace("/page/" + str(page - 1), "/page/" + str(page))
            yield Request(follow_url, callback=self.parse, meta={'property_type': response.meta["property_type"], 'page':page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Mstreet_Com_PySpider_australia")
        item_loader.add_value("external_id", response.url.split("listing/")[1].split("-")[0])
        
        title = response.xpath("normalize-space(//h1[@class='entry-title']/text())").get()
        if title:
            item_loader.add_value("title", title)
            if "Deposit Taken" in title:
                return
            
            # if " in " in title.lower():
            #     address = title.lower().split(" in ")[1].strip()
            #     item_loader.add_value("address", address)
            #     item_loader.add_value("city", address)
        address=response.url 
        if address:
            address=address.split("-")
            addre=''
            for add in address:
                num=add.isalpha()
                if num:
                    addre+=' '+add
                num1=re.findall("\d+",add)
                if num1:
                    zip="NSW "+num1[-1]
            item_loader.add_value("address",addre.upper())
            item_loader.add_value("zipcode",zip)
            item_loader.add_value("city","Petersham")

        
        item_loader.add_xpath("rent", "//span[@itemprop='price']/@content")
        item_loader.add_value("currency", "AUD")
        
        room_count = response.xpath("//span[@class='listing-details-label'][contains(.,'Bed')]/following-sibling::span/text()").get()
        item_loader.add_value("room_count", room_count)
        
        bathroom_count = response.xpath("//span[@class='listing-details-label'][contains(.,'Bath')]/following-sibling::span/text()").get()
        item_loader.add_value("bathroom_count", bathroom_count)
        
        desc = " ".join(response.xpath("//div[@itemprop='description']//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        if "sqm" in desc:
            square_meters = desc.split("sqm")[0].strip().split(" ")[-1]
            item_loader.add_value("square_meters", square_meters)
        
        if "floor " in desc:
            floor = desc.split("floor ")[0].strip().split(" ")[-1]
            item_loader.add_value("floor", floor)
        
        images = [x for x in response.xpath("//span[@class='image']//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        latitude_longitude = response.xpath("//script[contains(.,'LatLng')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('LatLng(')[1].split(',')[0]
            longitude = latitude_longitude.split('LatLng(')[1].split(',')[1].split(')')[0].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
        
        parking = response.xpath("//span[@class='listing-details-label'][contains(.,'Parking')]/following-sibling::span/text()[.!='0']").get()
        if parking:
            item_loader.add_value("parking", True)
        
        landlord_name = "".join(response.xpath("//div[@itemprop='name']/text()").getall())
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_xpath("landlord_phone", "//span[contains(@class,'agent-phone')]/text()")
        
        yield item_loader.load_item()