# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from python_spiders.helper import ItemClear
import re

class MySpider(Spider):
    name = 'realestatealliance_ie'
    execution_type='testing'
    country='ireland'
    locale='en'    
    thousand_separator = ','
    scale_separator = '.'    

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.realestatealliance.ie/search?sta=toLet&sta=let&st=rent&pt=residential&stygrp=3",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.realestatealliance.ie/search?sta=toLet&sta=let&st=rent&pt=residential&stygrp=2&stygrp=8&stygrp=9&stygrp=6",
                ],
                "property_type" : "house",
            },
        ]
        for url in start_urls:
            for item in url["url"]:
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url['property_type']})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='SearchResults']/div/div/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

        next_button = response.xpath("//a[contains(@class,'-next')]/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse, meta={"property_type":response.meta["property_type"]})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("/")[-1])
        item_loader.add_value("external_source", "Realestatealliance_PySpider_ireland")
        item_loader.add_xpath("title", "//title/text()")
        
        rented = response.xpath("//div[contains(@class,'DetailsPageImg-status')]/text()").get()
        if rented and rented.strip() == "Let":  
            return

        address = " ".join(response.xpath("//h1[@class='Address ']/span//text()[normalize-space()]").extract())
        if address:
            item_loader.add_value("address", address.replace("\n","").strip())
        zipcode = " ".join(response.xpath("//h1//span[@class='Address-addressPostcode']/span/text()[normalize-space()]").extract())
        if zipcode:
            item_loader.add_value("zipcode", zipcode.strip())
        city = response.xpath("//span[@class='Address-addressRegion']/text()").get()
        if city:                   
            item_loader.add_value("city",city.replace(","," ").strip())
        
        rent = " ".join(response.xpath("//span[contains(@class,'Price--single')]//text()").extract())
        if rent:  
            item_loader.add_value("rent_string", rent) 

        square_meters = response.xpath("//li[@class='ListingFeatures-size']/span[@class='ListingFeatures-figure']/text()").get()
        if square_meters:      
            meters = square_meters.split("m")[0].replace("sq.","").strip()          
            item_loader.add_value("square_meters", int(float(meters)))

        item_loader.add_xpath("bathroom_count", "//li[@class='ListingFeatures-bathrooms']/span[@class='ListingFeatures-figure']/text()") 
        item_loader.add_xpath("room_count", "//li[@class='ListingFeatures-bedrooms']/span[@class='ListingFeatures-figure']/text()") 

        description = " ".join(response.xpath("//div[@class='ListingDescr-text']//text()").getall()).strip()   
        if description:
            item_loader.add_value("description", description.strip())

        item_loader.add_xpath("latitude", "//meta[@property='og:latitude']/@content")
        item_loader.add_xpath("longitude","//meta[@property='og:longitude']/@content")
            
        images = [x for x in response.xpath("//div[@class='Slideshow-thumbs']/a/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)

        furnished = response.xpath("//li[@class='ListingFeatures-furnished']/span/text()").extract_first()
        if furnished:
            if "unfurnished" in furnished.lower():
                item_loader.add_value("furnished",False)
            elif "furnished" in furnished.lower():
                item_loader.add_value("furnished",True)
        landlord_phone = response.xpath("//script[contains(.,'name') and contains(.,'telephone')]/text()").extract_first()
        if landlord_phone:
            item_loader.add_value("landlord_phone",landlord_phone.split('"telephone": "')[-1].split('"')[0])
            
        item_loader.add_value("landlord_email", "info@rea.ie")
        item_loader.add_xpath("landlord_name", "//div[@id='PropertyEnquiryFormContainer']/p/strong/text()")
          
        yield item_loader.load_item()