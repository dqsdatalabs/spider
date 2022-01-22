# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import math 
import re
import dateparser

class MySpider(Spider):
    name = 'mafstudents_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    external_source='MafStudents_Co_PySpider_united_kingdom'

    def start_requests(self):
        start_urls = [
            {"url": "https://mafstudents.co.uk/search/?showstc=on&showsold=on&department=Student&searchable=&num_bedrooms=", "property_type": "student_apartment"},          
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # # 1. FOLLOWING      
    def parse(self, response, **kwargs):
        listings = response.xpath('//div[@class="thumb-description"]/h3/a')
        for listing in listings:
            property_url = response.urljoin(listing.xpath("./@href").extract_first())
            address = listing.xpath("normalize-space(./text())").extract_first()
            yield scrapy.Request(
                url=property_url, 
                callback=self.populate_item, 
                meta={'request_url':property_url,
                    'property_type':response.meta.get('property_type'),
                    'address':address})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_id",take_external_id(response.url))

        title=response.xpath("(//title/text())[1]").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title",title)

        rent = response.xpath("(//span[@class='price']/strong/text())[1]").get()
        if rent:
            rent = rent.split('£')[-1].lower().split('p')[0].strip().replace(',', '').replace('\xa0', '')
            item_loader.add_value("rent", str(int(float(rent)) * 4))
            item_loader.add_value("currency", 'GBP')

        room_count = response.xpath("(//div[@class='col-sm-5 col-md-5']/p/text())[2]").get()
        if int(room_count) > 0:           
            item_loader.add_value("room_count", room_count)
        else:
            is_studio = response.xpath("//title[contains(.,'Studio')]/text()").get()
            if is_studio:
                room_count = 1
                item_loader.add_value("room_count", room_count)
            
        bathroom = response.xpath("(//div[@class='col-sm-5 col-md-5']/p/text())[1]").get()
        if int(bathroom) > 0:    
            item_loader.add_value("bathroom_count", bathroom)
        
        available_date = response.xpath("(//div[@class='col-sm-7 col-md-7']/p/text())[1]").get()
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))

        address = response.meta.get('address')
        if address:
            item_loader.add_value("address", address)
            city = address.split(",")[-1].strip()
            item_loader.add_value("city", city)

        lat_lng = response.xpath("//script[contains(.,'latitude')]/text()").get()
        if lat_lng:
            latitude = lat_lng.split("latitude")[-1].split(":")[1].split('"')[1].strip()
            longitude = lat_lng.split("longitude")[-1].split(":")[1].split('"')[1].strip()
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)

        furnished = "".join(response.xpath("(//div[@class='col-sm-7 col-md-7']/p/strong/text())[2]").getall())
        if furnished:
                item_loader.add_value("furnished",True)

        desc = "".join(response.xpath("//div[@class='short-description']/text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
        
        images = [x for x in response.xpath("//div[@class='carousel-inner']/div/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))

        floor_plan_images = [response.urljoin(x) for x in response.xpath("//div[@id='property-floorplans']//img/@src").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)

        item_loader.add_value("landlord_name", "MAF Properties")
        item_loader.add_value("landlord_phone", "0114 272 6006")
        item_loader.add_value("landlord_email","info@mafstudents.co.uk")
        
        yield item_loader.load_item()



def take_external_id(external_link):
    list1 = external_link.split("details/")
    list2 = list1[1].split("/")
    result = list2[0]
    return result
