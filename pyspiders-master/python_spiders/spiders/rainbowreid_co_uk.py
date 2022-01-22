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
import dateparser

class MySpider(Spider):
    name = 'rainbowreid_co_uk'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    external_source='Rainbowreid_Co_PySpider_united_kingdom'

    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://rainbowreid.co.uk/for-rent/",
                ],
            },

        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                yield Request(
                    url=item,
                    callback=self.parse,
                )
    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//div[@class='listing-thumb']/a/@href").extract():
            follow_url = response.urljoin(item)
            print(follow_url)
            yield Request(follow_url, callback=self.populate_item)
        
        next_page = response.xpath("//div[@class='pagination-wrap']//li[@class='page-item']//a[contains(@class,'page-link')][contains(@aria-label,'Next')]/@href").get()
        if next_page:
            yield Request(response.urljoin(next_page), callback=self.parse)
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("external_link", response.url)

        p_type = response.url.split("/")[-2]
        if p_type and ("apartment" in p_type.lower() or "flat" in p_type.lower() or "maisonette" in p_type.lower()):
            if "studio" in response.url:
                item_loader.add_value("property_type", "studio")
            else:
                item_loader.add_value("property_type", "apartment")
        elif p_type and "house" in p_type.lower():
            item_loader.add_value("property_type", "house")
        elif p_type and "studio" in p_type.lower():
            item_loader.add_value("property_type", "studio")
        elif p_type and "students only" in p_type.lower():
            item_loader.add_value("property_type", "student_apartment")
        else:
            return

        item_loader.add_value("external_source", self.external_source)

        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", title.strip())

        external_id = response.xpath("//li[contains(.,'Property ID:')]//span//text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id)        

        address = response.xpath("//li[contains(.,'Address')]//span//text()").get()
        if address:
            item_loader.add_value("address", address)
        
        city = response.xpath("//li[contains(.,'City')]//span//text()").get()
        if city:
            item_loader.add_value("city", city)

        zipcode = response.xpath("//li[contains(.,'Zip/Postal Code')]//span//text()").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode)

        description = " ".join(response.xpath("//div[@class='block-content-wrap']/p/text()").getall())   
        if description:
            item_loader.add_value("description", description)

        square_meters = response.xpath("//li[contains(.,'Property Size:')]//span//text()").get()
        if square_meters:
            if square_meters and "m" in square_meters.lower():
                square_meters = square_meters.split("m²")[0]
            item_loader.add_value("square_meters", square_meters)

        room_count = response.xpath("//li[contains(.,'Bedrooms:')]//span//text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)

        bathroom_count = response.xpath("//li[contains(.,'Bathrooms:')]//span//text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)

        rent = response.xpath("//li[contains(.,'Price:')]//span//text()").get()
        if rent:
            rent = rent.split("£")[1]
            if rent and "mo" in rent.lower():
                rent = rent.split("/mo")[0]
            if rent and "," in rent:
                rent = rent.replace(",","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", 'GBP')
        
        images = [response.urljoin(x) for x in response.xpath("//div[@class='row']//img[@class='img-fluid']/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))
    
        latitude_longitude = response.xpath("//script[@id='houzez-single-property-map-js-extra']/text()").get()
        if latitude_longitude:
            latitude= latitude_longitude.split('lat":"')[-1].split('",')[0]
            longitude = latitude_longitude.split('lng":"')[-1].split('",')[0].strip()
            if latitude and "cdata" not in latitude.lower():
                item_loader.add_value("latitude", latitude)
            if longitude and "cdata" not in longitude.lower():
                item_loader.add_value("longitude", longitude)

        item_loader.add_value("landlord_name", "Rainbow Reid")
        item_loader.add_value("landlord_phone", "020 8830 0181")
        item_loader.add_value("landlord_email", "info@rainbowreid.co.uk")

        yield item_loader.load_item()
