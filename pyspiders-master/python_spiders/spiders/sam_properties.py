# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import re
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json

class MySpider(Spider):
    name = 'sam_properties' 
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    def start_requests(self):
        start_urls = [
            {
                "url": ["https://www.sam.properties/search/?keyword=&status%5B%5D=to-let&type%5B%5D=flat-apartment&bedrooms=&bathrooms=&property_id=&available-datef5dcd5efb566d6=&min-price=500&max-price=3000"
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "https://www.sam.properties/search/?keyword=&status%5B%5D=to-let&type%5B%5D=bungalow&type%5B%5D=house&bedrooms=&bathrooms=&property_id=&available-datef5dcd5efb566d6=&min-price=500&max-price=3000"
                ],
                "property_type": "house"
            }
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                yield Request(
                    url=item,
                    callback=self.parse,
                    meta={
                        'property_type': url.get('property_type'),
                        'base_url': item
                    }
                )

    # 1. FOLLOWING
    def parse(self, response):
     
        for item in response.xpath("//div[contains(@class,'item-wrap item-wrap-')]"):
            follow_url = item.xpath(".//div[@class='listing-thumb']/a/@href").get()
            studio_type = item.xpath(".//h2/a/text()[contains(.,'Studio ')]").get()
            property_type =  response.meta.get('property_type')
            if studio_type:
                property_type = "studio"

            yield Request(follow_url, callback=self.populate_item, meta={"property_type": property_type})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", "SamProperties_PySpider_united_kingdom")

        title = response.xpath("//h1/text()").get()
        if title:
            item_loader.add_value("title", title.strip())

        address = response.xpath("//li[strong[contains(.,'Address')]]/span/text()").get()
        if address:
            item_loader.add_value("address", address)
        
        zipcode = response.xpath("//li[strong[contains(.,'Zip')]]/span/text()").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode)
        
        city = response.xpath("//li[strong[contains(.,'county')]]/span/text()").get()
        if city:
            item_loader.add_value("city", city)
        
        rent = response.xpath("//li[@class='item-price']/text()").get()
        if rent:
            rent = rent.split("/")[0].replace("£","").replace(",","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "GBP")
        
        description = "".join(response.xpath("//div[@class='block-content-wrap']/p//text()").getall())
        if description:
            item_loader.add_value("description", description.strip())
        
        parking = response.xpath("//li[contains(.,'parking')]")
        if parking:
            item_loader.add_value("parking", True)
        
        item_loader.add_xpath("room_count", "//li[strong[contains(.,'Bedroom')]]/span/text()")
        item_loader.add_xpath("bathroom_count", "//li[strong[contains(.,'Bathroom')]]/span/text()")
        
        import dateparser
        available_date = response.xpath("//li[strong[contains(.,'Available')]]/span/text()").get()
        if available_date and "now" not in available_date.lower():
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        external_id= response.xpath("//li[strong[contains(.,'Property ID')]]/span/text()").get()
        item_loader.add_value("external_id", external_id)
        externalidcheck=item_loader.get_output_value("external_id")
        if not externalidcheck:
            externalid=response.xpath("//link[@rel='shortlink']/@href").get()
            if externalid:
                item_loader.add_value("external_id",externalid.split("=")[-1])
        
        images = [x for x in response.xpath("//img[@class='img-fluid']//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        latitude_longitude = response.xpath("//script[contains(.,'lng')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('"lat":"')[1].split('"')[0]
            longitude = latitude_longitude.split('"lng":"')[1].split('"')[0].strip()
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
        features=response.xpath("//ul[@class='list-3-cols list-unstyled']//li//text()").getall()
        if features:
            for i in features:
                if "furnished" in i.lower():
                    item_loader.add_value("furnished",True)
        
        item_loader.add_value("landlord_name", "Sam Properties")
        item_loader.add_value("landlord_phone", "0161 610 8383")
        item_loader.add_value("landlord_email", "enquiries@sam.properties")
        
        yield item_loader.load_item()