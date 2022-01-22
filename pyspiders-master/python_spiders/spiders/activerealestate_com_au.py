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
import re
class MySpider(Spider):
    name = 'activerealestate_com_au'
    execution_type='testing'
    country='australia'
    locale='en'

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "http://www.activerealestate.com.au/rent?search=&property_type=Apartment&min_rent=0&max_rent=&bedrooms=&bathrooms=",
                    "http://www.activerealestate.com.au/rent?search=&property_type=Flat&min_rent=0&max_rent=&bedrooms=&bathrooms=",
                    "http://www.activerealestate.com.au/rent?search=&property_type=Unit&min_rent=0&max_rent=&bedrooms=&bathrooms=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "http://www.activerealestate.com.au/rent?search=&property_type=Duplex&min_rent=0&max_rent=&bedrooms=&bathrooms=",
                    "http://www.activerealestate.com.au/rent?search=&property_type=Semi-detached&min_rent=0&max_rent=&bedrooms=&bathrooms=",
                    "http://www.activerealestate.com.au/rent?search=&property_type=House&min_rent=0&max_rent=&bedrooms=&bathrooms=",
                    "http://www.activerealestate.com.au/rent?search=&property_type=Terrace&min_rent=0&max_rent=&bedrooms=&bathrooms=",
                    "http://www.activerealestate.com.au/rent?search=&property_type=Townhouse&min_rent=0&max_rent=&bedrooms=&bathrooms=",
                    "http://www.activerealestate.com.au/rent?search=&property_type=Villa&min_rent=0&max_rent=&bedrooms=&bathrooms=",
                ],
                "property_type" : "house"
            },
                        {
                "url" : [
                    "http://www.activerealestate.com.au/rent?search=&property_type=Studio&min_rent=0&max_rent=&bedrooms=&bathrooms=",
                ],
                "property_type" : "studio",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='propertyItem']//h4/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Activerealestate_Com_PySpider_australia")
        item_loader.add_value("external_id", response.url.split("id=")[1])          
        item_loader.add_xpath("title","//div[@class='container']/h1//text()")
        item_loader.add_xpath("room_count", "//li[contains(.,'Bedrooms ')]/span/text()")
        item_loader.add_xpath("bathroom_count", "//li[contains(.,'Bathrooms ')]/span/text()")
        rent = response.xpath("//p[@class='price']//text()").get()
        if rent:
            rent = rent.split("$")[-1].lower().split('p')[0].strip().replace(',', '')
            item_loader.add_value("rent", int(float(rent)) * 4)
        item_loader.add_value("currency", 'AUD')
        
        city = response.xpath("//li[contains(.,'Location ')]/span/text()").get()
        if city:
            item_loader.add_value("city", city.strip())
       
        item_loader.add_xpath("address", "//div[@class='container']/h1//text()")
        parking = response.xpath("//li[contains(.,'Car Spaces ')]/span/text()").get()
        if parking:
            item_loader.add_value("parking", True) if parking.strip() != "0" else item_loader.add_value("parking", False)
        balcony = response.xpath("//ul[@class='features']/li//text()[contains(.,'Balcony')]").get()
        if balcony:
            item_loader.add_value("balcony", True) 
        dishwasher = response.xpath("//ul[@class='features']/li//text()[contains(.,'Dishwasher')]").get()
        if dishwasher:
            item_loader.add_value("dishwasher", True) 
        swimming_pool = response.xpath("//ul[@class='features']/li//text()[contains(.,'Swimming Pool')]").get()
        if swimming_pool:
            item_loader.add_value("swimming_pool", True) 
        
        available_date = response.xpath("//p[contains(@class,'forSale')]//following-sibling::p//text()[contains(.,'AVAILABLE')]").get()
        if available_date:
            if not "now" in available_date.lower():
                available_date = available_date.split(":")[1].replace("(pending works)","").strip()
                date_parsed = dateparser.parse(available_date, date_formats=["%d %m %Y"])
                if date_parsed:
                    item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))
        
        desc = " ".join(response.xpath("//p[contains(@class,'forSale')]//following-sibling::p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        script_map = response.xpath("//script[contains(.,').setView([')]/text()").get()
        if script_map:
            latlng = script_map.split(").setView([")[1].split("]")[0]
            item_loader.add_value("latitude", latlng.split(",")[0].strip())
            item_loader.add_value("longitude", latlng.split(",")[1].strip())
        images = [x for x in response.xpath("//ul[@class='bxslider2']/li/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
      
        item_loader.add_value("landlord_name", "Active Real Estate")
        item_loader.add_value("landlord_phone", "(07) 3721 8585")
        item_loader.add_value("landlord_email", "reception@activerealestate.com.au")

        yield item_loader.load_item()