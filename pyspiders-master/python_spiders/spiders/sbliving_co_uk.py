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
    name = 'sbliving_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'    
    thousand_separator = ','
    scale_separator = '.'  

    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://sbliving.co.uk/property-search/?address_keyword=&department=residential-lettings&property_cat=Professional&maximum_rent=&maximum_price=&minimum_bedrooms=&maximum_bedrooms=",
                ],
                "property_type": ""
            },
	        {
                "url": [
                    "https://sbliving.co.uk/property-search/?address_keyword=&department=residential-lettings&property_cat=Room&maximum_rent=&maximum_price=&minimum_bedrooms=&maximum_bedrooms="
                ],
                "property_type": "room"
            },
            {
                "url": [
                    "https://sbliving.co.uk/property-search/?address_keyword=&department=residential-lettings&property_cat=Student&maximum_rent=&maximum_price=&minimum_bedrooms=&maximum_bedrooms=",
                ],
                "property_type": "student_apartment"
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                yield Request(
                    url=item,
                    callback=self.parse,
                    meta={'property_type': url.get('property_type')}
                )

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//a[contains(.,'View Property')]/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
        
        next_page = response.xpath("//a[contains(.,'Next')]/@href").get()
        if next_page:
            yield Request(next_page, callback=self.parse, meta={"property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        rented = response.xpath("//div[@class='price']//text()[contains(.,'Let Agreed')]").get()
        if rented:
            return
        
        desc = "".join(response.xpath("//div[@class='summary']//p//text()").getall())
        
        prop_type = response.meta.get('property_type')
        if prop_type:
            item_loader.add_value("property_type", prop_type)
        elif get_p_type_string(desc):
            item_loader.add_value("property_type", get_p_type_string(desc))
        else: return
            
        item_loader.add_value("external_source", "Sbliving_Co_PySpider_united_kingdom")
  
        item_loader.add_xpath("title", "//div[@class='headerSingleProperty']//h1[@class='entry-title']/text()")
        externalid=response.xpath("//link[@rel='shortlink']/@href").get()
        if externalid:
            item_loader.add_value("external_id",externalid.split("=")[-1])
        address = response.xpath("//div[@class='headerSingleProperty']//h1[@class='entry-title']/text()").get()
        if address:
            item_loader.add_value("address", address)
            item_loader.add_value("city", address.split(",")[-2].strip())
            item_loader.add_value("zipcode", address.split(",")[-1].strip())
        
        rent=response.xpath("//div[@class='container']//div[@class='price']//text()").get()
        if rent:
            if "pw" in rent.lower():
                rent1=rent.split("£")[-1].split("p")[0].strip()
                item_loader.add_value("rent",int(rent1)*4)
            elif "cm" in rent.lower():
                rent1=rent.split("£")[-1].split("p")[0].strip()
                item_loader.add_value("rent",rent1)
        item_loader.add_value("currency","GBP")


        deposit = response.xpath("//p[contains(.,'Deposit: ')]/text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit)
       
        desc = " ".join(response.xpath("//div[@class='summary']/p//text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())
 
        item_loader.add_xpath("room_count", "//li[strong[.=' Bedrooms:']]/span/text()")
        item_loader.add_xpath("bathroom_count", "//li[strong[.=' Bathrooms:']]/span/text()")
        
        available_date = "".join(response.xpath("//div[@class='available_date']/text()").getall())
        if available_date:
            date_parsed = dateparser.parse(available_date.split(":")[-1], date_formats=["%d/%m/%Y"])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))

        parking = response.xpath("//div[@class='features']//li[contains(.,'Parking')]/text()").get()
        if parking:
            item_loader.add_value("parking", True)
        furnished = response.xpath("//div[@class='features']//li[contains(.,'furnished') or contains(.,'Furnished') or contains(.,'furnishings')]/text()").get()
        if furnished:
            if "unfurnished" in furnished.lower():
                item_loader.add_value("furnished", False)
            elif "furnished" in furnished.lower() or "furnishings" in furnished.lower():
                item_loader.add_value("furnished", True)

        images = [x for x in response.xpath("//div[@id='grand_slider']/div[@class='slides']//img/@data-lazy-src").getall()]
        if images:
            item_loader.add_value("images", images)        
        floor_plan_images = [x for x in response.xpath("//div[@id='tab_floorplan']//div[@class='floorplan']//img/@data-lazy-src").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)        
        latitude_longitude = response.xpath("//span[@class='openGallery']/@data-items[contains(.,'q=loc:')]").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('=loc:')[1].split(',')[0]
            longitude = latitude_longitude.split('=loc:')[1].split(',')[1].split('"')[0].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
        item_loader.add_value("landlord_name", "sbliving")
        item_loader.add_value("landlord_phone", "0113 278 8651")
        item_loader.add_value("landlord_email", "admin@sbliving.co.uk")
        yield item_loader.load_item()


def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower() or "unit" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "home" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "cottage" in p_type_string.lower() or "detached" in p_type_string.lower()):
        return "house"
    elif p_type_string and ("villa" in p_type_string.lower() or "bedroom" in p_type_string.lower()):
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    else:
        return None