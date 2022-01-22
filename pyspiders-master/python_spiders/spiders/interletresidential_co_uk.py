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
    name = 'interletresidential_co_uk'
    start_urls = ["https://www.interletresidential.co.uk/properties/"]
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    thousand_separator = ','
    scale_separator = '.'   
    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//a[.='View details']/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
        
        next_page = response.xpath("//a[.='Next »']/@href").get()
        if next_page:
            yield Request(response.urljoin(next_page), callback=self.parse)
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("external_link", response.url)

        desc = "".join(response.xpath("//div[@class='property-description']/p//text()").getall())
        if desc and ("apartment" in desc.lower() or "flat" in desc.lower() or "maisonette" in desc.lower()):
            item_loader.add_value("property_type", "apartment")
        elif desc and "house" in desc.lower():
             item_loader.add_value("property_type", "house")
        elif desc and "studio" in desc.lower():
             item_loader.add_value("property_type", "studio")
        elif desc and "students only" in desc.lower():
             item_loader.add_value("property_type", "student_apartment")
        else:
            return

        title = response.xpath("//div//h2//text()").extract_first()
        if title:
            item_loader.add_value("title", title.strip())

        address = response.xpath("//ul[@class='property-details']/li[strong[.='Area:']]/text()").extract_first()
        if address:
            city = address.split(",")[-2].strip()
            if city:
                item_loader.add_value("address", address.strip())
                item_loader.add_value("city", city)
            else:
                item_loader.add_value("address", title.replace("- 2021","").strip())
                item_loader.add_value("city", title.split(",")[-2].replace("- 2021","").strip())
            item_loader.add_value("zipcode", address.split(",")[-1].strip())
       
        item_loader.add_value("external_source", "Interletresidential_Co_PySpider_united_kingdom")
 
        room_count = response.xpath("//ul[@class='property-details']/li[strong[.='Bedrooms:']]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
  
        bathroom_count=response.xpath("//ul[@class='property-details']/li[strong[.='Bathrooms:']]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)

        item_loader.add_value("external_id", response.url.split("/")[-2].strip()) 

        energy = response.xpath("//div[@class='property-description']/p[contains(.,'EPC')]//text()").get()
        if energy:
            energy_label = energy.replace("\u00a0"," ").strip().split(" ")[-1]
            if energy_label.isalpha():
                item_loader.add_value("energy_label",energy_label) 
      
        available_date = response.xpath("//ul[@class='property-details']/li[strong[.='Available:']]/text()[not(contains(.,'NOW'))]").get()
        if available_date:       
            try: 
                date_parsed = dateparser.parse(available_date.strip(), languages=['en'])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
            except:
                pass
           
        furnished = response.xpath("//ul[@class='property-details']/li[strong[.='Furnished:']]/text()").get()
        if furnished:
            if "yes" in furnished.lower():
                item_loader.add_value("furnished", True) 
            elif "no" in furnished.lower():
                item_loader.add_value("furnished", False) 
        pets = response.xpath("//div[@class='property-description']/p[contains(.,' PETS') or contains(.,' pets')]//text()").get()
        if pets:
            if "yes" in pets.lower():
                item_loader.add_value("pets_allowed", True) 
            elif "no" in pets.lower():
                item_loader.add_value("pets_allowed", False) 

        rent =response.xpath("//span[@class='property-view-price']//text()").extract_first()
        if rent:
            item_loader.add_value("rent_string", rent)    
       
        desc = " ".join(response.xpath("//div[@class='property-description']/p//text()").extract())
        if desc:
            item_loader.add_value("description",desc.strip())
            if "parking" in desc:
                item_loader.add_value("parking", True) 

     
        images = [response.urljoin(x) for x in response.xpath("//div[@class='gallery']//img/@src").extract()]
        if images:
            item_loader.add_value("images", images)   
    
        item_loader.add_value("landlord_phone", "029 20 40 00 00")
        item_loader.add_value("landlord_email", "lettings@interletresidential.co.uk")
        item_loader.add_value("landlord_name", "Interlet Residential")        

        yield item_loader.load_item()
