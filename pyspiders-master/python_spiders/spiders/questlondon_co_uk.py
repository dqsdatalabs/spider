# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from urllib.parse import urljoin
import re
class MySpider(Spider):

    name = 'questlondon_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    
    start_urls = ["https://questlondon.co.uk/for-rent/"]
    
    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='phone']/a[contains(@class,'btn btn-primary')]/@href").extract():
            f_url = response.urljoin(item)
            yield Request(
                f_url, 
                callback=self.populate_item, 
            )
        
        next_page = response.xpath("//a[@rel='Next']/@href").get()
        if next_page:
            yield Request(
                url=response.urljoin(next_page),
                callback=self.parse,
            )
        
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)

        prop_type = "".join(response.xpath("//li[@class='prop_type']/text()").extract())
        if prop_type and ("apartment" in prop_type.lower() or "flat" in prop_type.lower()):
            item_loader.add_value("property_type", "apartment")
        elif prop_type and "house" in prop_type.lower():
            item_loader.add_value("property_type", "house")
        elif prop_type and "room" in prop_type.lower():
            item_loader.add_value("property_type", "room")
        elif prop_type and "studio" in prop_type.lower():
            item_loader.add_value("property_type", "studio")
        else:
            return
        
        item_loader.add_value("external_source", "Questlondon_PySpider_"+ self.country + "_" + self.locale)

        title=response.xpath("//div//h1//text()").extract_first()
        if title:
            item_loader.add_value("title",title)
            if "TERRACE" in title:
                item_loader.add_value("terrace",True)

        address=response.xpath("//div[@class='header-left']//address//text()").extract_first()
        if address:
            item_loader.add_value("address",address)   
            zipcode = ""
            city =""
            address_city = address.split(",")[-1].strip()
            if "UK" in address_city:
                address_city = address.split(",")[-2].strip() 
            if len(address_city.split(" "))==2:               
                zipcode =address_city.split(" ")[-1].strip()
                city = address_city.replace(zipcode,"").strip()
            elif len(address_city.split(" "))>2:               
                zipcode =address_city.split(" ")[-2].strip()+" "+ address_city.split(" ")[-1].strip()
                city = address_city.replace(zipcode,"").strip()
            else:
                city = address_city.strip()
            if city.isalpha():
                item_loader.add_value("city",city)   
            if zipcode:
                item_loader.add_value("zipcode",zipcode)  
            elif not zipcode and title:
                zipcode = title.strip().split(" ")[-1]
                if not zipcode.isalpha():
                    item_loader.add_value("zipcode",zipcode) 
                


        rent = response.xpath("//ul//li[strong[contains(.,'Price')]]/text()").extract_first()
        if rent:
            if "Per Week" in rent:
                numbers = re.findall(r'\d+(?:\.\d+)?', rent.replace(",","."))
                if numbers:
                    rent = int(numbers[0].replace(".",""))*4
                    rent = "£"+str(rent)
            item_loader.add_value("rent_string", rent.replace(",","."))  
          
        room = response.xpath("//ul//li[strong[contains(.,'Bedroom')]]/text()").extract_first()
        if room:
            item_loader.add_value("room_count", room.strip())
        floor = response.xpath("//div[contains(@class,'property-description')]//p//text()[contains(.,'Floor') and not(contains(.,'Flooring'))]").extract_first()
        if floor:
            item_loader.add_value("floor", floor.split("Floor")[0].replace("\u2013","").strip())
        bathroom = response.xpath("//ul//li[strong[contains(.,'Bathrooms')]]/text()").extract_first()
        if bathroom:
            if "&" in bathroom:
                bath_count1 = bathroom.split("&")[0].strip()
                bath_count2 = bathroom.split("&")[1].strip().split(" ")[0]
                if bath_count1.isdigit() and bath_count2.isdigit():
                    bathroom = int(bath_count1)+int(bath_count2)
            if not str(bathroom).strip().isdigit():
                bathroom = bathroom.strip().split(" ")[0]
            item_loader.add_value("bathroom_count", bathroom)
      
        desc = "".join(response.xpath("//div[contains(@class,'property-description')]//p//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
            if "furnished" in desc.lower():
                item_loader.add_value("furnished", True)
            if "balcony" in desc.lower():
                item_loader.add_value("balcony", True)
            if "parking" in desc.lower():
                item_loader.add_value("parking", True)
            if "lift" in desc.lower():
                item_loader.add_value("elevator", True)
            if "terrace" in desc.lower():
                item_loader.add_value("terrace", True)
            if "washing machine" in desc.lower():
                item_loader.add_value("washing_machine", True)
            if "swimming pool" in desc.lower():
                item_loader.add_value("swimming_pool", True)
        map_coordinate = response.xpath("//script[@type='text/javascript']//text()[contains(.,'lng')]").extract_first()
        if map_coordinate:
            lat = map_coordinate.split('"property_lat":"')[1].split('"')[0]
            lng = map_coordinate.split('"property_lng":"')[1].split('"')[0]
            if lat and lng:
                item_loader.add_value("longitude", lng)
                item_loader.add_value("latitude", lat)
            
        images = [response.urljoin(x) for x in response.xpath("//div[@class='detail-slider-nav-wrap']//img/@src").extract()]
        if images:
            item_loader.add_value("images", images)      
        else:
            img=response.xpath("//div[@id='gallery']/@style").extract() 
            if img:
                images=[]
                for x in img:
                    image = x.split("background-image: url(")[1].split(")")[0]
                    images.append(response.urljoin(image))
                if images:
                    item_loader.add_value("images",  list(set(images)))

        item_loader.add_value("landlord_phone", "020 3620 7000")
        item_loader.add_value("landlord_email", "info@questlondon.co.uk")
        item_loader.add_value("landlord_name", "Quest London")
      
        yield item_loader.load_item()

