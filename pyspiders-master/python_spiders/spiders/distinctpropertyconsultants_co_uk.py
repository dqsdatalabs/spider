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
    name = 'distinctpropertyconsultants_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en' 
    thousand_separator = ','
    scale_separator = '.' 
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.distinctpropertyconsultants.co.uk/properties.aspx?Mode=1&PropertyTypeGroup=2&PriceMin=0&PriceMax=0&Bedrooms=0&ShowSearch=1",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.distinctpropertyconsultants.co.uk/properties.aspx?Mode=1&PropertyTypeGroup=1&PriceMin=0&PriceMax=0&Bedrooms=0&ShowSearch=1",
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
        for item in response.xpath("//div[@class='info']/h3/a"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
         
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Distinctpropertyconsultants_Co_PySpider_united_kingdom")
     
        title = response.xpath("//div/h1/text()").extract_first()
        if title:
            address = title.strip()
            item_loader.add_value("title",title.strip() ) 
            item_loader.add_value("address", address)    
            city = ""           
            zipcode = address.split(",")[-1].strip()
            if zipcode.isalpha():
                zipcode = "" 
                city = address.split(",")[-1].strip()
            else:
                city = address.split(",")[-2].strip()
            if zipcode:
                item_loader.add_value("zipcode",zipcode)
            if city:
                if " and " in city:
                   city = city.split(" and ")[-1].strip()     
                item_loader.add_value("city",city)
            
        rent = response.xpath("//div[contains(@class,'price')]/span//text()[contains(.,'Price')]").extract_first()
        if rent:
            item_loader.add_value("rent_string",rent) 
        
        item_loader.add_xpath("room_count","//ul[@class='amenities']/li[i[@class='icon-bedrooms']]/text()") 
        item_loader.add_xpath("bathroom_count","//ul[@class='amenities']/li[i[@class='icon-bathrooms']]/text()") 

        item_loader.add_xpath("external_id", "//p[contains(.,'Property ID')]/span/text()")
        item_loader.add_value("landlord_email", "hello@distinctpropertyconsultants.co.uk")
        
        square_meters = response.xpath("//ul[@class='amenities']/li[i[@class='icon-area']]/text()[contains(.,'m²')]").extract_first()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("m")[0].strip()) 
        floor =response.xpath("//li[contains(.,'Floor') and not(contains(.,'Flooring'))]//text()").extract_first()    
        if floor:
            item_loader.add_value("floor", floor.split("Floor")[0].strip())
        energy =response.xpath("//li[contains(.,'EPC')]//text()").extract_first()    
        if energy:
            energy = energy.split(" ")[-1].strip()
            if energy in ["A","B","C","D","E","F","G"]:
                item_loader.add_value("energy_label", energy)
        furnished =response.xpath("//h2[contains(.,'furnished') or contains(.,'Furnished')]//text()").extract_first()    
        if furnished:
            if "unfurnished" in furnished.lower():
                item_loader.add_value("furnished", False)
            elif "furnished" in furnished.lower():
                item_loader.add_value("furnished", True)
        parking =response.xpath("//li[contains(.,'Parking')]//text()").extract_first()    
        if parking:
            item_loader.add_value("parking", True)
        deposit =response.xpath("//p/span[contains(@id,'blPropertyMainDescription')]//text()[contains(.,'Deposit') and contains(.,'£')]").extract_first()    
        if deposit:
            item_loader.add_value("deposit", deposit.replace(",","."))
        dishwasher =response.xpath("//li[contains(.,'Dishwasher')]//text()").extract_first()    
        if dishwasher:
            item_loader.add_value("dishwasher", True)
     
        images = [response.urljoin(x) for x in response.xpath("//div[@id='property-detail-thumbs']/div/img/@src").extract()]
        if images:
                item_loader.add_value("images", images)
        floor_plan_images = [response.urljoin(x) for x in response.xpath("//div[@id='tabFloorPlan']/img/@src").extract()]
        if floor_plan_images:
                item_loader.add_value("floor_plan_images", floor_plan_images)               
        script_map = response.xpath("substring-after(//div[@id='tabStreetView']/iframe/@src,'cbll=')").get()
        if script_map:
            item_loader.add_value("latitude", script_map.split('&cbp')[0].split(',')[0].strip())
            item_loader.add_value("longitude", script_map.split('&cbp')[0].split(',')[1].strip())
        desc = " ".join(response.xpath("//p/span[contains(@id,'blPropertyMainDescription')]//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())

        item_loader.add_value("landlord_name", "Distinct Property Consultants")
        item_loader.add_value("landlord_phone", "01295 234 750")     
       
        yield item_loader.load_item()