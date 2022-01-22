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
class MySpider(Spider):
    name = 'residence_housing_nl'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.residence-housing.nl/rent-listings?city=&min_price=&max_price=&bedrooms=&interior=&available_at_date=&house_type=Apartment&available_at=",
                    "https://www.residence-housing.nl/rent-listings?city=&min_price=&max_price=&bedrooms=&interior=&available_at_date=&house_type=Upstairs+apartment&available_at="
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.residence-housing.nl/rent-listings?city=&min_price=&max_price=&bedrooms=&interior=&available_at_date=&house_type=Family+house&available_at=",
                    "https://www.residence-housing.nl/rent-listings?city=&min_price=&max_price=&bedrooms=&interior=&available_at_date=&house_type=Hoekhuis&available_at=",
                    "https://www.residence-housing.nl/rent-listings?city=&min_price=&max_price=&bedrooms=&interior=&available_at_date=&house_type=Villa&available_at="
                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "https://www.residence-housing.nl/rent-listings?city=&min_price=&max_price=&bedrooms=&interior=&available_at_date=&house_type=Studio&available_at=",
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
        page = response.meta.get("page", 2)

        seen = False
        for item in response.xpath("//li[@class='listing']"):
            status = item.xpath("./div/p[@class='label']/text()").get()
            if status and ("binnenkort" in status.lower() or "verhuurd" in status.lower()):
                continue
            follow_url = response.urljoin(item.xpath("./div/@data-url").get())
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta['property_type']})
            seen = True
        
        if page == 2 or seen:
            p_url = response.url.split("&page=")[0] + f"&page={page}"
            yield Request(
                p_url,
                callback=self.parse,
                meta={'property_type': response.meta['property_type'], "page":page+1}
            )
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link",  response.url)
        item_loader.add_value("external_id",  response.url.split("-")[-1])
      
        item_loader.add_value("external_source", "Residence_Housing_PySpider_netherlands")
        item_loader.add_xpath("title", "//div[contains(@class,'no-mobile')]//h2[@class='address']//text()")
 
        city =response.xpath("substring-before(//div[contains(@class,'no-mobile')]//p[@class='district-city']//text(),'• ')").extract_first()
        if city:
            item_loader.add_value("city",city.strip() ) 
        address =response.xpath("//div[contains(@class,'no-mobile')]//h2[@class='address']//text()").extract_first()
        if address:
            if city:
                address = address + ", "+city
            item_loader.add_value("address",address.strip() ) 

        item_loader.add_xpath("deposit","//div[contains(@class,'more-info-pane')]//li[span[contains(.,'Borgbedrag')]]/span[2]//text()")                
               
        rent =response.xpath("//p[contains(@class,'features')]//text()").extract_first()
        if rent:     
           item_loader.add_value("rent_string", rent.split("•")[-1].split("- ")[0])   
         
        available_date = response.xpath("//div[contains(@class,'more-info-pane')]//li[span[contains(.,'Beschikbaar per')]]/span[2]//text()").extract_first() 
        if available_date:          
            date_parsed = dateparser.parse(available_date.strip(), languages=['nl'])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
  
        room_count = response.xpath("//div[contains(@class,'more-info-pane')]//li[span[contains(.,'Slaapkamers')]]/span[2]//text()").extract_first() 
        if room_count:   
            if room_count != "0":
                item_loader.add_value("room_count",room_count.strip())
            elif response.meta.get('property_type') == "studio":
                item_loader.add_value("room_count","1")

        bathroom_count = response.xpath("//div[contains(@class,'more-info-pane')]//li[span[contains(.,'Badkamers')]]/span[2]//text()").extract_first() 
        if bathroom_count:   
            item_loader.add_value("bathroom_count",bathroom_count.strip())

        square =response.xpath("//div[contains(@class,'more-info-pane')]//li[span[contains(.,'Woonoppervlakte')]]/span[2]//text()").extract_first()
        if square:
            square_meters =  square.split("m")[0].strip()
            item_loader.add_value("square_meters", square_meters) 

        furnished =response.xpath("//div[contains(@class,'more-info-pane')]//li[span[contains(.,'Interieur')]]/span[2]//text()").extract_first()     
        if furnished:
            if "gemeubileerd" in furnished.lower() or "gestoffeerd" in furnished.lower():
                item_loader.add_value("furnished", True)
            else:
                item_loader.add_value("furnished", False)

        washing_machine =response.xpath("//div[contains(@class,'more-info-pane')]//li[span[contains(.,'Wasmachine')]]/span[2]//text()").extract_first()    
        if washing_machine:
            if "true" in washing_machine.lower():
                item_loader.add_value("washing_machine", True)
            
        dishwasher =response.xpath("//div[contains(@class,'more-info-pane')]//li[span[contains(.,'Vaatwasser')]]/span[2]//text()").extract_first()    
        if dishwasher:
            if "true" in dishwasher.lower():
                item_loader.add_value("dishwasher", True)
            
        elevator =response.xpath("//div[contains(@class,'more-info-pane')]//li[span[contains(.,'Lift')]]/span[2]//text()").extract_first()    
        if elevator:
            if "ja" in elevator.lower():
                item_loader.add_value("elevator", True)
            
        terrace =response.xpath("//div[contains(@class,'more-info-pane')]//li[span[contains(.,'terras')]]/span[2]//text()").extract_first()    
        if terrace:
            if "ja" in terrace.lower():
                item_loader.add_value("terrace", True)
            
        parking =response.xpath("//div[contains(@class,'more-info-pane')]//li[span[contains(.,'Parkeren')]]/span[2]//text()").extract_first()    
        if parking:
            if "Nee" in parking:
                item_loader.add_value("parking", False)
            else:
                item_loader.add_value("parking", True)
        balcony =response.xpath("//div[contains(@class,'more-info-pane')]//li[span[contains(.,'Balkon')]]/span[2]//text()").extract_first()    
        if balcony:
            if "ja" in balcony.lower():
                item_loader.add_value("balcony", True)
        desc = " ".join(response.xpath("//div[contains(@class,'description')]//p//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
              
        images = [x for x in response.xpath("//ul[@class='slides']/li/@data-src").extract()]
        if images:
                item_loader.add_value("images", images)

        item_loader.add_value("landlord_name", "Residence Housing & Relocation")
        item_loader.add_value("landlord_phone", "+31 (0)43 3210508")
        item_loader.add_value("landlord_email", "info@residence-housing.nl")

        lat = response.xpath("//div[@id='mapview-canvas']/@data-lat").get()
        lng = response.xpath("//div[@id='mapview-canvas']/@data-lng").get()
        if lat and lng:
            item_loader.add_value("latitude", lat.strip())
            item_loader.add_value("longitude", lng.strip())
    
        yield item_loader.load_item()