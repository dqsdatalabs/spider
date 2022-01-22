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
    name = 'gewoonmakelaardij_nl'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.gewoonmakelaardij.nl/zoekopdracht/?keyword=&property_id=&status%5B%5D=te-huur&min-price=&max-price=&type%5B%5D=appartement&location=&min-area=&max-area=",
                ],
                "property_type" : "apartment",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//h2[@class='property-title']/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

        next_page = response.xpath("//a[@rel='Next']/@href").get()
        if next_page:
            yield Request(
                response.urljoin(next_page),
                callback=self.parse,
                meta={"property_type":response.meta["property_type"]}
            )    
        
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Gewoonmakelaardij_PySpider_netherlands")        
        item_loader.add_xpath("title", "//div/h1/text()")
        item_loader.add_xpath("external_id", "//li[div[contains(.,'Object ID')]]/div[2]//text()")
        item_loader.add_xpath("city", "//li[div[contains(.,'Plaatsnaam')]]/div[2]//text()")
        item_loader.add_xpath("zipcode", "//li[div[contains(.,'Postcode')]]/div[2]//text()")
      
        address = response.xpath("//div/h1/text()").extract_first()
        if address:  
            item_loader.add_value("address",address.strip() )        
      
        room_count = response.xpath("//li[div[contains(.,'Slaapkamers')]]/div[2]//text()").extract_first() 
        if room_count: 
            item_loader.add_value("room_count",room_count) 
        else:
            room_count = response.xpath("//li[div[contains(.,'Kamers')]]/div[2]//text()").extract_first() 
            if room_count: 
                item_loader.add_value("room_count",room_count) 

        rent = response.xpath("//li[div[contains(.,'Huurprijs')]]/div[2]//text()").extract_first() 
        if rent: 
            item_loader.add_value("rent_string",rent)   
        deposit = response.xpath("//li[div[contains(.,'Waarborgsom')]]/div[2]//text()").extract_first() 
        if deposit: 
            item_loader.add_value("deposit",deposit)   
        else:
            deposit = response.xpath("//div[@id='description']//div[@class='panel1']//text()[contains(.,'Borg')]").extract_first() 
            if deposit: 
                item_loader.add_value("deposit",deposit)   

        utilities = response.xpath("//div[@id='description']//div[@class='panel1']//text()[contains(.,'Servicekosten') and not(contains(.,'€0,0') )]").extract_first() 
        if utilities: 
            utilities = utilities.split(",")[0]
            item_loader.add_value("utilities",utilities)  

        available_date = response.xpath("//li[div[contains(.,'Beschikbaar vanaf')]]/div[2]//text()").extract_first() 
        if not available_date:
            available_date = response.xpath("substring-after(//div[@id='description']//div[@class='panel1']//text()[contains(.,'Beschikbaar')],'Beschikbaar')").extract_first() 
        if available_date:       
            date_parsed = dateparser.parse(available_date.replace(":","").replace("circa","").strip(),date_formats=["%d-%m-%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
      
        square =response.xpath("//li[div[contains(.,'Woonoppervlakte')]]/div[2]//text()").extract_first()
        if square:
            square_meters =  square.split("m")[0].strip()
            item_loader.add_value("square_meters", square_meters) 

        furnished =response.xpath("//li[div[contains(.,'Huur specificatie')]]/div[2]//text()").extract_first()    
        if furnished:
            if "gemeubileerd" in furnished.lower():
                item_loader.add_value("furnished", True)
            
        parking =response.xpath("//li[div[contains(.,'Parkeerfaciliteiten')]]/div[2]//text()").extract_first()    
        if parking:
            item_loader.add_value("parking", True)    
        
        desc = " ".join(response.xpath("//div[@id='description']//div[@class='panel1']//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
              
        images = [response.urljoin(x) for x in response.xpath("//div[@class='detail-slider-wrap']/div/a/@href").extract()]
        if images:
            item_loader.add_value("images", images)
        script_map = response.xpath("//script[@type='text/javascript']/text()[contains(.,'property_lng')]").get()
        if script_map:
            item_loader.add_value("latitude", script_map.split('"property_lat":"')[1].split('"')[0].strip())
            item_loader.add_value("longitude", script_map.split('"property_lng":"')[1].split('"')[0].strip())
        item_loader.add_value("landlord_name", "GEWOON Makelaardij")
        item_loader.add_value("landlord_phone", "043 - 364 10 40")
        item_loader.add_value("landlord_email", "info@gewoonmakelaardij.nl")
        
        status = response.xpath("//span[@class='label-wrap']/span[contains(@class,'label-status-142')]//text()").get()
        if not status:
            yield item_loader.load_item()