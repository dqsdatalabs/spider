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
    name = 'anvastgoed_nl'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'
    external_source="Anvastgoed_PySpider_netherlands"
    start_urls = ["https://www.anvastgoed.nl/huurwoningen"] #LEVEL-1

    # 1. FOLLOWING
    def parse(self, response):
        token = response.xpath("//input[@name='_token']/@value").get()
        start_urls = [
            {
                "property_type" : "apartment",
                "type" : "Appartementen"
            },
            {
                "property_type" : "house",
                "type" : "Woonhuizen"
            },
        ]
        for item in start_urls:
            formdata = {
                "_token": token,
                "url": "huurwoningen",
                "inputLocation": "Geen voorkeur",
                "inputWijk": "Geen voorkeur",
                "inputType": item["type"],
                "inputBeds": "0",
                "inputPersonen": "1",
                "inputPriceFrom": "100",
                "inputPriceTo": "5000",
                "zoekblok": "1",
            }
            yield FormRequest(
                "https://www.anvastgoed.nl/huurwoningen",
                callback=self.jump,
                formdata=formdata,
                dont_filter=True,
                meta={
                    "property_type":item["property_type"]
                }

            )
    

    def jump(self, response):
        for item in response.xpath("//div[contains(@class,'title')]/h2/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={
                    "property_type":response.meta["property_type"]
                })
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)
        external_id = response.xpath("//input[@name='WoningID']//@value").get()
        if external_id:
            item_loader.add_value("external_id", external_id)

        rented =response.xpath("//div/h1[@class='page-header']//span[contains(.,'VERHUURD')]//text()").extract_first()
        if rented:     
            return     
        status =response.xpath("//tr[th[contains(.,'Type')]]/td//text()").extract_first()
        if status:     
            if "Bedrijfsruimte" in status:
                return    
        city = response.xpath("//tr[th[contains(.,'Plaats')]]/td//text()").extract_first()
        if city:
            item_loader.add_value("city",city.strip() ) 
        address = response.xpath("//tr[th[contains(.,'Buurt')]]/td//text()[normalize-space()]").extract_first()
        if address:
            if city:
                address = address.strip() +", "+city.strip()
            item_loader.add_value("address",address.replace("\r\n","").strip() ) 

        item_loader.add_xpath("title", "//div/h1[@class='page-header']/text()")
        item_loader.add_xpath("utilities","//tr[th[contains(.,'Servicekosten')]]/td//text()[not(contains(.,'€ 0'))]")                
        item_loader.add_xpath("rent_string","//tr[td[contains(.,'Totale huurprijs')]]//text()")                
         
        room_count =response.xpath("//tr[th[contains(.,'Slaapkamer')]]/td//text()[.!='0']").extract_first()
        if room_count:     
            item_loader.add_value("room_count", room_count)  
        else:
            room_count =response.xpath("//div[@class='property-detail']/h2//text()[contains(.,'studio')]").extract_first()
            if room_count:     
                item_loader.add_value("room_count","1")  
        available_date = response.xpath("//tr[th[contains(.,'Beschikbaar per')]]/td//text()").extract_first() 
        if available_date:
            date_parsed = dateparser.parse(available_date.strip(), languages=['nl'])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
      
        floor = response.xpath("//tr[th[contains(.,'Etage')]]/td//text()").extract_first() 
        if floor:   
            item_loader.add_value("floor",floor.replace("etage","").strip())      
       
        square =" ".join(response.xpath("//tr[th[contains(.,'Oppervlakte')]]/td//text()").extract())
        if square:
            square_meters =  square.split("m")[0].strip()
            item_loader.add_value("square_meters", int(float(square_meters))) 

        pets_allowed =response.xpath("//tr[th[contains(.,'Huisdieren')]]/td//text()[normalize-space()]").extract_first()    
        if pets_allowed:
            if "niet" in pets_allowed.lower():
                item_loader.add_value("pets_allowed", False)
            else:
                item_loader.add_value("pets_allowed", True)
        
        furnished =response.xpath("//tr[th[contains(.,'Oplevering')]]/td//text()").extract_first()    
        if furnished:
            if "kaal" in furnished.lower():
                item_loader.add_value("furnished", False)
            else:
                item_loader.add_value("furnished", True)
  
        script_map =response.xpath("//script[@type='text/javascript']//text()[contains(.,'position: new google.maps.LatLng(')]").extract_first()    
        if script_map:
            latlng = script_map.split("position: new google.maps.LatLng(")[1].split("),")[0]
            if "0.00000" not in latlng:
                item_loader.add_value("latitude", latlng.split(",")[0].strip())
                item_loader.add_value("longitude", latlng.split(",")[1].strip())

        desc = " ".join(response.xpath("//div[@class='property-detail']/p//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
              
        images = [x for x in response.xpath("//div[@class='galleria']//img/@src").extract()]
        if images:
                item_loader.add_value("images", images)
        name=response.xpath("//div[@class='agent']//div[@class='name']//text()").get()
        if name:
            item_loader.add_value("landlord_name", name.split("via")[0])
        item_loader.add_value("landlord_phone", "070 358 63 91")
        email=response.xpath("(//div[@class='agent']//div[@class='email']//text())[1]").get()
        if email:
            item_loader.add_value("landlord_email", email.split(" ")[0])
        yield item_loader.load_item()
