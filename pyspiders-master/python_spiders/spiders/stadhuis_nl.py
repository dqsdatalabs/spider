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
from datetime import datetime

class MySpider(Spider):
    name = 'stadhuis_nl'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'
    external_source="Stadhuis_PySpider_netherlands"
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://stadhuis.nl/woning-huren/?woning-type=Appartement&slaapkamers=Geen+voorkeur&plaats=Geen+voorkeur&minimum-prijs=Geen+voorkeur&maximum-prijs=Geen+voorkeur",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://stadhuis.nl/woning-huren/?woning-type=Studio&slaapkamers=Geen+voorkeur&plaats=Geen+voorkeur&minimum-prijs=Geen+voorkeur&maximum-prijs=Geen+voorkeur",
                ],
                "property_type" : "studio",
            },
            {
                "url" : [
                    "https://stadhuis.nl/woning-huren/?woning-type=Kamer&slaapkamers=Geen+voorkeur&plaats=Geen+voorkeur&minimum-prijs=Geen+voorkeur&maximum-prijs=Geen+voorkeur",
                ],
                "property_type" : "room",
            },
            {
                "url" : [
                    "https://stadhuis.nl/en/woning-huren/?woning-type=Family+house&slaapkamers=No+preference&plaats=No+preference&minimum-prijs=No+preference&maximum-prijs=No+preference#aanbod-huurwoningen",
                ],
                "property_type": "house",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//a[@class='img-wrapper']"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
     
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        status = "".join(response.xpath("//div[@class='detail-page-property-status']//text()").getall())
        if status and "verhuurd" in status.lower():
            return
        dontallow=response.xpath("//span[.='Rented']/text()").get()
        if dontallow:
            return 
        external_id=response.xpath("//link[@rel='shortlink']/@href").get()
        if external_id:
            item_loader.add_value("external_id",external_id.split("p=")[-1])
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)        
        item_loader.add_xpath("title", "//div/h1/text()")
     
        item_loader.add_xpath("zipcode", "//div[div/p[contains(.,'Postcode')]]/div[2]/p/text()")
        city = response.xpath("//div[div/p[contains(.,'Stad')]]/div[2]/p/text()").extract_first() 
        if city: 
            item_loader.add_value("city",city) 

        address =", ".join(response.xpath("//div[div/p[contains(.,'Straat')]]/div[2]/p/text() | //div[div/p[contains(.,'Wijk')]]/div[2]/p/text()").extract())
        if address:  
            if city:
                address = address.strip()+", "+city.strip()
            item_loader.add_value("address",address.strip() ) 
               
        bathroom_count = response.xpath("//div[div/p[contains(.,'badkamer')]]/div[2]/p/text()").extract_first() 
        if bathroom_count: 
            item_loader.add_value("bathroom_count",bathroom_count) 
        valid_room_count = response.xpath("//i[@class='fas fa-bed']/../following-sibling::span/text()").get()
        if valid_room_count != "0": 
            room_count = response.xpath("//div[div/p[contains(.,'slaapkamer')]]/div[2]/p/text()[.!='0']").extract_first() 
            if room_count: 
                item_loader.add_value("room_count",room_count) 
            elif "studio" in response.meta.get('property_type') or "room" in response.meta.get('property_type'):
                item_loader.add_value("room_count","1") 
    
        rent =" ".join(response.xpath("//div[contains(@class,'is_widget_properties_price')]//text()[normalize-space()]").extract() )
        if rent: 
            item_loader.add_value("rent_string",rent)   
        location=response.xpath("//a[contains(@href,'Breda/@')]/@href").get()
        if location:
            item_loader.add_value("latitude",location.split("Breda/@")[-1].split(",")[0])
            item_loader.add_value("longitude",location.split("Breda/@")[-1].split(",")[1])
       
        available_date = response.xpath("//div[div/p[contains(.,'Beschikbaar')]]/div[2]/p/text()").extract_first() 
        if available_date:     
            if "direct" in available_date.lower():
                item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
            else:
                date_parsed = dateparser.parse(available_date.replace("Vanaf","").strip(),date_formats=["%d-%m-%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
      
        square =response.xpath("//div[div/p[contains(.,'Oppervlakte')]]/div[2]/p/text()").extract_first()
        if square:
            square_meters =  square.split("m")[0].strip()
            item_loader.add_value("square_meters", square_meters) 

        furnished =response.xpath("//div[div/p[contains(.,'Interieur')]]/div[2]/p/text()").extract_first()    
        if furnished:
            if "ongemeubileer" in furnished.lower():
                item_loader.add_value("furnished", False)
            elif "gemeubileerd" in furnished.lower():
                item_loader.add_value("furnished", True)
            
        parking =response.xpath("//div[div/p[contains(.,'Parkeren')]]/div[2]/p/text()").extract_first()    
        if parking:
            if parking.lower().strip() == "geen" or parking.lower().strip() == "nee":
                item_loader.add_value("parking", False)
            else:
                item_loader.add_value("parking", True) 
        elevator =response.xpath("//div[div/p[contains(.,'Lift')]]/div[2]/p/text()").extract_first()    
        if elevator:
            if "nee" in elevator.lower():
                item_loader.add_value("elevator", False)
            else:
                item_loader.add_value("elevator", True)    
        balcony =response.xpath("//div[div/p[contains(.,'Balkon')]]/div[2]/p/text()").extract_first()    
        if balcony:
            if "geen" in balcony.lower():
                item_loader.add_value("balcony", False)
            else:
                item_loader.add_value("balcony", True)  
        terrace =response.xpath("//div[div/p[contains(.,'Dakterras')]]/div[2]/p/text()").extract_first()    
        if terrace:
            if "geen" in terrace.lower():
                item_loader.add_value("terrace", False)
            else:
                item_loader.add_value("terrace", True)  
        desc = " ".join(response.xpath("//div[@id='description-wrapper']//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
              
        images = [response.urljoin(x) for x in response.xpath("//div[@id='gallery']/img/@src").extract()]
        if images:
            item_loader.add_value("images", images)
        lat_lng = response.xpath("//a[contains(@href,'maps')]/@href").re_first(r"@(\d+.\d+,.*\d+.\d+),")
        if lat_lng:
            item_loader.add_value("latitude", lat_lng.split(",")[0])
            item_loader.add_value("longitude", lat_lng.split(",")[1])
        item_loader.add_value("landlord_name", "StadHuis")
        item_loader.add_value("landlord_phone", "+31 (0)76 - 572 90 74")
        item_loader.add_value("landlord_email", "breda@stadhuis.nl")  
              
        yield item_loader.load_item()