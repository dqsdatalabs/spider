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
    name = 'regio56huisvesting_nl'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'
    def start_requests(self):
        start_urls = [
            {"url": "https://www.regio56makelaars.nl/woningaanbod/huur/type-appartement", "property_type": "apartment"},
	        {"url": "https://www.regio56makelaars.nl/woningaanbod/huur/type-woonhuis", "property_type": "house"},
            
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')
                        })

    # 1. FOLLOWING
    def parse(self, response):
        property_type = response.meta.get("property_type")

        for item in response.xpath("//div[contains(@class,'object_list')]//article//a[contains(@title,'Te huur')]/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":property_type})
        
        pagination = response.xpath("//ul[contains(@class,'pagination')]//a[contains(@class,'next-page')]/@href").get()
        if pagination:
            yield Request(response.urljoin(pagination), callback=self.parse, meta={"property_type":property_type})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_xpath("title", "//h2[@class='object_title']/text()")
        
        item_loader.add_value("external_source", "Regio56huisvesting_PySpider_netherlands")
         
        address =response.xpath("//tr[td[contains(.,'Adres')]]/td[2]/text()").extract_first()
        if not address: 
            address =response.xpath("substring-after(//h2[@class='object_title']/text(),':')").extract_first()
        if address:
            item_loader.add_value("address",address.strip() ) 
            address= address.split(",")[-1].strip()   
            city = address.split(" ")[-1]
            zipcode = address.replace(city,"")   
            item_loader.add_value("zipcode",zipcode.strip() )    
            item_loader.add_value("city",city.strip() )    
                
        item_loader.add_xpath("external_id","//tr[td[contains(.,'Referentienummer')]]/td[2]/text()")                
        item_loader.add_xpath("deposit","//tr[td[contains(.,'Borg')]]/td[2]/text()")                
        item_loader.add_xpath("utilities","//tr[td[contains(.,'Servicekosten')]]/td[2]/text()")                
        rent =response.xpath("//div[@class='object_price']/text()").extract_first()
        if rent:     
           item_loader.add_value("rent_string", rent)   
         
        available_date = response.xpath("//tr[td[contains(.,'Aanvaarding')]]/td[2]/text()").extract_first() 
        if available_date:
            if len(available_date.strip().split(" "))>3:
                date_parsed = dateparser.parse(" ".join(available_date.strip().split(" ")[2:]), languages=['nl'])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
      
        floor = response.xpath("//tr[td[contains(.,'Woonlaag')]]/td[2]/text()").extract_first() 
        if floor:   
            item_loader.add_value("floor",floor.replace("e woonlaag","").strip())      
        room_count = response.xpath("//tr[td[contains(.,'kamers')]]/td[2]/text()").extract_first() 
        if room_count:   
            if "slaapkamers" in room_count:
                item_loader.add_value("room_count",room_count.split("slaapkamers")[0].strip().split(" ")[-1])
            else:
                item_loader.add_value("room_count",room_count.split("(")[0].strip())
        bathroom_count = response.xpath("//tr[td[contains(.,'badkamer')]]/td[2]/text()").extract_first() 
        if bathroom_count:   
            if "(" in bathroom_count:
                bathroom_count = bathroom_count.split("(")[0]
            item_loader.add_value("bathroom_count",bathroom_count.strip())
        square =response.xpath("//tr[td[contains(.,'Gebruiksoppervlakte')]]/td[2]/text()").extract_first()
        if square:
            square_meters =  square.split("m")[0].strip()
            item_loader.add_value("square_meters", int(float(square_meters.replace(",",".")))) 

        furnished = response.xpath("//tr[td]/td[2]/text()[contains(.,'Gemeubileerd')]").extract_first()       
        if furnished:
            item_loader.add_value("furnished", True)
        parking = response.xpath("//tr[td[contains(.,'parkeerplaatsen')]]/td[2]/text()").extract_first()       
        if parking:
            item_loader.add_value("parking", True)
        energy = response.xpath("//tr[td[contains(.,'Energielabel')]]/td[2]/text()").extract_first()
        if energy:
            item_loader.add_value("energy_label", energy.strip())
    
        elevator =response.xpath("//tr[td[contains(.,'lift')]]/td[2]/text()").extract_first()    
        if elevator:
            if "ja" in elevator.lower():
                item_loader.add_value("elevator", True)
            else:
                item_loader.add_value("elevator", False)

        desc = " ".join(response.xpath("//div/div[contains(@class,'description')]//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
              
        images = [x for x in response.xpath("//div[@id='object-photos']/a/img/@src").extract()]
        if images:
                item_loader.add_value("images", images)

        item_loader.add_value("landlord_name", "Regio56 Brokers & Housing")
        item_loader.add_value("landlord_phone", "040-2351234")
        item_loader.add_value("landlord_email", "info@regio56makelaars.nl")

        script_map = response.xpath("//script[@type='text/javascript']/text()[contains(.,'center: [')]").get()
        if script_map:
            item_loader.add_value("latitude", script_map.split("center: [")[1].split(",")[1].split("]")[0].split(";")[0].strip())
            item_loader.add_value("longitude", script_map.split("center: [")[1].split(",")[0].strip())
        yield item_loader.load_item()