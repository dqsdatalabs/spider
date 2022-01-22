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

class MySpider(Spider):
    name = 'agir_paca_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    custom_settings = {"HTTPCACHE_ENABLED": False}

    def start_requests(self):
        yield Request("https://www.agir-paca.fr/a-louer/1", callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 2)
        seen = False

        for item in response.xpath("//div[@class='bienTitle']"):
            seen = True
            follow_url = response.urljoin(item.xpath("./h1/a/@href").get())
            property_type = item.xpath("./h2/text()").get()
            if property_type:
                if get_p_type_string(property_type): yield Request(follow_url, callback=self.populate_item, meta={'property_type': get_p_type_string(property_type)})
        
        if page == 2 or seen:
            yield Request(f"https://www.agir-paca.fr/a-louer/{page}", callback=self.parse, meta={"page": page+1})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Agir_Paca_PySpider_france")
        if response.url == "https://www.agir-paca.fr":
            return
        item_loader.add_xpath("title", "//div[@class='themTitle']/h1//text()")    
        item_loader.add_value("external_id",response.xpath("substring-after(//ul[@class='list-inline']/li[contains(.,'Ref')]//text(),'Ref')").extract_first().strip())                
        rent =" ".join(response.xpath("//ul[@class='list-inline']/li[contains(.,'€')]//text()").extract())
        if rent:     
           rent = rent.strip().replace(" ","").split("€")[0]
           item_loader.add_value("rent", int(float(rent.replace(",","."))))     
        item_loader.add_value("currency", "EUR")     
        address = response.xpath("//p[span[contains(.,'Ville')]]/span[2]/text()").extract_first() 
        if address:   
            item_loader.add_value("address",address.strip())  
        city = response.xpath("//p[span[contains(.,'Ville')]]/span[2]/text()").extract_first() 
        if city:   
            item_loader.add_value("city",city.strip())    
        zipcode = response.xpath("//p[span[contains(.,'Code postal')]]/span[2]/text()").extract_first() 
        if city:   
            item_loader.add_value("zipcode",zipcode.strip())       
        floor = response.xpath("//p[span[contains(.,'Etage')]]/span[2]/text()").extract_first() 
        if floor:   
            item_loader.add_value("floor",floor.strip())     
             
        room_count = response.xpath("//p[span[contains(.,'chambre')]]/span[2]/text()").extract_first() 
        if not room_count:
            room_count = response.xpath("//p[span[contains(.,'pièces')]]/span[2]/text()").extract_first() 
        if room_count:   
            item_loader.add_value("room_count",room_count.split("(")[0].strip())
     
        square =response.xpath("//p[span[contains(.,'Surface habitable')]]/span[2]/text()").extract_first()
        if square:
            square_meters =  square.split("m")[0].strip()
            item_loader.add_value("square_meters", int(float(square_meters.replace(",",".")))) 

        furnished = response.xpath("//p[span[contains(.,'Meublé')]]/span[2]/text()").extract_first()       
        if furnished:
            if "NON" in furnished.upper():
                item_loader.add_value("furnished", False)
            else:
                item_loader.add_value("furnished", True)
        elevator = response.xpath("//p[span[contains(.,'Ascenseur')]]/span[2]/text()").extract_first()       
        if elevator:
            if "NON" in elevator.upper():
                item_loader.add_value("elevator", False)
            else:
                item_loader.add_value("elevator", True)
        parking = response.xpath("//div[@class='themTitle']/h1//text()[contains(.,'PARKING')]").extract_first()       
        if parking:         
            item_loader.add_value("parking", True)
        desc = " ".join(response.xpath("//div[@class='row']/div[div[contains(.,'Description')]]/p//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
              
        images = [response.urljoin(x) for x in response.xpath("//ul[contains(@class,'imageGallery')]//@src").extract()]
        if images:
                item_loader.add_value("images", images)

        utilities = response.xpath("//span[contains(.,'Charges')]//following-sibling::span//text()").get()
        if utilities:
            utilities = utilities.replace("€","").strip()
            item_loader.add_value("utilities", utilities)

        deposit = response.xpath("//span[contains(.,'Dépôt de garantie')]//following-sibling::span//text()").get()
        if deposit and "€" in deposit:
            deposit = deposit.replace("€","").strip().replace(" ","").replace(",",".")
            item_loader.add_value("deposit", deposit)

        item_loader.add_value("landlord_name", "AGIR PACA")
        item_loader.add_value("landlord_phone", "04 93 97 60 91")
        item_loader.add_value("landlord_email", "contact@agir-paca.fr")

        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("duplex" in p_type_string.lower() or "appartement" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "maison" in p_type_string.lower() or "villa" in p_type_string.lower()):
        return "house"
    else:
        return None