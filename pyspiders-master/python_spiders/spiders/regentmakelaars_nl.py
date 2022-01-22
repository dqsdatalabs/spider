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
    name = 'regentmakelaars_nl'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'
    start_urls = ["https://www.regentmakelaars.nl/aanbod/woningaanbod/huur/"]

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//li[contains(@class,'al2woning')]"):
            status = item.xpath(".//span[contains(@class,'objectstatusbanner')]/text()").get()
            if status and "verhuurd" in status.lower():
                continue
            follow_url = response.urljoin(item.xpath("./a/@href").get())
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 2 or seen:
            p_url = f"https://www.regentmakelaars.nl/aanbod/woningaanbod/huur/pagina-{page}/"
            yield Request(
                p_url,
                callback=self.parse,
                meta={"page":page+1})
        
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)

        f_text = "".join(response.xpath("//span[contains(.,'Soort object')]/following-sibling::span[1]/text()").getall())
        if get_p_type_string(f_text):
            item_loader.add_value("property_type", get_p_type_string(f_text))
        else:
            return
        item_loader.add_value("external_source", "Regentmakelaars_PySpider_netherlands")      
        item_loader.add_value("external_id", response.url.split("-")[1].split("-")[0].strip())      

        item_loader.add_xpath("title", "//h1[@class='street-address']//text()")        
         
        address =", ".join(response.xpath("//div[contains(@class,'addressInfo')]//text()[normalize-space()]").extract()) 
        if address:
            item_loader.add_value("address",address.strip() ) 

        item_loader.add_xpath("zipcode", "//div[@class='ogDetails']//span[@class='postal-code']//text()")
        item_loader.add_xpath("city", "//div[@class='ogDetails']//span[@class='locality']//text()")
        item_loader.add_xpath("deposit","//span[span[contains(.,'Waarborgsom')]]/span[2]/text()")                
        item_loader.add_xpath("utilities","substring-before(//span[span[contains(.,'Servicekosten')]]/span[2]/text(),',')")                
        item_loader.add_xpath("rent_string","//span[span[contains(.,'Huurprijs')]]/span[2]/text()")                
        
        available_date = response.xpath("//span[span[contains(.,'Aanvaarding')]]/span[2]/text()").extract_first() 
        if available_date:  
            date_parsed = dateparser.parse(available_date.replace("Per","").replace("Direct","now").strip(),date_formats=["%d-%m-%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        room_count = response.xpath("//span[span[contains(.,'slaapkamer')]]/span[2]/text()").extract_first() 
        if room_count:   
            item_loader.add_value("room_count",room_count.strip())  
        else:
            room_count = response.xpath("//span[span[contains(.,'Aantal kamer')]]/span[2]/text()").extract_first() 
            if room_count:   
                item_loader.add_value("room_count",room_count.strip())  
      
        floor = response.xpath("//span[span[contains(.,'woonlagen')]]/span[2]/text()").extract_first() 
        if floor:   
            item_loader.add_value("floor",floor.split("woonla")[0].strip())      
       
        square =response.xpath("//span[span[contains(.,'Woonoppervlakte')]]/span[2]/text()").extract_first()
        if square:
            square_meters =  square.split("m")[0].strip()
            item_loader.add_value("square_meters", square_meters) 

        energy = response.xpath("//span[span[contains(.,'Energieklasse')]]/span[2]/text()").extract_first()
        if energy:
            item_loader.add_value("energy_label", energy.strip())
            
        terrace =response.xpath("//span[span[contains(.,'Dakterras')]]/span[2]/text()").extract_first()    
        if terrace:
            if "ja" in terrace.lower():
                item_loader.add_value("terrace", True)
            else:
                item_loader.add_value("terrace", False)
        balcony =response.xpath("//span[span[contains(.,'Balkon')]]/span[2]/text()").extract_first()    
        if balcony:
            if "ja" in balcony.lower():
                item_loader.add_value("balcony", True)
            else:
                item_loader.add_value("balcony", False)
        parking =response.xpath("//span[span[contains(.,'Garage') or contains(.,'Parkeer')]]/span[2]/text()").extract_first()    
        if parking:
            if "geen" in parking.lower():
                item_loader.add_value("parking", False)
            else:
                item_loader.add_value("parking", True)
        elevator =response.xpath("//span[span[contains(.,'Voorzieningen')]]/span[2]/text()[contains(.,'Lift')]").extract_first()    
        if elevator:
            item_loader.add_value("elevator", True)

        furnished =response.xpath("//span[span[contains(.,'Bijzonderheden')]]/span[2]/text()").extract_first()    
        if furnished:
            if "gestoffeerd" in furnished.lower():
                item_loader.add_value("furnished", False)
            elif "gemeubeld"  in furnished.lower() or "gemeubileerd" in furnished.lower():
                item_loader.add_value("furnished", True)
            
        desc = " ".join(response.xpath("//div[@id='Omschrijving']/h3/following-sibling::text()[normalize-space()]").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
        else:
            desc = " ".join(response.xpath("//div[@id='Omschrijving']/h3/following-sibling::*/text()[normalize-space()]").extract())
            if desc:
                item_loader.add_value("description", desc.strip())
              
        images = [x for x in response.xpath("//div[@class='ogFotos']/div[@class='detailFotos']//a/img/@src").extract()]
        if images:
                item_loader.add_value("images", images)

        script_map = response.xpath("//script[@type='text/javascript']/text()[contains(.,'myMarker = new google.maps.Marker({') and contains(.,'position: new google.maps.LatLng(')]").get()
        if script_map:
            latlng = script_map.split("myMarker = new google.maps.Marker({")[1].split("position: new google.maps.LatLng(")[1].split("),")[0]
            item_loader.add_value("latitude", latlng.split(",")[0].strip())
            item_loader.add_value("longitude", latlng.split(",")[1].strip())

        item_loader.add_value("landlord_name", "Regent Makelaars")
        item_loader.add_value("landlord_phone", "0513-640000")
        item_loader.add_value("landlord_email", "info@regentmakelaars.nl")    
        
        yield item_loader.load_item()


def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and "kamer" in p_type_string.lower():
        return "room"
    elif p_type_string and ("apartment" in p_type_string.lower() or "appartement" in p_type_string.lower() or "bovenwoning" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "woning" in p_type_string.lower() or "huis" in p_type_string.lower()):
        return "house"
    else:
        return None