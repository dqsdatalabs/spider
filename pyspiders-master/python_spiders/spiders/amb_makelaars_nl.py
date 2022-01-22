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
    name = 'amb_makelaars_nl'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'
    start_urls = ["https://www.amb-makelaars.nl/aanbod/woningaanbod/huur/"] #LEVEL-1

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='vakfoto']/a[@class='aanbodEntryLink']"):
            status = item.xpath("./div/div/@class").get()
            if status and "verhuurd" in status.lower():
                continue 
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item)   

        next_page = response.xpath("//span[contains(@class,'next-page')]/a/@href").get()
        if next_page:
            yield Request(
                response.urljoin(next_page),
                callback=self.parse,
            )     
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        p_type_info = response.xpath("//span[.='Soort object']/following-sibling::*/text()").get()
        if get_p_type_string(p_type_info):
            item_loader.add_value("property_type", get_p_type_string(p_type_info))
        else:
            return
        item_loader.add_value("external_source", "Amb_Makelaars_PySpider_netherlands")

        item_loader.add_xpath("title", "//h1[@class='street-address']//text()")        
         
        address =" ".join(response.xpath("//div[contains(@class,'addressInfo')]//text()[normalize-space()]").extract()) 
        if address:
            item_loader.add_value("address",address.strip() ) 

        item_loader.add_xpath("zipcode", "//div[@class='ogDetails']//span[@class='postal-code']//text()")
        item_loader.add_xpath("city", "//div[@class='ogDetails']//span[@class='locality']//text()")
        item_loader.add_xpath("room_count", "//span[span[contains(.,'Aantal kamers')]]/span[2]/text()")
        item_loader.add_xpath("deposit","//span[span[contains(.,'Waarborgsom')]]/span[2]/text()")                
        item_loader.add_xpath("rent_string","//span[span[contains(.,'Huurprijs')]]/span[2]/text()")                
         
        utilities =response.xpath("//span[span[contains(.,'Servicekosten')]]/span[2]/text()").extract_first()
        if utilities:     
            utilities = utilities.split("€")[1].strip()
            item_loader.add_value("utilities", int(float(utilities.replace(",","."))))  
        available_date = response.xpath("//span[span[contains(.,'Aanvaarding')]]/span[2]/text()").extract_first() 
        if available_date:
            date_parsed = dateparser.parse(available_date.replace("Per","").strip(), languages=['nl'])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
      
        floor = response.xpath("//span[span[contains(.,'woonlagen')]]/span[2]/text()").extract_first() 
        if floor:   
            item_loader.add_value("floor",floor.replace("woonlaag","").strip())      
       
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

        desc = " ".join(response.xpath("//div[@class='middle']/div/h3/following-sibling::text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
              
        images = [x for x in response.xpath("//div[@class='ogFotos']/div[@class='detailFotos']//a/img/@src").extract()]
        if images:
                item_loader.add_value("images", images)
        latlng=response.xpath("//script[contains(.,'longitude')]/text()").get()
        if latlng:
            longitude=latlng.split("longitude")[-1].split("}")[0].split(":")[-1]
            item_loader.add_value("longitude",longitude)
            latitude=latlng.split("latitude")[-1].split(",")[0].split(":")[-1]
            item_loader.add_value("latitude",latitude)

        item_loader.add_value("landlord_name", "AMB MAKELAARS")
        item_loader.add_xpath("landlord_phone", "//a[@class='verkopendevestiging-telnr']//text()")
        item_loader.add_value("landlord_email", "denbosch@amb-makelaars.nl")
      
        yield item_loader.load_item()


def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower() or "appartement" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "woonhuis" in p_type_string.lower() or "cottage" in p_type_string.lower() or "terrace" in p_type_string.lower()):
        return "house"
    else:
        return None