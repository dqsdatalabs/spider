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
from datetime import datetime

class MySpider(Spider):
    name = 'dehaagschemakelaar_nl'
    execution_type = 'testing' 
    country = 'netherlands'
    locale = 'nl'
    start_urls = ["https://www.dehaagschemakelaar.nl/woningaanbod/huur"]
    external_source = "Dehaagschemakelaar_PySpider_netherlands"
    # 1. FOLLOWING
    def parse(self, response):
  
        for item in response.xpath("//a[@class='object_data_container']"):
            status = item.xpath(".//h2/text()").get()
            if status and "verhuurd" in status.lower():
                continue
            follow_url = response.urljoin(item.xpath("./@href").get())          
            yield Request(follow_url, callback=self.populate_item)

        pagination = response.xpath("//li/a[@class='sys_paging next-page']/@href").get()
        if pagination:
            p_url = response.urljoin(pagination)
            yield Request(
                p_url,
                callback=self.parse)
        
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
       
        item_loader.add_value("external_link", response.url)

        f_text = "".join(response.xpath("//tr[td[.='Type object']]/td[2]/text()").getall())
        if get_p_type_string(f_text):
            item_loader.add_value("property_type", get_p_type_string(f_text))
        else: 
            return
        item_loader.add_value("external_source", self.external_source)
        
        item_loader.add_xpath("external_id","//tr[td[.='Referentienummer']]/td[2]/text()")
        # available_until = response.xpath("//div[contains(@class,'more-info-pane')]//li[span[.='Beschikbaar tot']]/span[2]/text()").extract_first() 
        # if available_until:
        #     date_now = datetime.now()
        #     date_old = datetime.strptime(available_until, '%d-%m-%Y')
        #     if date_now > date_old:
        #         return
                
        # item_loader.add_xpath("zipcode", "//div[contains(@class,'more-info-pane')]//li[span[.='Postcode']]/span[2]/text()")
        title = response.xpath("//h1/text()").extract_first() 
        if title:
            item_loader.add_value("title",title.strip() ) 
            address = title.split(":")[-1].strip()
            zipcode = " ".join(address.split(",")[-1].strip().split(" ")[:2])
            city = " ".join(address.split(",")[-1].strip().split(" ")[2:])
            item_loader.add_value("address",address ) 
            item_loader.add_value("zipcode",zipcode) 
            item_loader.add_value("city",city) 

        room_count = response.xpath("//tr[td[.='Aantal kamers']]/td[2]/text()").extract_first() 
        if room_count: 
            item_loader.add_value("room_count",room_count.split("slaapka")[0].strip().split(" ")[-1]) 
        bathroom_count = response.xpath("//tr[td[.='Aantal badkamers']]/td[2]/text()").extract_first() 
        if bathroom_count: 
            item_loader.add_value("bathroom_count",bathroom_count.split("(")[0]) 
        deposit = response.xpath("//tr[td[.='Borg']]/td[2]/text()").extract_first() 
        if deposit: 
            item_loader.add_value("deposit",deposit) 

        rent = response.xpath("//tr[td[.='Huurprijs']]/td[2]/text()").extract_first() 
        if rent: 
            item_loader.add_value("rent_string",rent.split("•")[-1])      
       
        available_date = response.xpath("//tr[td[.='Beschikbaar vanaf']]/td[2]/text()").extract_first() 
        if available_date:
            available_date = " ".join(available_date.strip().split(" ")[-1])
            date_parsed = dateparser.parse(available_date.strip(),date_formats=["%d-%m-%Y"])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))
      
        square =response.xpath("//tr[td[.='Gebruiksoppervlakte wonen']]/td[2]/text()").extract_first()
        if square:
            square_meters =  square.split("m")[0].strip()
            item_loader.add_value("square_meters", square_meters) 

        furnished ="".join(response.xpath("//tr[td[.='Inrichting']]/td[2]/text()").extract())
        if furnished:
            if "gemeubileerd" in furnished.lower():
                item_loader.add_value("furnished", True)                 
            else:
                item_loader.add_value("furnished", False)    

        balcony =response.xpath("//tr[td[.='Heeft een balkon']]/td[2]/text()").extract_first()    
        if balcony:
            if "nee" in balcony.lower():
                item_loader.add_value("balcony", False)
            else:
                item_loader.add_value("balcony", True)   
        # elevator =response.xpath("//div[contains(@class,'more-info-pane')]//li[span[.='Lift']]/span[2]/text()").extract_first()    
        # if elevator:
        #     if "ja" in elevator.lower():
        #        item_loader.add_value("elevator", True)  

        # parking = " ".join(response.xpath("//div[contains(@class,'more-info-pane')]//li[span[.='Parkeren']]/span[2]/text()").extract())    
        # if parking:
        #     if parking.lower().strip() != "nee":
        #         item_loader.add_value("parking", True)
                             
        desc = " ".join(response.xpath("//div[@class='description']//text()[.!='Beschrijving'][normalize-space()]").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
              
        images = [x for x in response.xpath("//div[@class='gallery']/a/@href").extract()]
        if images:
            item_loader.add_value("images", images)
        item_loader.add_xpath("energy_label", "//tr[td[.='Energielabel']]/td[2]/text()")
        item_loader.add_xpath("floor", "//tr[td[.='Aantal bouwlagen']]/td[2]/text()")
    
        item_loader.add_value("landlord_name", "De Haagsche Makelaar")
        item_loader.add_value("landlord_phone", "070-3695428")
        item_loader.add_value("landlord_email", "info@dehaagschemakelaar.nl")   
        
        yield item_loader.load_item()


def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "appartement" in p_type_string.lower() or "bovenwoning" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "cottage" in p_type_string.lower() or "terrace" in p_type_string.lower()):
        return "house"
    else:
        return None