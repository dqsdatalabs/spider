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
    name = 'rentastone_nl'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'
    start_urls = ["https://rentastone.nl/huurwoningen/"]
    thousand_separator = ','
    scale_separator = '.'       
    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//ul[@id='search-results']/li"):
            status = item.xpath(".//span[contains(@class,'status-sticker')]/text()").get()
            if status and "onder optie" in status.lower() or "verhuurd" in status.lower():
                continue
            follow_url = response.urljoin(item.xpath(".//a[contains(@class,'btn-theme-primary')]/@href").get())
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 2 or seen:
            p_url = f"https://rentastone.nl/huurwoningen/page/{page}/"
            yield Request(
                p_url,
                callback=self.parse,
                meta={"page":page+1})
        
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)

        f_text = "".join(response.xpath("//div[contains(@class,'card-unit-description')]//text()").getall())
        if get_p_type_string(f_text):
            item_loader.add_value("property_type", get_p_type_string(f_text))
        else:
            return
        item_loader.add_value("external_source", "Rentastone_PySpider_netherlands")
        title = response.xpath("//div/h1/text()").extract_first()
        if title:
            item_loader.add_value("title",title.strip() )  
     
        address = ", ".join(response.xpath("//div[@class='container']//div/h1/text() | //div[@class='container']//div/span/text()").extract())
        if address:            
            item_loader.add_value("address", address.replace("  ","").strip()) 
            address = address.split(",")[-1].strip()  
            zipcode =address.split(" ")[0]
            city = address.replace(zipcode,"")   
            item_loader.add_value("zipcode",zipcode.strip() )    
            item_loader.add_value("city",city.strip() ) 

        room_count = response.xpath("//dl/dt[contains(.,'Slaapkamer')]/following-sibling::dd[1]/text()").extract_first()
        if room_count:
            item_loader.add_value("room_count", room_count)
       
        item_loader.add_xpath("bathroom_count", "//dl/dt[contains(.,'Badkamer')]/following-sibling::dd[1]/text()")
        rent =response.xpath("//span[@class='page-price']//text()").extract_first()
        if rent:     
            rent = rent.replace("€","").replace(".","")
            item_loader.add_value("rent",int(float(rent.replace(",","."))))  
        item_loader.add_value("currency", "EUR")    

        deposit =response.xpath("//dl/dt[contains(.,'Borg')]/following-sibling::dd[1]/text()").extract_first()
        if deposit:     
            deposit = deposit.replace("€","").replace(".","")
            item_loader.add_value("deposit", int(float(deposit.replace(",",".")))) 
      
        square = response.xpath("//dl/dt[contains(.,'Oppervlakte')]/following-sibling::dd[1]/text()").extract_first()
        if square:
            square_meters =  square.split("m")[0].strip()
            item_loader.add_value("square_meters",int(float(square_meters))) 

        furnished =response.xpath("//dl/dt[contains(.,'furnished')]/following-sibling::dd[1]//i/@class").extract_first()    
        if furnished:
            item_loader.add_value("furnished", True)
        balcony =response.xpath("//dl/dt[contains(.,'Balkon')]/following-sibling::dd[1]//i/@class").extract_first()    
        if balcony:
            item_loader.add_value("balcony", True)
        parking = response.xpath("//dl/dt[contains(.,'parkeerplaats')]/following-sibling::dd[1]//i/@class").extract_first()    
        if parking:
            item_loader.add_value("parking", True)
        desc = " ".join(response.xpath("//div[@id='collapsed-unit-description']/div//text()[not(contains(.,'Beschrijving'))]").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
        data_cord =response.xpath("//div/@data-cord").extract_first()    
        if data_cord:
            item_loader.add_value("latitude", data_cord.split(",")[0].strip())
            item_loader.add_value("longitude", data_cord.split(",")[1].strip())
    
        images = [response.urljoin(x) for x in response.xpath("//div[@class='carousel-inner']//a/@href").extract()]
        if images:
                item_loader.add_value("images", images)
        available_date = response.xpath("//dl/dt[contains(.,'Beschikbaar')]/following-sibling::dd[1]/text()").extract_first() 
        if available_date:
            date_parsed = dateparser.parse(available_date.strip(), date_formats=["%d %B %Y"], languages=['nl'])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
    
        item_loader.add_value("landlord_name", "Rent a Stone")
        item_loader.add_value("landlord_phone", "+31 (0) 85-4884787")
        item_loader.add_value("landlord_email", "info@rentastone.nl")
              
        yield item_loader.load_item()


def get_p_type_string(p_type_string):
    if p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "appartement" in p_type_string.lower() or "bovenwoning" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "cottage" in p_type_string.lower() or "terrace" in p_type_string.lower()):
        return "house"
    else:
        return None