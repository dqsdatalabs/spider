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
    name = 'deblooisvastgoed_nl'
    execution_type='testing'
    country = 'netherlands'
    locale = 'nl'
    def start_requests(self):
        start_urls = [
            {"url": "https://www.deblooisvastgoed.nl/huuraanbod/"}
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                        )
    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@id='residences']/div[contains(@class,'residence') and not(contains(.,'Verhuurd'))]"):
            follow_url = item.xpath(".//a[contains(.,'bekijken')]/@href").get()
            yield Request(follow_url, callback=self.populate_item)

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        property_type = "".join(response.xpath("//ul[@class='row']/li[span[.='Soort appartement']]/span[2]/text()").getall())
        if property_type:
            if get_p_type_string(property_type): item_loader.add_value("property_type", get_p_type_string(property_type))
            else: return

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Deblooisvastgoed_PySpider_netherlands")           
        item_loader.add_xpath("title", "//h1/text()")        
        item_loader.add_xpath("address", "//h1/text()")        
        item_loader.add_xpath("zipcode", "//li[span[.='Postcode']]/span[2]//text()")
        item_loader.add_xpath("city", "//li[span[.='Stad']]/span[2]//text()")
        item_loader.add_xpath("rent_string","//li[span[.='Prijs']]/span[2]//text()")                
         
        available_date = response.xpath("//li[span[contains(.,'Beschikbaar')]]/span[2]//text()").extract_first() 
        if available_date:
            date_parsed = dateparser.parse(available_date.strip())
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
      
        square =response.xpath("//li[span[.='Oppervlakte']]/span[2]//text()").extract_first()
        if square:
            square_meters =  square.split("m")[0].strip()
            item_loader.add_value("square_meters", square_meters) 
        
        img = response.xpath("//div[@class='row photos']/div[@class='imgContainer']/@style").extract() 
        if img:
            images=[]
            for x in img:
                image = x.split("background-image:url(")[1].split(");")[0]
                images.append(image)
            if images:
                item_loader.add_value("images",  list(set(images)))
        furnished =response.xpath("//li[span[.='Interieur']]/span[2]//text() ").extract_first()   
        if furnished:
            if "gemeubileerd" in furnished.lower() or "gestoffeerd" in furnished.lower():
                item_loader.add_value("furnished", True)
            else:
                item_loader.add_value("furnished", False)
        desc = " ".join(response.xpath("//div[h2[contains(.,'Beschrijving')]]/div//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())

        item_loader.add_value("landlord_name", "De Bloois Vastgoed B.V.")
        item_loader.add_value("landlord_phone", " 015 - 750 30 40")
        item_loader.add_value("landlord_email", "info@deblooisvastgoed.nl")

        item_loader.add_value("external_id", response.url.split('/')[-2].strip())

        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "unit" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "villa" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "home" in p_type_string.lower() or "woning" in p_type_string.lower()):
        return "house"
    elif p_type_string and ("kamer" in p_type_string.lower() ):
        return "room"
    else:
        return None