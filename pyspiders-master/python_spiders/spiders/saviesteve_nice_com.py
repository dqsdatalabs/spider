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
    name = 'saviesteve_nice_com'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.saviesteve-nice.com/immobilier/location/appartement/partout/?lsi_s_extends=0",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.saviesteve-nice.com/immobilier/location/maison/partout/?lsi_s_extends=0",
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//a[contains(@class,'link-view-details')]/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta['property_type']})
        
        next_page = response.xpath("//a[@rel='next']/@href").get()
        if next_page:
            yield Request(
                response.urljoin(next_page),
                callback=self.parse,
                meta={'property_type': response.meta['property_type']}
            )
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Saviesteve_Nice_PySpider_france")
        title =response.xpath("//div[@id='adtitlelsiwidget-3']/h2[@class='widget-title']/span/text()").extract_first()
        if title:
            item_loader.add_value("title",re.sub("\s{2,}", " ", title))

        city =response.xpath("//div[@id='adtextlsiwidget-3']/h2[@class='widget-title']/span/text()").extract_first()
        if city:
            item_loader.add_value("city",city.split("(")[0].strip() ) 
            item_loader.add_value("zipcode",city.split("(")[1].split(")")[0].strip() ) 

        address =response.xpath("//li[strong[contains(.,'Adresse')]]/span[@class='value']//text()").extract_first()
        if address:
            if city:
                address = address+", "+city.split("(")[0].strip()
            item_loader.add_value("address",address.strip() )    
        elif city:  
            item_loader.add_value("address",re.sub("\s{2,}", " ", city))    
        item_loader.add_xpath("external_id","substring-after(//p[@class='mandate']//text(),': ')")                             
        item_loader.add_xpath("utilities","//li[strong[contains(.,'Charges')]]/span[@class='value']//text()")      
        deposit =response.xpath("//li[strong[contains(.,'de garantie')]]/span[@class='value']//text()").extract_first()
        if deposit:     
           item_loader.add_value("deposit", deposit.replace(" ",""))     
        rent =response.xpath("//div[@id='adtitlelsiwidget-3']/h2[@class='widget-title']/span/text()").extract_first()
        if rent:     
           item_loader.add_value("rent_string", rent.split(" -")[-1].replace(" ","").replace("\u00a0",""))   

        floor = response.xpath("//li[strong[contains(.,'Étage')]]/span[@class='value']//text()").extract_first() 
        if floor:   
            item_loader.add_value("floor",floor.replace("eme","").strip())     
        room_count = response.xpath("//li[strong[contains(.,'chambre')]]/span[@class='value']//text()").extract_first() 
        if room_count:   
            item_loader.add_value("room_count",room_count.strip())
        else:
            room_count = response.xpath("//li[strong[contains(.,'pièce')]]/span[@class='value']//text()").extract_first() 
            if room_count:   
                item_loader.add_value("room_count",room_count.strip())
            
        bathroom_count = response.xpath("//li[strong[contains(.,'de bain')]]/span[@class='value']//text()").extract_first() 
        if bathroom_count:   
            item_loader.add_value("bathroom_count",bathroom_count.strip())

        square =response.xpath("//li[strong[contains(.,'Surface habitable')]]/span[@class='value']//text()").extract_first()
        if square:
            square_meters =  square.split("m")[0].strip()
            item_loader.add_value("square_meters", square_meters) 

        energy = response.xpath("//div/img[contains(@src,'/en') and contains(@src,'dpe')]/@src").extract_first()
        if energy:
            energy = energy.split("/en/")[1].split("?")[0].strip()
            if energy.isdigit():
                item_loader.add_value("energy_label", energy_label_calculate(energy))
    
        desc = " ".join(response.xpath("//p[@class='description']//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
      
        elevator = response.xpath("//li[strong[contains(.,'Ascenseur')]]//text()[normalize-space()]").extract_first()
        if elevator:
            item_loader.add_value("elevator", True)  
        furnished = response.xpath("//li[strong[contains(.,'Meublé')]]//text()[normalize-space()]").extract_first()
        if furnished:
            item_loader.add_value("furnished", True) 
        terrace = response.xpath("//li[strong[contains(.,'Terrasse')]]//text()[normalize-space()]").extract_first()
        if terrace:
            item_loader.add_value("terrace", True) 
        parking = response.xpath("//li[strong[contains(.,'Parking') or contains(.,'Garage')]]//text()[normalize-space()]").extract_first()
        if parking:
            item_loader.add_value("parking", True)
        swimming_pool = response.xpath("//li[strong[contains(.,'Piscine')]]//text()[normalize-space()]").extract_first()
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)      
        images = [response.urljoin(x) for x in response.xpath("//div[@class='flexslider carousel']//a/img/@src").extract()]
        if images:
            item_loader.add_value("images", images)

        item_loader.add_value("landlord_name", "SAVI ESTEVE")
        item_loader.add_value("landlord_phone","04 93 88 06 75")
        yield item_loader.load_item()


def energy_label_calculate(energy_number):
    energy_number = int(energy_number)
    energy_label = ""
    if energy_number <= 50:
        energy_label = "A"
    elif energy_number > 50 and energy_number <= 90:
        energy_label = "B"
    elif energy_number > 90 and energy_number <= 150:
        energy_label = "C"
    elif energy_number > 150 and energy_number <= 230:
        energy_label = "D"
    elif energy_number > 230 and energy_number <= 330:
        energy_label = "E"
    elif energy_number > 330 and energy_number <= 450:
        energy_label = "F"
    elif energy_number > 450:
        energy_label = "G"
    return energy_label