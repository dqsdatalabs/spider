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
    name = 'delta_immobilier_com'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.delta-immobilier.com/annonces?id_polygon=&localisation_etendu=1&visite_virtuelle=&categorie=location&type_bien=appartement&nb_pieces=&surface=&budget=&localisation=&submit=Rechercher",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.delta-immobilier.com/annonces?id_polygon=&localisation_etendu=1&visite_virtuelle=&categorie=location&type_bien=maison&nb_pieces=&surface=&budget=&localisation=&submit=Rechercher",
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

        for item in response.xpath("//h2[@itemprop='name']/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta['property_type']})

        
        next_page = response.xpath("//a[.='Suivante']/@href").get()
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
        item_loader.add_xpath("title", "//div/h1/text()")
     
        item_loader.add_value("external_source", "Delta_Immobilier_PySpider_france")
        item_loader.add_value("external_id",response.xpath("//li[strong[contains(.,'Réf.')]]/text()").extract_first().strip().replace("\n",""))    
        item_loader.add_xpath("city","//li[strong[contains(.,'Ville')]]/text()")    
        item_loader.add_xpath("address","//li[strong[contains(.,'Ville')]]/text()")    
        item_loader.add_xpath("zipcode","//li[strong[contains(.,'Code postal')]]/text()")    
       
                        
        rent = " ".join(response.xpath("//div[@class='head-offre-prix']//a//text()").extract())
        if rent:     
           item_loader.add_value("rent_string", rent.replace(" ",""))   
      
        floor = response.xpath("//li[strong[contains(.,'Etage')]]/text()").extract_first() 
        if floor:   
            item_loader.add_value("floor",floor.strip())      
        
        room_count = response.xpath("//li[strong[contains(.,'chambres')]]/text()").extract_first() 
        if room_count:   
            item_loader.add_value("room_count",room_count.strip())
        else:
            room_count = response.xpath("//li[strong[contains(.,'pièces')]]/text()").extract_first() 
            if room_count:   
                item_loader.add_value("room_count",room_count.strip())
        
        available_date = response.xpath("//li[strong[contains(.,'Disponibilité')]]/text()").extract_first() 
        if available_date:
            if "immédiatement" in available_date:
                available_date = datetime.now().strftime("%Y-%m-%d")
                item_loader.add_value("available_date", available_date)
            else:
                date_parsed = dateparser.parse(available_date.strip(), languages=['fr'])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)

        square =response.xpath("//li[strong[contains(.,'Surface totale')]]/text()").extract_first()
        if square:
            square_meters =  square.split("m")[0].strip()
            item_loader.add_value("square_meters", int(float(square_meters.replace(",",".")))) 
        
        utilities ="".join(response.xpath("//li[strong[contains(.,'Charges')]]/text()").extract())
        if utilities:
            item_loader.add_value("utilities",utilities.replace(" ","")) 

        elevator =response.xpath("//li[strong[contains(.,'Ascenseur')]]/text()").extract_first()    
        if elevator:
            if "non" in elevator.lower():
                item_loader.add_value("elevator", False)
            else:
                item_loader.add_value("elevator", True)
        parking =response.xpath("//li[strong[contains(.,'parking')]]/text()").extract_first()    
        if parking:
            if "non" in parking.lower() or "0" in parking:
                item_loader.add_value("parking", False)
            else:
                item_loader.add_value("parking", True)
        
        desc = " ".join(response.xpath("//p[@itemprop='description']//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
         
        energy = response.xpath("//div[@class='diagnostic_images']//img[contains(@data-src,'dpe')]/@data-src").extract_first() 
        if energy:   
            energy = energy.split("dpe/")[1].split("/")[0].strip()            
            item_loader.add_value("energy_label",energy_label_calculate(energy))
        
        images = [response.urljoin(x)for x in response.xpath("//div[@id='photoslider']//li//img/@src").extract()]
        if images:
                item_loader.add_value("images", images)

        item_loader.add_value("landlord_name", "Agence Delta")
        item_loader.add_value("landlord_phone", "04 94 75 61 30")
      
        script_map = response.xpath("//script[@type='text/javascript']//text()[contains(.,'longitude') and not(contains(.,'default_longitude'))]").get()
        if script_map:
            item_loader.add_value("latitude", script_map.split("latitude =")[1].split(";")[0].strip())
            item_loader.add_value("longitude", script_map.split("longitude =")[1].split(";")[0].strip())
 
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