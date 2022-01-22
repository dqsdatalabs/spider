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
    name = 'cabinetcoubertin_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.cabinetcoubertin.fr/annonces?id_polygon=&localisation_etendu=1&visite_virtuelle=&categorie=location&type_bien=appartement&nb_pieces=&surface=&budget=&localisation=&submit=Rechercher",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.cabinetcoubertin.fr/annonces?id_polygon=&localisation_etendu=1&visite_virtuelle=&categorie=location&type_bien=maison&nb_pieces=&surface=&budget=&localisation=&submit=Rechercher",
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
        for item in response.xpath("//p[@class='lien-detail']/a"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

        next_page = response.xpath("//div[@class='pagelinks-next']/a/@href").get()
        if next_page:
            p_url = response.urljoin(next_page)
            yield Request(
                p_url,
                callback=self.parse,
                meta={"property_type":response.meta["property_type"]})

    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split("?")[0])
        item_loader.add_value("external_source", "Cabinetcoubertin_PySpider_france")
        item_loader.add_xpath("title", "//h1/text()")        
         
        address =response.xpath("//ul/li[strong[contains(.,'Ville')]]/text()").extract_first()
        if address:
            item_loader.add_value("address",address ) 
        external_id = response.xpath("//ul/li[strong[.='Réf. : ']]/text()").extract_first()
        if external_id:
            item_loader.add_value("external_id",external_id.strip()) 
        room_count = response.xpath("//ul/li[strong[contains(.,'Nb. de chambres')]]/text()").extract_first()
        if room_count:
            item_loader.add_value("room_count",room_count) 
        else:
            room_count = response.xpath("//ul/li[strong[contains(.,'Nb. de pièces')]]/text()").extract_first()
            if room_count:
                item_loader.add_value("room_count",room_count) 

        item_loader.add_xpath("city", "//ul/li[strong[contains(.,'Ville')]]/text()")
        item_loader.add_xpath("zipcode", "//ul/li[strong[contains(.,'Code postal')]]/text()") 
        item_loader.add_xpath("bathroom_count", "//ul/li[strong[contains(.,'Nb. de salle d')]]/text()")
        rent =" ".join(response.xpath("//ul/li[strong[contains(.,'Loyer')]]/text()").extract())
        if rent:     
            item_loader.add_value("rent_string",rent.replace('\xa0', '').replace(' ',''))  
  
        utilities = " ".join(response.xpath("//ul/li[strong[contains(.,'Charges')]]/text()").extract())
        if utilities:   
            item_loader.add_value("utilities", utilities.replace(" ","").strip())  
        deposit = " ".join(response.xpath("//ul/li[strong[contains(.,'Dépot de garantie')]]/text()").extract())
        if deposit:   
            item_loader.add_value("deposit", deposit.replace(" ","").strip())  

        square =response.xpath("//ul/li[strong[contains(.,'Surface totale')]]/text()").extract_first()
        if square:
            square_meters =  square.split("m")[0].strip()
            item_loader.add_value("square_meters",int(float(square_meters.replace(",",".")))) 

        parking = response.xpath("//ul/li[strong[contains(.,'Nb. garage')]]/text()").extract_first()    
        if parking:
            item_loader.add_value("parking", True)
        terrace =response.xpath("//ul/li[strong[contains(.,'Terrasse')]]/text()").extract_first()    
        if terrace:
            if "non" in terrace.lower():
                item_loader.add_value("terrace", False)
            else:
                item_loader.add_value("terrace", True)
          
        desc = " ".join(response.xpath("//div[@class='description']/p/text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
              
        images = [response.urljoin(x) for x in response.xpath("//div[@id='photoslider']//ul/li/a/@href").extract()]
        if images:
                item_loader.add_value("images", images)
        latlng = response.xpath("//script[@type='text/javascript']/text()[contains(.,'latitude') and not(contains(.,'txt_secteur '))]").get()
        if latlng:
            item_loader.add_value("latitude", latlng.split("latitude =")[1].split(";")[0].strip())
            item_loader.add_value("longitude", latlng.split("longitude =")[1].split(";")[0].strip())
     
        item_loader.add_value("landlord_name", "CABINET COUBERTIN")
        item_loader.add_value("landlord_phone", "0473939100")
        
        yield item_loader.load_item()