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
from datetime import datetime
import dateparser

class MySpider(Spider):
    name = 'agence_chaumette_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "http://www.agence-chaumette.fr/annonces-locations/35-location-maison.htm?p=1",
                    "http://www.agence-chaumette.fr/annonces-locations/35-location-duplex.htm?p=1",
                    "http://www.agence-chaumette.fr/annonces-locations/35-location-tous.htm?p=1"
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
        for item in response.xpath("//a[@class='positionImg']"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        sale = response.xpath("//h1//text()[contains(.,'Vente')]").extract_first()
        if sale:return
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split("?")[0])
        item_loader.add_value("external_source", "Agence_Chaumette_PySpider_france")
        item_loader.add_xpath("title", "//h1[@class='dummy']/text()")        
         
        address =response.xpath("//h1[@class='dummy']/text()[contains(.,'/')]").extract_first()
        if address:
            address = address.split("/")[0].strip()
            item_loader.add_value("address", address) 
            item_loader.add_value("city", address) 
        zipcode =response.xpath("//meta[@itemprop='postalcode']/@content").extract_first()
        if zipcode:
            item_loader.add_value("zipcode",zipcode ) 
        external_id = response.xpath("//span[@class='reference']/text()").extract_first()
        if external_id:
            item_loader.add_value("external_id",external_id.strip()) 
        floor = response.xpath("//ul/li[span[contains(.,'Etage :')]]/text()[normalize-space()]").extract_first()
        if floor:
            item_loader.add_value("floor",floor.strip()) 
        
        room_count ="".join(response.xpath("//ul/li[span[contains(.,'Nombre de chambre')]]//text()").extract())
        if room_count:
            item_loader.add_value("room_count",room_count.split(":")[-1].strip()) 
        else:
            room_count ="".join(response.xpath("//ul/li[span[contains(.,'Nombre de pièce')]]//text()").extract())
            if room_count:
                item_loader.add_value("room_count",room_count.split(":")[-1].strip()) 
        bathroom_count ="".join(response.xpath("//ul/li[span[contains(.,'Nombre de salle(s) d')]]//text()").extract())
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count.split(":")[-1].strip()) 
        item_loader.add_xpath("latitude","substring-before(substring-after(//a/@href[contains(.,'centerLat')],'centerLat='),'&')") 
        item_loader.add_xpath("longitude","substring-after(//a/@href[contains(.,'centerLat')],'centerLng=')") 

        
        # lat_lng = response.xpath("normalize-space(//a/@href[contains(.,'Lat')])").extract_first()
        # print("--------",lat_lng)
        # if lat_lng:
        #     item_loader.add_value("latitude", lat_lng.split("Lat="[1]).split("&")[0].strip())
        #     item_loader.add_value("longitude", lat_lng.split("Lng="[1]).strip())
 
        rent =" ".join(response.xpath("//h2[@class='prix']/text()").extract())
        if rent:     
            item_loader.add_value("rent_string",rent.replace(' ',''))  
       
        utilities = " ".join(response.xpath("//div[@class='caracteristique']//p//text()[contains(.,'Honoraires charge locataire')]").extract())
        if utilities:   
            utilities = utilities.split("Honoraires charge locataire")[1].split("€")[0]
            item_loader.add_value("utilities", utilities.replace(" ","").strip())  
        deposit = " ".join(response.xpath("//div[@class='caracteristique']//p[contains(.,'Dépôt de garantie')]//text()").extract())
        if deposit:   
            deposit = deposit.split("Dépôt de garantie")[1].split("€")[0]
            item_loader.add_value("deposit", deposit.replace(" ","").strip())  
        

        available_date = response.xpath("//ul/li[span[contains(.,'Disponible')]]/text()[normalize-space()]").get()
        if available_date:
            if "immédiatement" in available_date:
                item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
            else:
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
        square ="".join(response.xpath("//ul/li[span[contains(.,'Surface')]]//text()").extract())
        if square:
            square_meters = square.split(":")[1].split("m")[0].strip()
            item_loader.add_value("square_meters",square_meters) 

        energy_label = response.xpath("substring-after(//span[@class='dpe']//span[@class='libelle']/text(),'Cat.')").extract_first()    
        if energy_label:
            energy_label = energy_label.split(":")[0].strip()
            if energy_label in ["A","B","C","D","E","F","G"]:
                item_loader.add_value("energy_label", energy_label)
        parking = response.xpath("//ul/li[span[contains(.,'Nombre de parking')]]/text()[normalize-space()]").extract_first()    
        if parking:
            if "non" in parking.lower() or "0" in parking:
                item_loader.add_value("parking", False)
            else:
                item_loader.add_value("parking", True)
        terrace =response.xpath("//ul/li[span[contains(.,'Nombre de terrasse')]]/text()[normalize-space()]").extract_first()    
        if terrace:
            if "non" in terrace.lower() or "0" in terrace.lower():
                item_loader.add_value("terrace", False)
            else:
                item_loader.add_value("terrace", True)
        furnished ="".join(response.xpath("//ul/li[span[contains(.,'Meublé')]]//text()").extract())   
        if furnished:
            if "no" in furnished.lower():
                item_loader.add_value("furnished", False)
            else:
                item_loader.add_value("furnished", True)
          
        desc = " ".join(response.xpath("//div[@class='description']/p//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
              
        images = [response.urljoin(x) for x in response.xpath("//div[@id='carousel-bien']//a/img/@src").extract()]
        if images:
            item_loader.add_value("images", images)
     
        item_loader.add_value("landlord_name", "Agence Chaumette")
        item_loader.add_value("landlord_phone", "01 60 66 40 24")
        item_loader.add_value("landlord_email", "agence-chaumette@wanadoo.fr")
        
        yield item_loader.load_item()