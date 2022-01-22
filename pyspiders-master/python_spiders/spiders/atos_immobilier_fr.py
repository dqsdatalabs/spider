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
    name = 'atos_immobilier_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    
    def start_requests(self):
        start_urls = [
            {
                "property_type" : "apartment",
                "type" : "2",
            },
        ]
        for item in start_urls:
            formdata = {
                "data[Search][offredem]": "2",
                "data[Search][idtype][]": item["type"],
                "data[Search][prixmax]": "",
                "data[Search][piecesmin]": "",
                "data[Search][NO_DOSSIER]": "",
                "data[Search][distance_idvillecode]": "",
                "data[Search][prixmin]": "",
                "data[Search][surfmin]": "",
            }
            api_url = "https://www.atos-immobilier.fr/votre-recherche/"
            yield FormRequest(
                url=api_url,
                callback=self.parse,
                formdata=formdata,
                dont_filter=True,
                meta={
                    "property_type":item["property_type"],
                })

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//a[@class='block-link']"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_source", "Atos_Immobilier_PySpider_france")
        title =response.xpath("//div[@class='bienTitle row']//h2/text()").extract_first()
        if title:
            item_loader.add_value("title", re.sub("\s{2,}", " ", title))   
        
        zipcode =response.xpath("//div/p[contains(.,'Code postal')]/span[@class='valueInfos ']/text()").extract_first()
        if zipcode:
            item_loader.add_value("zipcode",zipcode.strip() ) 
        city =response.xpath("//div/p[contains(.,'Ville')]/span[@class='valueInfos ']/text()").extract_first()
        if city:
            item_loader.add_value("city", city.strip()) 
            if zipcode:
                city = city.strip()+" ("+zipcode.strip()+")"
            item_loader.add_value("address", city.strip()) 
        
        external_id = response.xpath("substring-after(//span[@class='ref']/text()[contains(.,'Ref')],'Ref')").extract_first()
        if external_id:
            item_loader.add_value("external_id",external_id.strip()) 
        floor = response.xpath("//div/p[contains(.,'Etage')]/span[@class='valueInfos ']/text()").extract_first()
        if floor:
            item_loader.add_value("floor",floor.strip()) 
        room_count =response.xpath("//div/p[contains(.,'Nombre de chambre')]/span[@class='valueInfos ']/text()").extract_first()
        if room_count:
            item_loader.add_value("room_count",room_count.strip()) 
        else:
            room_count =response.xpath("//div/p[contains(.,'Nombre de pièces')]/span[@class='valueInfos ']/text()").extract_first()
            if room_count:
                item_loader.add_value("room_count",room_count.strip()) 
    
        bathroom_count =response.xpath("//div/p[contains(.,'Nb de salle d')]/span[@class='valueInfos ']/text()").extract_first()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count.strip()) 
 
        rent =" ".join(response.xpath("//div/p[contains(.,'Loyer CC* / mois')]/span[@class='valueInfos ']/text()[not(contains(.,'Non'))]").extract())
        if rent:     
            item_loader.add_value("rent",rent.split("€")[0].strip().replace(' ',''))  
        item_loader.add_value("currency", "EUR")  
       
        utilities = response.xpath("//div/p[contains(.,'Charges ')]/span[@class='valueInfos ']/text()[not(contains(.,'Non'))]").extract_first()
        if utilities:   
            utilities = utilities.replace(" ","").split("€")[0].strip()
            item_loader.add_value("utilities", int(float(utilities.replace(",","."))))  
        deposit = response.xpath("//div/p[contains(.,'Dépôt de garantie')]/span[@class='valueInfos ']/text()[not(contains(.,'Non'))]").extract_first()
        if deposit:    
            deposit = deposit.replace(" ","").split("€")[0].strip()
            item_loader.add_value("deposit", int(float(deposit.replace(",","."))))  
  
        square = response.xpath("//div/p[contains(.,'Surface habitable')]/span[@class='valueInfos ']/text()").extract_first()
        if square:
            square_meters = square.split("m")[0].strip()
            item_loader.add_value("square_meters",int(float(square_meters.replace(",",".")))) 
        parking = response.xpath("//div/p[contains(.,'Nombre de garage')]/span[@class='valueInfos ']/text()").extract_first()    
        if parking:
            if "non" in parking.lower() or "0" in parking:
                item_loader.add_value("parking", False)
            else:
                item_loader.add_value("parking", True)
        terrace = response.xpath("//div/p[contains(.,'Terrasse')]/span[@class='valueInfos ']/text()").extract_first()    
        if terrace:
            if terrace.upper().strip() =="NON":
                item_loader.add_value("terrace", False)
            elif terrace.upper().strip() == "OUI":
                item_loader.add_value("terrace", True)
        furnished =response.xpath("//div/p[contains(.,'Meublé')]/span[@class='valueInfos ']/text()").extract_first()  
        if furnished:
            if furnished.upper().strip() =="NON":
                item_loader.add_value("furnished", False)
            elif furnished.upper().strip() == "OUI":
                item_loader.add_value("furnished", True)
        elevator =response.xpath("//div/p[contains(.,'Ascenseur')]/span[@class='valueInfos ']/text()").extract_first()  
        if elevator:
            if elevator.upper().strip() =="NON":
                item_loader.add_value("elevator", False)
            elif elevator.upper().strip() == "OUI":
                item_loader.add_value("elevator", True)
        balcony =response.xpath("//div/p[contains(.,'Balcon')]/span[@class='valueInfos ']/text()").extract_first()  
        if balcony:
            if balcony.upper().strip() =="NON":
                item_loader.add_value("balcony", False)
            elif balcony.upper().strip() == "OUI":
                item_loader.add_value("balcony", True) 
        desc = " ".join(response.xpath("//p[@itemprop='description']//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
              
        images = [response.urljoin(x) for x in response.xpath("//ul[contains(@class,'imageGallery')]/li/img/@src").extract()]
        if images:
            item_loader.add_value("images", images)
        latlng = response.xpath("//script/text()[contains(.,'center: { lat : ')]").get()
        if latlng:
            item_loader.add_value("latitude", latlng.split("center: { lat :")[1].split(",")[0].strip())
            item_loader.add_value("longitude", latlng.split("center: { lat :")[1].split("lng:")[1].split("}")[0].strip())
        item_loader.add_value("landlord_name", "Atos immobilier")
        item_loader.add_value("landlord_phone", "0558713391")
        item_loader.add_value("landlord_email", "contact@atos-immobilier.fr")
        
        yield item_loader.load_item()
