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
    name = 'champagneimmobilier_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source="Champagneimmobilier_PySpider_france"
    custom_settings = {
        "PROXY_TR_ON": True,
        "CONCURRENT_REQUESTS" : 4,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": .5,
        "AUTOTHROTTLE_MAX_DELAY": 2,
        "RETRY_TIMES": 3,
        "DOWNLOAD_DELAY": 1,
    }
  
    def start_requests(self):
        formdata = {
            "data[Search][offredem]": "2",
            "data[Search][idtype]": "2",
            "data[Search][surfmin]": "",
            "data[Search][surfmax]": "",
            "data[Search][piecesmin]": "",
            "data[Search][piecesmax]": "",
            "data[Search][idvillecode]": "void",
            "data[Search][NO_DOSSIER]": "",
            "data[Search][distance_idvillecode]": "",
            "data[Search][prixmin]": "0",
            "data[Search][prixmax]": "600",
        }
        api_url = "https://champagneimmobilier.fr/recherche/"
        yield FormRequest(
            url=api_url,
            callback=self.parse,
            formdata=formdata,
            dont_filter=True,
            meta={
                "property_type":"apartment",
            })
            
    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//a[contains(@class,'btn-listing')]"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
            seen = True
        
        if page == 2 or seen:
            p_url = f"https://champagneimmobilier.fr/recherche//{page}"
            yield Request(
                p_url,
                callback=self.parse,
                meta={"page":page+1, "property_type":response.meta["property_type"]})

    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_source", self.external_source)
        title =response.xpath("//div[@class='bienTitle']/h2/text()").extract_first()
        if title:
            item_loader.add_value("title", re.sub("\s{2,}", " ", title))     
        address =response.xpath("//div/p[contains(.,'Ville')]/span[@class='valueInfos ']/text()").extract_first()
        if address:
            item_loader.add_value("address", address.strip()) 
        city =response.xpath("//div/p[contains(.,'Ville')]/span[@class='valueInfos ']/text()").extract_first()
        if city:
            item_loader.add_value("city", city.strip()) 
   
        zipcode =response.xpath("//div/p[contains(.,'Code postal')]/span[@class='valueInfos ']/text()").extract_first()
        if zipcode:
            item_loader.add_value("zipcode",zipcode.strip() ) 
        external_id = response.xpath("substring-after(//ul[@class='list-inline']/li/text()[contains(.,'Ref')],'Ref')").extract_first()
        if external_id:
            item_loader.add_value("external_id",external_id.strip()) 
        floor = response.xpath("//div/p[contains(.,'Etage')]/span[@class='valueInfos ']/text()").extract_first()
        if floor:
            item_loader.add_value("floor",floor.strip()) 
        room_count =response.xpath("//div/p[contains(.,'Nombre de chambre')]/span[@class='valueInfos ']/text()").extract_first()
        if room_count:
            item_loader.add_value("room_count",room_count.strip()) 
    
        bathroom_count =response.xpath("//div/p[contains(.,'Nb de salle d')]/span[@class='valueInfos ']/text()").extract_first()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count.strip()) 
 
        rent =" ".join(response.xpath("//ul[@class='list-inline']/li/text()[contains(.,'€')]").extract())
        if rent:     
            item_loader.add_value("rent_string",rent.replace(' ',''))  

        latitude = response.xpath("//script[contains(.,'getMapBien')]/text()").get()
        if latitude:
            item_loader.add_value("latitude", latitude.split('lat :')[1].split(',')[0].strip())
            item_loader.add_value("longitude", latitude.split('lng:')[1].split('}')[0].strip())
       
        utilities = response.xpath("//div/p[contains(.,'Charges ')]/span[@class='valueInfos ']/text()").extract_first()
        if utilities:   
            utilities = utilities.replace(" ","").split("€")[0].strip()
            item_loader.add_value("utilities", int(float(utilities.replace(",","."))))  
        deposit = response.xpath("//div/p[contains(.,'Dépôt de garantie')]/span[@class='valueInfos ']/text()").extract_first()
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
     
        item_loader.add_value("landlord_name", "CHAMPAGNE IMMOBILIER")
        item_loader.add_value("landlord_phone", "03 26 65 03 11")
        item_loader.add_value("landlord_email", "champagne.immobilier@wanadoo.fr")

        if not item_loader.get_collected_values("parking"):
            parking = response.xpath("//span[contains(.,'Nombre de parking')]/following-sibling::span/text()").get()
            if parking:
                if int(parking.strip()) > 0: item_loader.add_value("parking", True)
                else: item_loader.add_value("parking", False)

        yield item_loader.load_item()
