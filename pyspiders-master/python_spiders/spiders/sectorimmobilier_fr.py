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
    name = 'sectorimmobilier_fr'
    execution_type='testing'
    country='france'
    locale='fr'
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
            "data[Search][idtype][]": "2",
            "data[Search][prixmax]": "",
            "data[Search][piecesmin]": "",
            "data[Search][NO_DOSSIER]": "",
            "data[Search][distance_idvillecode]": "",
            "data[Search][prixmin]": "",
            "data[Search][surfmin]": "",
        }
        api_url = "https://www.sectorimmobilier.fr/recherche/"
        yield FormRequest(
            url=api_url,
            callback=self.parse,
            formdata=formdata,
            dont_filter=True,
            meta={
                "property_type":"apartment",
            })

    p_info = True
    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//a[@class='btn-primary']"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
            seen = True
        
        if page == 2 or seen:
            p_url = f"https://www.sectorimmobilier.fr/recherche/{page}"
            yield Request(
                p_url,
                dont_filter=True,
                callback=self.parse,
                meta={"page":page+1,"property_type":response.meta["property_type"]},
            )
        elif self.p_info:
            self.p_info = False
            formdata = {
                "data[Search][offredem]": "2",
                "data[Search][idtype][]": "4",
                "data[Search][prixmax]": "",
                "data[Search][piecesmin]": "",
                "data[Search][NO_DOSSIER]": "",
                "data[Search][distance_idvillecode]": "",
                "data[Search][prixmin]": "",
                "data[Search][surfmin]": "",
            }
            api_url = "https://www.sectorimmobilier.fr/recherche/"
            yield FormRequest(
                url=api_url,
                callback=self.parse,
                formdata=formdata,
                dont_filter=True,
                meta={
                    "property_type":"studio",
                })

        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_source", "Sectorimmobilier_PySpider_france")
        item_loader.add_xpath("title", "//div/h1[@itemprop='name']/text()")        
         
        address =", ".join(response.xpath("//div/p[contains(.,'Quartier')]/span/text() | //div/p[contains(.,'Ville')][1]/span/text()").extract())
        if address:
            item_loader.add_value("address", re.sub("\s{2,}", " ", address)) 
    
        city =response.xpath("//div/p[contains(.,'Ville')]/span/text()").extract_first()
        if city:
            item_loader.add_value("city", city.strip()) 
        zipcode =response.xpath("//div/p[contains(.,'Code postal')]/span/text()").extract_first()
        if zipcode:
            item_loader.add_value("zipcode",zipcode.strip() ) 
        external_id = response.xpath("substring-after(//span[@itemprop='productID']//text(),':')").extract_first()
        if external_id:
            item_loader.add_value("external_id",external_id.strip()) 
        floor = response.xpath("//div/p[contains(.,'Etage')]/span/text()").extract_first()
        if floor:
            item_loader.add_value("floor",floor.strip()) 
        
        room_count =response.xpath("//div/p[contains(.,'Nombre de chambre')][1]/span/text()").extract_first()
        if room_count:
            item_loader.add_value("room_count",room_count.strip()) 
        else:
            room_count =response.xpath("//div/p[contains(.,'Nombre de pièces')]/span/text()").extract_first()
            if room_count:
                item_loader.add_value("room_count",room_count.strip()) 
        
        bathroom_count =response.xpath("//div/p[contains(.,'Nb de salle d')]/span/text()").extract_first()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count.strip()) 
 
        rent =" ".join(response.xpath("//div/p[contains(.,'Prix du bien')]/span/text()").extract())
        if rent:     
            item_loader.add_value("rent_string",rent.replace(' ',''))  
       
        utilities = " ".join(response.xpath("//div/p[contains(.,'Charges')]/span/text()").extract())
        if utilities:   
            utilities = utilities.replace(" ","").split("€")[0].strip()
            item_loader.add_value("utilities", int(float(utilities.replace(",","."))))  
        deposit = " ".join(response.xpath("//div/p[contains(.,'Dépôt de garantie')]/span/text()").extract())
        if deposit:    
            deposit = deposit.replace(" ","").split("€")[0].strip()
            item_loader.add_value("deposit", int(float(deposit.replace(",","."))))  
  
        square =response.xpath("//div/p[contains(.,'Surface habitable')]/span/text()").extract_first()
        if square:
            square_meters = square.split("m")[0].strip()
            item_loader.add_value("square_meters",int(float(square_meters.replace(",",".")))) 

        terrace = response.xpath("//div/p[contains(.,'Terrasse')]/span/text()").extract_first()    
        if terrace:
            if "non" in terrace.lower():
                item_loader.add_value("terrace", False)
            else:
                item_loader.add_value("terrace", True)
        furnished =response.xpath("//div/p[contains(.,'Meublé')]/span/text()").extract_first()  
        if furnished:
            if "non" in furnished.lower():
                item_loader.add_value("furnished", False)
            else:
                item_loader.add_value("furnished", True)
        elevator =response.xpath("//div/p[contains(.,'Ascenseur')]/span/text()").extract_first()  
        if elevator:
            if "non" in elevator.lower():
                item_loader.add_value("elevator", False)
            else:
                item_loader.add_value("elevator", True)
        balcony =response.xpath("//div/p[contains(.,'Balcon')]/span/text()").extract_first()  
        if balcony:
            if "non" in balcony.lower():
                item_loader.add_value("balcony", False)
            else:
                item_loader.add_value("balcony", True)  
        desc = " ".join(response.xpath("//p[@itemprop='description']//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
              
        images = [response.urljoin(x) for x in response.xpath("//ul[contains(@class,'imageGallery')]/li/img/@src").extract()]
        if images:
            item_loader.add_value("images", images)
        latlng = response.xpath("//script/text()[contains(.,'Map.setCenter(map, {')]").get()
        if latlng:
            latlng = latlng.split("Map.setCenter(map, {")[1].split("}")[0].strip()
            item_loader.add_value("latitude", latlng.split("lat:")[1].split(",")[0].strip())
            item_loader.add_value("longitude", latlng.split("lng:")[1].strip())
     
        item_loader.add_value("landlord_name", "SECTOR IMMOBILIER")
        item_loader.add_value("landlord_phone", "01 42 29 29 00")
        item_loader.add_value("landlord_email", "sectorimmobilier@orange.fr")
        
        yield item_loader.load_item()
