# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json

class MySpider(Spider):
    name = 'vivreoceanbleu_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source="Vivreoceanbleu_PySpider_france"
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
            "data[Search][idtype][]": "1",
            "data[Search][prixmax]": "",
            "data[Search][piecesmin]": "",
            "data[Search][NO_DOSSIER]": "",
            "data[Search][distance_idvillecode]": "",
            "data[Search][prixmin]": "",
            "data[Search][surfmin]": "",
        }
        api_url = "http://www.vivreoceanbleu.com/recherche/"
        yield FormRequest(
            url=api_url,
            callback=self.parse,
            formdata=formdata,
            dont_filter=True,
            meta={
                "property_type":"house",
            })

    p_info = True
    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//article[@class='card']/a"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
            seen = True
        
        if page == 2 or seen:
            p_url = f"http://www.vivreoceanbleu.com/recherche/{page}"
            yield Request(
                p_url,
                callback=self.parse,
                meta={"page":page+1, "property_type":response.meta["property_type"]})
        elif self.p_info:
            self.p_info = False
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
            api_url = "http://www.vivreoceanbleu.com/recherche/"
            yield FormRequest(
                url=api_url,
                callback=self.parse,
                formdata=formdata,
                dont_filter=True,
                meta={
                    "property_type":"apartment",
                })
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_source", self.external_source)
      
        item_loader.add_xpath("title", "//h1/span/text()")     
        item_loader.add_xpath("zipcode", "//table//tr[th[.='Code postal']]/th[2]/text()")
        item_loader.add_xpath("city", "//table//tr[th[.='Ville']]/th[2]/text()")
        item_loader.add_xpath("address", "//table//tr[th[.='Ville']]/th[2]/text()")
        item_loader.add_xpath("room_count", "//table//tr[th[.='Nombre de chambre(s)']]/th[2]/text()")
        item_loader.add_xpath("floor", "//table//tr[th[.='Etage']]/th[2]/text()")
        item_loader.add_xpath("bathroom_count", "//table//tr[th[contains(.,'Nb de salle d')]]/th[2]/text()")
        item_loader.add_xpath("deposit", "//table//tr[th[.='Dépôt de garantie TTC']]/th[2]/text()")
        item_loader.add_xpath("utilities", "//table//tr[th[contains(.,'Charges ')]]/th[2]/text()")
        
        external_id = response.xpath("//h2/span[contains(.,'Ref')]/following-sibling::text()[1]").extract_first()
        if external_id:
            item_loader.add_value("external_id",external_id.strip()) 
   
        rent = response.xpath("//table//tr[th[contains(.,'Loyer CC* / mois')]]/th[2]/text()").extract_first()
        if rent:     
            item_loader.add_value("rent_string",rent.replace(' ',''))  

        square = response.xpath("//table//tr[th[contains(.,'Surface habitable')]]/th[2]/text()").extract_first()
        if square:
            square_meters = square.split("m")[0].strip()
            item_loader.add_value("square_meters",int(float(square_meters.replace(",",".")))) 
        parking = response.xpath("//table//tr[th[.='Nombre de garage']]/th[2]/text()").extract_first()    
        if parking:
            if "non" in parking.lower() or "0" in parking:
                item_loader.add_value("parking", False)
            else:
                item_loader.add_value("parking", True)
        terrace = response.xpath("//table//tr[th[.='Terrasse']]/th[2]/text()").extract_first()    
        if terrace:
            if terrace.upper().strip() =="NON":
                item_loader.add_value("terrace", False)
            elif terrace.upper().strip() == "OUI":
                item_loader.add_value("terrace", True)
        furnished =response.xpath("//table//tr[th[.='Meublé']]/th[2]/text()").extract_first()  
        if furnished:
            if furnished.upper().strip() =="NON":
                item_loader.add_value("furnished", False)
            elif furnished.upper().strip() == "OUI":
                item_loader.add_value("furnished", True)
        elevator =response.xpath("//table//tr[th[.='Ascenseur']]/th[2]/text()").extract_first()  
        if elevator:
            if elevator.upper().strip() =="NON":
                item_loader.add_value("elevator", False)
            elif elevator.upper().strip() == "OUI":
                item_loader.add_value("elevator", True)
        balcony =response.xpath("//table//tr[th[.='Balcon']]/th[2]/text()").extract_first()  
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

        latitude_longitude = response.xpath("//script[contains(.,'setCenter')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('lat :')[1].split(',')[0]
            longitude = latitude_longitude.split('lng:')[1].split('}')[0].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
     
        item_loader.add_xpath("landlord_name", "//div[@class='media-body']/span[1]/text()")
        item_loader.add_xpath("landlord_phone", "//div[@class='media-body']/span[2]/text()")
        item_loader.add_xpath("landlord_email", "//div[@class='media-body']/span/a/text()")
        
        yield item_loader.load_item()
