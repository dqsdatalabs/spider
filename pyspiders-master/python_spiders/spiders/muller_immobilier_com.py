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
    name = 'muller_immobilier_com'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = ["https://www.muller-immobilier.com/a-louer/1"]
        yield Request(start_urls[0], callback=self.parse)   

    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 2)
        seen = False

        for item in response.xpath("//div[contains(@class,'bien')]"):
            follow_url = response.urljoin(item.xpath(".//div[@class='overlay']/a/@href").get())
            property_type = item.xpath(".//p[@class='card-text']/text()[1]").get()
            if property_type:
                if get_p_type_string(property_type): 
                    seen = True
                    yield Request(follow_url, callback=self.populate_item, meta={"property_type":get_p_type_string(property_type)})

        if page == 2 or seen: yield Request("https://www.muller-immobilier.com/a-louer/" + str(page), callback=self.parse, meta={"page":page+1})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Muller_Immobilier_PySpider_france")
        
        item_loader.add_xpath("title", "//div/h1[@itemprop='name']/span/text()")      
        zipcode = response.xpath("//table//tr[th[.='Code postal']]/th[2]/text()").extract_first()
        if zipcode:
            item_loader.add_value("zipcode",zipcode.strip())   
        address =response.xpath("//table//tr[th[.='Ville']]/th[2]/text()").extract_first()
        if address:
            if zipcode:
                address = address.strip()+", "+zipcode.strip()
            item_loader.add_value("address",address.strip()) 
        item_loader.add_xpath("city", "//table//tr[th[.='Ville']]/th[2]/text()")
        item_loader.add_xpath("floor", "//table//tr[th[.='Etage']]/th[2]/text()")
        item_loader.add_xpath("bathroom_count", "//table//tr[th[contains(.,'Nb de salle d')]]/th[2]/text()")
                
        external_id = response.xpath("//h2/span[contains(.,'Ref')]/following-sibling::text()[1]").extract_first()
        if external_id:
            item_loader.add_value("external_id",external_id.strip()) 
        room_count = response.xpath("//table//tr[th[.='Nombre de chambre(s)']]/th[2]/text()").extract_first()
        if room_count:
            item_loader.add_value("room_count",room_count.strip())
        elif "studio" in response.meta["property_type"]:
            item_loader.add_value("room_count", "1")
   
        rent = response.xpath("//table//tr[th[contains(.,'Loyer CC* / mois')]]/th[2]/text()").extract_first()
        if rent:     
            rent = rent.split("€")[0].strip().replace(" ","")
            item_loader.add_value("rent",int(float(rent.replace(",",".")))) 
        item_loader.add_value("currency", "EUR") 

        square = response.xpath("//table//tr[th[contains(.,'Surface habitable')]]/th[2]/text()").extract_first()
        if square:
            square_meters = square.split("m")[0].strip()
            item_loader.add_value("square_meters",int(float(square_meters.replace(",",".")))) 
        deposit = response.xpath("//table//tr[th[.='Dépôt de garantie TTC']]/th[2]/text()").extract_first()
        if deposit:
            item_loader.add_value("deposit",int(float(deposit.split("€")[0].strip().replace(",",".").replace(" ","")))) 
        utilities = response.xpath("//table//tr[th[contains(.,'Charges ')]]/th[2]/text()").extract_first()
        if utilities:
            item_loader.add_value("utilities",int(float(utilities.split("€")[0].strip().replace(",",".")))) 

        parking = response.xpath("//table//tr[th[.='Nombre de garage' or .='Nombre de parking']]/th[2]/text()").extract_first()    
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
        latlng = response.xpath("//script/text()[contains(.,'Map.setCenter(map, {')]").get()
        if latlng:
            latlng = latlng.split("Map.setCenter(map, {")[1].split("}")[0].strip()
            item_loader.add_value("latitude", latlng.split("lat:")[1].split(",")[0].strip())
            item_loader.add_value("longitude", latlng.split("lng:")[1].strip())      
        images = [response.urljoin(x) for x in response.xpath("//ul[contains(@class,'imageGallery')]/li/img/@src").extract()]
        if images:
            item_loader.add_value("images", images)
     
        item_loader.add_value("landlord_name", "AGENCE MULLER IMMOBILIER")
        item_loader.add_value("landlord_phone", "04 74 35 73 55")
         
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and "appartement" in p_type_string.lower():
        return "apartment"
    elif p_type_string and "maison" in p_type_string.lower():
        return "house"
    else:
        return None