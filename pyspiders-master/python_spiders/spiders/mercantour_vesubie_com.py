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
    name = 'mercantour_vesubie_com'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_url = "http://www.mercantour-vesubie.com/a-louer-vesubie-arriere-pays-nice/1"
        yield Request(start_url, callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 2)
        seen = False

        for item in response.xpath("//ul[@class='listingUL']/li"):
            follow_url = response.urljoin(item.xpath("./@onclick").get().split("location.href='")[-1].split("'")[0])
            property_type = item.xpath(".//h2/text()").get()
            seen = True
            if property_type:
                if get_p_type_string(property_type): yield Request(follow_url, callback=self.populate_item, meta={"property_type":get_p_type_string(property_type)})

        if page == 2 or seen: yield Request(f"http://www.mercantour-vesubie.com/a-louer-vesubie-arriere-pays-nice/{page}", callback=self.parse, meta={"page":page+1})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Mercantour_Vesubie_PySpider_france")
        title = " ".join(response.xpath("//div[contains(@class,'bienTitle')]//h2/text()").extract())
        if title:
            item_loader.add_value("title", re.sub("\s{2,}", " ", title)) 
         
        zipcode = response.xpath("//p[span[.='Code postal']]/span[2]/text()").extract_first()
        if zipcode:
            item_loader.add_value("zipcode",zipcode.strip())   
        address =response.xpath("//p[span[.='Ville']]/span[2]/text()").extract_first()
        if address:
            item_loader.add_value("city",address.strip()) 
            if zipcode:
                address = address.strip()+", "+zipcode.strip()
            item_loader.add_value("address",address.strip()) 
        item_loader.add_xpath("floor", "//p[span[.='Etage']]/span[2]/text()")
        item_loader.add_xpath("bathroom_count", "//p[span[contains(.,'Nb de salle d')]]/span[2]/text()")
                
        external_id = response.xpath("substring-after(//span[@class='ref']/text(),'Ref')").extract_first()
        if external_id:
            item_loader.add_value("external_id",external_id.strip()) 
        room_count = response.xpath("//p[span[.='Nombre de chambre(s)']]/span[2]/text()").extract_first()
        if room_count:
            item_loader.add_value("room_count",room_count.strip())
        else:
            room_count = response.xpath("//p[span[.='Nombre de pièces']]/span[2]/text()").extract_first()
            if room_count:
                item_loader.add_value("room_count",room_count.strip())
   
        rent = "".join(response.xpath("//div[@class='value-prix']//text()").extract())
        if rent:     
            item_loader.add_value("rent_string",rent.replace(" ","").replace('\xa0', '')) 
     
        square = response.xpath("//p[span[contains(.,'Surface habitable')]]/span[2]/text()").extract_first()
        if square:
            square_meters = square.split("m")[0].strip()
            item_loader.add_value("square_meters",int(float(square_meters.replace(",",".")))) 
        deposit = response.xpath("//p[span[.='Dépôt de garantie TTC']]/span[2]/text()[not(contains(.,'Non'))]").extract_first()
        if deposit:
            item_loader.add_value("deposit",int(float(deposit.split("€")[0].strip().replace(" ","").replace(",",".")))) 
        utilities = response.xpath("//p[span[.='Honoraires TTC charge locataire']]/span[2]/text()[not(contains(.,'Non'))]").extract_first()
        if utilities:
            item_loader.add_value("utilities",int(float(utilities.split("€")[0].strip().replace(" ","").replace(",",".")))) 

        terrace = response.xpath("//p[span[.='Terrasse']]/span[2]/text()").extract_first()    
        if terrace:
            if terrace.upper().strip() =="NON":
                item_loader.add_value("terrace", False)
            elif terrace.upper().strip() == "OUI":
                item_loader.add_value("terrace", True)
        furnished =response.xpath("//p[span[.='Meublé']]/span[2]/text()").extract_first()  
        if furnished:
            if furnished.upper().strip() =="NON":
                item_loader.add_value("furnished", False)
            elif furnished.upper().strip() == "OUI":
                item_loader.add_value("furnished", True)
        elevator =response.xpath("//p[span[.='Ascenseur']]/span[2]/text()").extract_first()  
        if elevator:
            if elevator.upper().strip() =="NON":
                item_loader.add_value("elevator", False)
            elif elevator.upper().strip() == "OUI":
                item_loader.add_value("elevator", True)
        balcony =response.xpath("//p[span[.='Balcon']]/span[2]/text()").extract_first()  
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
     
        item_loader.add_value("landlord_name", "Agence du mercantour")
        item_loader.add_value("landlord_phone", "04 93 03 29 26")
        item_loader.add_value("landlord_email", "transaction@mercantour-vesubie.com")

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