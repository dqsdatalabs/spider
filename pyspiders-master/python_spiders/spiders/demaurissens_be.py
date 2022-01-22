# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy.linkextractors import LinkExtractor
from scrapy import Request
from scrapy.selector import Selector 
from python_spiders.loaders import ListingLoader
import json
import re  
import dateparser    
  
class MySpider(Spider): 
    name = "demaurissens_be" 
    execution_type = 'testing'
    country = 'belgium'  
    locale = 'fr'  

    def start_requests(self):
        start_urls = ["https://www.demaurissens.be/page-data/fr/a-louer/page-data.json"]
        yield Request(
            url=start_urls[0],
            callback=self.parse 
        ) 

    # 1. FOLLOWING
    def parse(self, response):
        data_json = json.loads(response.body)
        data = data_json["result"]["pageContext"]["data"]["contentRow"][0]["data"]["propertiesList"]
        for item in data:
            if item["language"] == "fr":
                property_type = ""
                if get_p_type_string(item["TypeDescription"]):
                    property_type = get_p_type_string(item["TypeDescription"])
                elif get_p_type_string(item["DescriptionA"]):
                    property_type = get_p_type_string(item["DescriptionA"])
                else: property_type = ""
                
                if property_type:
                    f_url = item["TypeDescription"].replace("-","").replace(",","").replace(" ","-").lower().replace("à","a").replace("é","e").replace("è","").strip("-")
                    follow_url = f"https://www.demaurissens.be/fr/a-louer/{item['City'].lower()}/{f_url}/{item['ID']}/"
                    yield Request(
                        follow_url,
                        callback=self.populate_item,
                        meta={"property_type": property_type, "item":item}
                    )
                     

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", "Demaurissens_PySpider_" + self.country + "_" + self.locale)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        
        item = response.meta.get('item')
        item_loader.add_value("external_id", str(item['ID']))
        externalid=item_loader.get_output_value("external_id")
        if externalid and externalid=="1005938":
            return 

  

        title=item["TypeDescription"]
        item_loader.add_value("title",title)


        
        street = item["Street"]
        house_number = item["HouseNumber"]
        city = item["City"]
        zipcode = item["Zip"]
        item_loader.add_value("address", f"{street} {house_number} {city} {zipcode}")
        item_loader.add_value("city", city)
        item_loader.add_value("zipcode", zipcode)
        lat = item["GoogleX"]  
        lon = item["GoogleY"]
        item_loader.add_value("latitude", lat)
        item_loader.add_value("longitude", lon)
        item_loader.add_value("square_meters", item['SurfaceTotal'])       
        item_loader.add_value("description", item['DescriptionA'])
        item_loader.add_value("room_count", item['NumberOfBedRooms'])
        item_loader.add_value("rent", item['Price'])
        item_loader.add_value("currency", "EUR")
 
        utilitiess=item['DescriptionA']
        utilitiesindex=utilitiess.find('Charges')
        utilitiestext=utilitiess[utilitiesindex:]
        utilities=re.findall("\d+",utilitiestext)
        if utilities:
            item_loader.add_value("utilities",utilities)
        elif not utilities:
            uti=response.xpath("//script[contains(.,'charges')]/text()").get()
            if uti:
                utilitiesindex1=uti.find('Loyer')
                if utilitiesindex1: 
                    texta=uti[utilitiesindex1:]
                    text=texta.split('+ ')[-1].strip() 
                    textra=re.findall("\d+",text)
                    if textra:
                        renta=item["Price"]

                        if renta and int(textra[0])<int(renta):
                            item_loader.add_value("utilities",textra)





        
 
          
        furnishedd=item['DescriptionA'] 
        furnishedindex=re.search('meublé',furnishedd)
        if furnishedindex:
            item_loader.add_value("furnished",True) 
        elif not furnishedindex:
            meuble=response.xpath("//script[contains(.,'meuble')]/text()").get()
            if meuble:
                meublee=meuble.find("meuble")
                if meublee:
                    item_loader.add_value("furnished",True)

 
  
 
        bathroomcount=item['NumberOfBathRooms']
        if int(bathroomcount)>0:
           item_loader.add_value("bathroom_count", bathroomcount)
        elif int(bathroomcount)==0: 
            bathroomcount1=item['NumberOfShowerRooms']
            if bathroomcount1 >0:
               item_loader.add_value("bathroom_count", bathroomcount1)  
        






        terrace = item["HasTerrace"] 
        if terrace:
            item_loader.add_value("terrace", True)
        
        parking = item["NumberOfGarages"]
        if parking and parking !=0:
            item_loader.add_value("parking", True)
        
        elif not parking:
            park=response.xpath("//meta[contains(@name,'twitter:description')]//@content").get()
            if park:
                park1=re.search("parking",park)
                if park1:
                    item_loader.add_value("parking", True)

        
        if "LargePictureItems" in item:
            images = item["LargePictureItems"]
            for x in images:
                try:
                    item_loader.add_value("images", x["Url"])
                except: pass
            try:
                item_loader.add_value("energy_label", str(item['EnergyPerformance']))
            except: pass
        
        item_loader.add_value("landlord_name", "De Maurissens")
        item_loader.add_value("landlord_phone", "02 673 40 20")
        item_loader.add_value("landlord_email", "info@demaurissens.be")

        yield item_loader.load_item()
def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "unit" in p_type_string.lower() or "appartement" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "huis" in p_type_string.lower() or "duplex" in p_type_string.lower() or "fermette" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "house"
    else:
        return None