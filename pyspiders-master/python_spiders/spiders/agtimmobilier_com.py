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
    name = 'agtimmobilier_com'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "http://agtimmobilier.com/dmxConnect/api/listeaffaires.php?typetransaction=LOCATION&typeaffaire=Appartement&prixmandateuro=100000000&bienville=&nbpieces=0&nmandat=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "http://agtimmobilier.com/dmxConnect/api/listeaffaires.php?typetransaction=LOCATION&typeaffaire=Maison&prixmandateuro=100000000&bienville=&nbpieces=0&nmandat=",
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item, callback=self.parse, meta={'property_type': url.get('property_type')})

    def parse(self, response):
        data = json.loads(response.body)
        for item in data["selectionaffaires"]:
            follow_url = "http://agtimmobilier.com/ficheaffaire.php?code=" + item["Code"]
            yield Request(follow_url, callback=self.jump, meta={"property_type": response.meta["property_type"]})

    def jump(self, response):      
        external_id = response.url.split("code=")[1]
        url = f"http://agtimmobilier.com/dmxConnect/api/selectfiche.php?code={external_id}"
        yield Request(url, callback=self.populate_item, meta={"property_type": response.meta["property_type"], "url":response.url})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
 
        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_link", response.meta.get("url"))
        item_loader.add_value("external_source", "Agtimmobilier_PySpider_france")
        data = json.loads(response.body)
        
        item_loader.add_value("title", data["selectionaffaire"]["CritC1"])
        external_id = data["selectionaffaire"]["Code"]
        item_loader.add_value("external_id", external_id)
        item_loader.add_value("address", data["selectionaffaire"]["BienVille"])
        item_loader.add_value("city", data["selectionaffaire"]["BienVille"])
        item_loader.add_value("zipcode", data["selectionaffaire"]["BienCP"])
        
        import dateparser
        available_date = data["selectionaffaire"]["LibreLe"]
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)

        item_loader.add_value("description", data["selectionaffaire"]["TextePub"])

        if data["selectionaffaire"]["NbChambres"] != '0':
            item_loader.add_value("room_count", data["selectionaffaire"]["NbChambres"])
        else:
            item_loader.add_value("room_count", data["selectionaffaire"]["NbPieces"])
        
        if data["selectionaffaire"]["SurfHab"] !='0':
            item_loader.add_value("square_meters", data["selectionaffaire"]["SurfHab"])
        item_loader.add_value("floor", data["selectionaffaire"]["Etage"])
        
        rent = data["selectionaffaire"]["PrixMandatEuro"]
        if rent:
            rent = rent.split(".")[0]
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")
        
        if data["selectionaffaire"]["Balcon"] and data["selectionaffaire"]["Balcon"]!='0':
            item_loader.add_value("balcony", True)
            
        if data["selectionaffaire"]["Terrasse"] and data["selectionaffaire"]["Terrasse"]!='0':
            item_loader.add_value("terrace", True)
        
        if data["selectionaffaire"]["Ascenseur"] and data["selectionaffaire"]["Ascenseur"]!='0':
            item_loader.add_value("elevator", True)
            
        if data["selectionaffaire"]["Garage"] and data["selectionaffaire"]["Garage"]!='0':
            item_loader.add_value("parking", True)
        if data["selectionaffaire"]["Parking"] and data["selectionaffaire"]["Parking"]!='0':
            item_loader.add_value("parking", True)
        
        if data["selectionaffaire"]["Meuble"] and data["selectionaffaire"]["Meuble"]!='0':
            item_loader.add_value("furnished", True)
        
        if data["selectionaffaire"]["Piscine"] and data["selectionaffaire"]["Piscine"]!='0':
            item_loader.add_value("swimming_pool", True)
        
        utilities = int(float(data["selectionaffaire"]["Charges"]))
        if utilities and utilities!=0:
            item_loader.add_value("utilities", utilities)

        item_loader.add_value("deposit", int(float(data["selectionaffaire"]["DepotGarantie"])))
        item_loader.add_value("energy_label", data["selectionaffaire"]["DPELettre"])
        item_loader.add_value("latitude", data["selectionaffaire"]["Lat"])
        item_loader.add_value("longitude", data["selectionaffaire"]["Lg"])
        
        item_loader.add_value("landlord_name", "AGT IMMOBILIER")
        item_loader.add_value("landlord_phone", "03 44 05 50 24")
        item_loader.add_value("landlord_email", "agtimmobilier@wanadoo.fr")
        
        len_photo = str(data["selectionaffaire"]).count("NumPhoto")
        image = f"https://www.selection-immo.com/Photos/agt60/Mini-Photos/{external_id}-1.jpg"
        item_loader.add_value("images", image)
        for i in range (1,len_photo):
            if data["selectionaffaire"][f"NumPhoto{i}"] !='0':
                image = f"https://www.selection-immo.com/Photos/agt60/Mini-Photos/{external_id}-{i}.jpg"
                item_loader.add_value("images", image)

        yield item_loader.load_item()