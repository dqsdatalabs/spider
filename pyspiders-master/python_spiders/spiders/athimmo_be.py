# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import itemadapter
from scrapy.loader.processors import TakeFirst, MapCompose
from scrapy.spiders import SitemapSpider
from w3lib.html import remove_tags
from python_spiders.items import ListingItem
from scrapy.loader.processors import MapCompose
from scrapy import Spider 
from scrapy.linkextractors import LinkExtractor
from scrapy import Request
from scrapy.selector import Selector
from python_spiders.loaders import ListingLoader
import json
import re  

class MySpider(SitemapSpider):
    name = "athimmo_be"
    execution_type = 'testing'
    country = 'belgium'
    locale = 'fr'
    external_source='Athimmo_PySpider_belgium'
    def start_requests(self):
        url = "https://athimmo.be/page-data/fr/a-louer/page-data.json"
        yield Request(url, self.parse)
    
    def parse(self, response): 
        data = json.loads(response.body)
        json_data = data["result"]["pageContext"]["data"]["contentRow"][0]["data"]["propertiesList"]
        for item in json_data:
            if item["language"] == "fr":
                type = item["TypeDescription"].lower()
                city = item["City"].replace(" ","-").replace("-","").replace("Ê","E").replace("É","E").lower()
                id = item["ID"]
                if 11155==id or 11177==id:
                    type=type.lower().strip().replace(" - ","--").replace("à","a").replace("é","e").replace("è","e").replace(" ","-").replace("Ê","E").replace("!","").replace("+","").replace(",","").replace(".","").replace("ô","o").replace("/mois","mois").split("our-la-consommation-d")[0].replace("-ville","ville").replace("(","").replace(")","").replace("-/--","--").replace("-->-","--").split("-le-pre")[0].replace("rez-de-chaussee","rezdechaussee").replace("--15-€-p","--15-€-p")
                elif 1689==id:
                    type=type.lower().strip().replace(" - ","--").replace("à","a").replace("é","e").replace("è","e").replace(" ","-").replace("Ê","E").replace("!","").replace("+","").replace(",","").replace(".","").replace("ô","o").replace("/mois","mois").split("our-la-consommation-d")[0].replace("-ville","ville").replace("(","").replace(")","").replace("-/--","--").replace("-->-","--").split("-le-pre")[0].replace("rez-de-chaussee","rezdechaussee").replace("--15-€-p","--15-€-p").split("on-eau")[0]
                else:
                    type=type.lower().strip().replace(" - ","--").replace("à","a").replace("é","e").replace("è","e").replace(" ","-").replace("Ê","E").replace("!","").replace("+","").replace(",","").replace(".","").replace("ô","o").replace("/mois","mois").split("our-la-consommation-d")[0].replace("-ville","ville").replace("(","").replace(")","").replace("-/--","--").replace("-->-","--").split("-le-pre")[0].replace("rez-de-chaussee","rezdechaussee").replace("--15-€-p","--15-€-po").split("auffage-et-eau-chaude")[0].split("retien-de-la-chaudiere")[0].replace("poro","pro")
                ext_url = f"/fr/a-louer/{city}/{type}/{id}/"
                ext_url=ext_url.replace("//","/")
                url = f"https://athimmo.be/page-data{ext_url}page-data.json"
                yield Request(url, callback=self.populate_item,meta = {"link":ext_url})

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source",self.external_source)
        item_loader.add_value("property_type",response.meta.get('property_type'))
        link=str(response.meta.get('link'))
        url = f"https://athimmo.be{link}"
        item_loader.add_value("external_link", url)

        value = json.loads(response.body)["result"]["pageContext"]["metadata"]["nl"]
        item_loader.add_value("title", str(value["pageTitle"]))
        item_loader.add_value("description", str(value["metadataDescription"]))
        if "schema" in value: 
            schema = json.loads(response.body)["result"]["pageContext"]["metadata"]["nl"]["schema"]
            for s in schema:
                if "numberOfRooms" in s:
                    item_loader.add_value("room_count", s["numberOfBedrooms"])
                    item_loader.add_value("square_meters", s["floorSize"])
                    item_loader.add_value("property_type", s["@type"].lower())
                if "priceSpecification" in s:
                    item_loader.add_value("rent", s["priceSpecification"]["price"])
                if "location" in s:
                    item_loader.add_value("zipcode", s["location"]["postalCode"])
                    item_loader.add_value("city", s["location"]["addressLocality"])
                    item_loader.add_value("address", "{} {}".format(s["location"]["postalCode"],s["location"]["addressLocality"]))
        item_loader.add_value("currency", "EUR")
        value_property = json.loads(response.body)["result"]["pageContext"]["data"]["contentRow"] 
        for item in value_property: 
            if "property" in item: 
                item_loader.add_value("longitude", item["property"]["GoogleY"])
                item_loader.add_value("latitude", item["property"]["GoogleX"])
                item_loader.add_value("images", item["property"]["LargePictures"])
                item_loader.add_value("landlord_phone", item["property"]["ManagerDirectPhone"])
                item_loader.add_value("landlord_email",  item["property"]["ManagerEmail"])
                item_loader.add_value("landlord_name",  item["property"]["ManagerName"])
                garage=item["property"]["NumberOfGarages"]
                if garage:
                    item_loader.add_value("parking",True)
                bathroom=item["property"]["NumberOfBathRooms"]
                if bathroom:
                    item_loader.add_value("bathroom_count",bathroom)
                bathroomcheck=item_loader.get_output_value("bathroom_count")
                if not bathroomcheck:
                    item_loader.add_value("bathroom_count",item["property"]["NumberOfShowerRooms"])
                date=item["property"]["DateFree"]
                if date:
                    date=str(date)
                    item_loader.add_value("available_date",date.split(" ")[0])

        url=f"https://athimmo.be/page-data/{link}/page-data.json"
        if url:
            yield Request(url, callback=self.other,meta = {"item_loader":item_loader})

    def other(self,response):
        item_loader=response.meta.get('item_loader')
        data=json.loads(response.body)["result"]["pageContext"]["data"]["contentRow"][0]["property"]
        if data['SurfaceTotal']:
            item_loader.add_value("square_meters",data['SurfaceTotal'])
        try:
            energy_label=data['EnergyPerformance']
            if energy_label:
                    energy = energy_label
                    if energy:
                        item_loader.add_value("energy_label",energy_label_calculate(energy))
        except:
            pass
        elevator=data['HasLift']
        if elevator and elevator=="true":
            item_loader.add_value("elevator",True)


        yield item_loader.load_item()

def energy_label_calculate(energy_number):
    energy_number = int(energy_number)
    energy_label = ""
    if energy_number <= 50:
        energy_label = "A"
    elif energy_number > 50 and energy_number <= 90:
        energy_label = "B"
    elif energy_number > 90 and energy_number <= 150:
        energy_label = "C"
    elif energy_number > 150 and energy_number <= 230:
        energy_label = "D"
    elif energy_number > 230 and energy_number <= 330:
        energy_label = "E"
    elif energy_number > 330 and energy_number <= 450:
        energy_label = "F"
    elif energy_number > 450:
        energy_label = "G"
    return energy_label