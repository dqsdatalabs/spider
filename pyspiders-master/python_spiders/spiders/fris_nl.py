# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request
from python_spiders.loaders import ListingItem
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser
 

class MySpider(Spider):
    name = "fris_nl"
    start_urls = [
        "https://fris.nl/volledige-woningaanbod"
    ] 
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'
    # LEVEL 1


    # 1. FOLLOWING LEVEL 1
    def parse(self, response):
        
        follow_url = "https://fris.nl/real-estate-api/search?geobox=52.259594,4.813551,52.485637,4.996021&zoekterm=&koopprijs=0,0&huurprijs=0,0&kamers=0&slaapkamers=0&oppervlakteperceel=0,0&oppervlakte=0,0&vloeroppervlakte=0,0&amsterdamenzaandam=true&overig=false&verkocht=false&verkochtov=true&huur=true&koop=false&rustig=false&levendig=false&groen=false&water=false&sportvereniging=false&winkels=false&restaurants=false&cafes=false&theater=false&bioscoop=false&speeltuin=false&basisschool=false&middelbareschool=false&kantoorruimte=false&horeca=false&overige=false&bedrijfsruimte=false&winkelruimte=false&bouwgrond=false&bedrijf=undefined&searchreason=text_search"
        yield response.follow(follow_url, self.parse_listing)

    def parse_listing(self, response):
        
        for item in json.loads(response.body):
 
            follow_url = f"https://fris.nl/real-estate-api/fetch?id={item}"
            yield response.follow(follow_url, self.populate_item)
        
    # 2. SCRAPING LEVEL 2
    def populate_item(self, response):
        
        jresp = json.loads(response.body)
        if len(jresp) == 0:
            return
        item = jresp[0]

        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", "Fris_PySpider_" + self.country + "_" + self.locale)

        if str(item.get("woonoppervlakte")) != "0" and str(item.get("aantalkamers")) != "0":
            item_loader.add_value("title", item.get("objectnaamlang"))
            r=item.get('objectnaamlang').lower().replace(" ","-")
            item_loader.add_value("external_link", f"https://fris.nl/detail/{item.get('sourceobjectid')}/{r}")
            item_loader.add_value("external_id", item.get("sourceobjectid"))

            property_type = item.get("realestatetype")
            if property_type and "woning" in property_type:
                item_loader.add_value("property_type", "house")
            elif property_type and "appartement" in property_type:
                item_loader.add_value("property_type", "apartment")
            else:
                return

            item_loader.add_value("square_meters",str(item.get("woonoppervlakte")))
            item_loader.add_value("rent",str(item.get("huurprijs")))
            item_loader.add_value("currency","EUR")
            item_loader.add_value("floor",str(item.get("aantalverdiepingen")))
            item_loader.add_value("address",item.get("adres"))
            item_loader.add_value("zipcode",item.get("postcode"))
            item_loader.add_value("city",item.get("woonplaats"))
            item_loader.add_value("room_count",str(item.get("aantalslaapkamers")))
            item_loader.add_value("description",item.get("beschrijving"))
            item_loader.add_value("latitude",str(item.get("geolocatie").get("lat")))
            item_loader.add_value("longitude",str(item.get("geolocatie").get("lon")))
            if "Servicekosten" in item["rawdata"]["Prijzen"][0].keys():
                item_loader.add_value("utilities",((item["rawdata"]["Prijzen"][0]["Servicekosten"])))

            available_date = str(item.get("rawdata").get("InvoerDatum"))
            if available_date:
                date_parsed = dateparser.parse(available_date, date_formats=["%d %B %Y"])
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)

            image = []
            images = item.get("afbeeldingen")
            for i in range(0,len(images)):
                a = images[i]["url"]
                image.append(a)

            item_loader.add_value("images", image)

            parking  = item.get("parkeermogelijkheden")
            if parking is not None:
                item_loader.add_value("parking",True)

            dishwasher = item.get("beschrijving")
            if "dishwasher" in dishwasher:
                item_loader.add_value("dishwasher",True)

            elevator = item.get("beschrijving")
            if "elevator" in elevator or "elevators" in elevator or "lift" in elevator or "lifts" in elevator:
                item_loader.add_value("elevator",True)

            item_loader.add_value("landlord_phone", "020-3017730")
            item_loader.add_value("landlord_email", "vastgoedmanagement@fris.nl")
            item_loader.add_value("landlord_name", "Fris")
            
            yield item_loader.load_item()

