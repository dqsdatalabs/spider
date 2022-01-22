# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re, requests
from urllib.parse import urlparse, urlunparse, parse_qs

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader

class PanoramaCasaImmobiliareSpider(Spider):
    name = 'Panoramacasaimmobiliare_it'
    country='italy'
    locale='it' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.panoramacasaimmobiliare.it"]
    start_urls = ["https://panoramacasaimmobiliare.it/affitto"]

    def parse(self, response):
        for url in response.css("div.img-overlay a::attr(href)").getall():
            yield Request(response.urljoin(url), callback=self.populate_item, dont_filter = True)
    
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        property_type = response.css("h1.margin-b-05::text").get()
        if( property_type == "Posti auto il locazione"):
            return
        
        if( property_type == "Spazioso locale ad uso laboratorio"):
            return

        if( property_type == "Negozio in locazione"):
            return

        property_type = "apartment"    
        rent = response.css("div.field-name-field-price:nth-child(1) > div:nth-child(1) > div:nth-child(1) > span:nth-child(2)::text").get().strip()
        address = response.css("h4.margin-t-0:nth-child(2)::text").get().strip()
        title = response.css(".margin-b-05::text").get().strip()
        title_lowered = title.lower()
        if (
            ("commerciale" in title_lowered) 
            or ("ufficio" in title_lowered) 
            or ("magazzino" in title_lowered) 
            or ("box" in title_lowered) 
            or ("auto" in title_lowered) 
            or ("negozio" in title_lowered)
            or ("laboratorio" in title_lowered) 
            or ("vendita" in title_lowered) ):
            return

        square_meters = response.css("div.col-xs-4:nth-child(1) > h4:nth-child(3)::text").get().strip()
        room_count = response.css("div.col-xs-4:nth-child(2) > h4:nth-child(3)::text").get().strip()
        elevator = response.css("div.col-xs-4:nth-child(3) > h4:nth-child(3)::text").get().strip()
        if(elevator == "No"):
            elevator = True
        else:
            elevator = False
        
        images = response.css("div.field-formatter-isotope-item a.colorbox img.img-responsive::attr(src)").getall()
        images_to_add = []
        for image in images:
            image = re.sub(r'_sm', "", image)
            images_to_add.append(response.urljoin(image))

        landlord_phone = "011 7717047"
        landlord_name = "panoramacasaimmobiliare"
        landlord_email = "info@panoramacasaimmobiliare.it"

        energy_label = response.css(".ec-label::text").get()
        description = response.css(".field-name-body > div:nth-child(1) > div:nth-child(1) > p:nth-child(1)::text").getall()
        description = " ".join(description)

        bathroom_count = response.css("div.field-label:contains('Bagno:') + div.field-items div.field-item::text").get()
        
        elevator = response.css("div.field-label:contains('Ascensore:') + div.field-items div.field-item::text").get()
        if(elevator == "No"):
            elevator = False
        else:
            elevator = True

        floor = response.css("div.field-label:contains('Piano:') + div.field-items div.field-item::text").get()
        
        furnished = response.css("div.field-label:contains('Arredato:') + div.field-items div.field-item::text").get()
        if(furnished == "No"):
            furnished = False
        else:
            furnished = True

        balcony = response.css("div.field-label:contains('Balconi') + div.field-items div.field-item::text").get()
        if(balcony == "No"):
            balcony = False
        else:
            balcony = True

        parking = response.css("div.field-label:contains('Posto auto:') + div.field-items div.field-item::text").get()
        if(parking == "No"):
            parking = False
        else:
            parking = True

        city = address.split(",")[1]
        external_id = response.css("div.field-label:contains('Riferimento:') + div.field-items div.field-item::text").get()
        map_script = response.css("iframe::attr(src)").get()

        address = parse_qs(map_script)['q'][0]
        responseGeocode = requests.get(f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/find?f=json&text={address}&maxLocations=1")
        responseGeocodeData = responseGeocode.json()

        longitude = responseGeocodeData['locations'][0]['feature']['geometry']['x']
        latitude = responseGeocodeData['locations'][0]['feature']['geometry']['y']

        responseGeocode = requests.get(f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
        responseGeocodeData = responseGeocode.json()
        zipcode = responseGeocodeData['address']['Postal']
        city = responseGeocodeData['address']['City']

        longitude  = str(longitude)
        latitude  = str(latitude)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("rent_string", rent)
        item_loader.add_value("address", address)
        item_loader.add_value("title", title)
        item_loader.add_value("square_meters", square_meters)
        item_loader.add_value("room_count", room_count)
        item_loader.add_value("elevator", elevator)
        item_loader.add_value("images", images_to_add)
        item_loader.add_value("furnished", furnished)
        item_loader.add_value("floor", floor)
        item_loader.add_value("elevator", elevator)
        item_loader.add_value("bathroom_count", bathroom_count)
        item_loader.add_value("description", description)
        item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_email", landlord_email)
        item_loader.add_value("energy_label", energy_label)
        item_loader.add_value("latitude", latitude)
        item_loader.add_value("longitude", longitude)
        item_loader.add_value("city", city)
        item_loader.add_value("zipcode", zipcode)
        item_loader.add_value("parking", parking)
        item_loader.add_value("balcony", balcony)
        item_loader.add_value("external_id", external_id)
       
        yield item_loader.load_item()
