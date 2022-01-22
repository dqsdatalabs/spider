# -*- coding: utf-8 -*-
# Author: Ahmed Hegab
import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import json
import requests
import math

class arivora_de_PySpider_germanySpider(scrapy.Spider):
    name = "arivora_de"
    start_urls = ['https://arivora.de/informationen-fuer-mieter/wohnung-suchen/?Charlottenburg-Wilmersdorf=Charlottenburg-Wilmersdorf&Friedrichshain-Kreuzberg=Friedrichshain-Kreuzberg&Lichtenberg=Lichtenberg&Marzahn-Hellersdorf=Marzahn-Hellersdorf&Mitte=Mitte&Neuk%C3%B6lln=Neuk%C3%B6lln&Pankow=Pankow&Reinickendorf=Reinickendorf&Spandau=Spandau&Steglitz-Zehlendorf=Steglitz-Zehlendorf&Tempelhof-Sch%C3%B6neberg=Tempelhof-Sch%C3%B6neberg&Treptow-K%C3%B6penick=Treptow-K%C3%B6penick&sortierung=']
    allowed_domains = ["arivora.de"]
    country = 'Germany' 
    locale = 'de' 
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing' 

    position = 1

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    def parse(self, response, **kwargs):
        urls = response.css("#immobilie > div > div.es-property-info > div.es-row-view > h2 > a::attr(href)").extract()
        for i in range(len(urls)):
            yield Request(url=urls[i],
            callback=self.populate_item)

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        title = response.css('.entry-title::text').get()
        furnished = None
        if 'Möblierte' in title:
            furnished = True
        
        address = response.css(".objektanschrift::text").get()

        all_images = response.css("img").extract()
        images = []
        for i in range(len(all_images)):
            if 'logo' not in all_images[i] and '300x200' not in all_images[i]:
                images.append(all_images[i])
        for i in range(len(images)):
            images[i] = images[i].split('src="')[1].split('"')[0]

        rent = int(response.css("#es-info > div.es-price-details-single > ul > li > span.es-price-details-single-value > span::text").get().split(' ')[0])
        strong = response.css("#es-info > div.es-property-fields > ul > li > strong::text").extract()
        value = response.css("#es-info > div.es-property-fields > ul > li::text").extract()
        for i in range(len(strong)):
            if value[i] == " ":
                value.pop(i)
        for i in range(len(strong)):
            if value[i] == " ":
                value.pop(i)
        for i in range(len(strong)):
            if value[i] == " ":
                value.pop(i)
        for i in range(len(strong)):
            if value[i] == " ":
                value.pop(i)
        for i in range(len(strong)):
            if value[i] == " ":
                value.pop(i)
        room_count = None
        bathroom_count = None
        square_meters = None
        floor = None
        available_date = None
        furnished = None
        pets_allowed = None
        deposit = None
        external_id = None
        for i in range(len(value)):
            if strong[i] == 'Wohnfläche:':
                square_meters = value[i]
            if strong[i] == 'Zimmer:':
                room_count = int(value[i])
            if strong[i] == 'Badezimmer:':
                bathroom_count = int(value[i])
            if strong[i] == 'Etage:':
               floor = value[i]
            if strong[i] == 'Verfügbar ab:':
                available_date = value[i]
            if strong[i] == 'Möblierung:':
                furnished = value[i]
                if "Ja" in furnished:
                    furnished = True 
            if strong[i] == 'Haustiere erlaubt:':
                pets_allowed = value[i]
                if "Nein" in pets_allowed:
                    pets_allowed = False
            if strong[i] == 'Kaution:':
               deposit = int(value[i])
            if strong[i] == 'Objekt-Nr.:':
                external_id = value[i]
        try:
            square_meters = int(math.ceil(float(square_meters.split('m²')[0])))
        except:
            pass
        description = response.css("#es-description > p::text").get()
        extra_info = response.css("#sonstiges1581701611f5e46d9eb2e7a8 > ul > li > p *::text").extract()
        info = ''
        for i in range(len(extra_info)):
            info = info + " " + extra_info[i]
        pets_allowed = None
        if 'Haustiere: Nicht erlaubt' in info:
            pets_allowed = False

        energy_label = response.css(".es-property-single-fields li:nth-child(3)").get().split(' ')[1]
        terrace = None
        if 'Dachterrasse' in title or 'Dachterrasse' in description:
            terrace = True
        
        latlng = response.css("#es-map > div").get()
        latitude = latlng.split('data-lat="')[1].split('"')[0]
        longitude = latlng.split('data-lng="')[1].split('"')[0]
        responseGeocode = requests.get(f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
        responseGeocodeData = responseGeocode.json()
        zipcode = responseGeocodeData['address']['Postal']
        city = responseGeocodeData['address']['City']
        elevator = None
        if 'Aufzug' in description:
            elevator = True
        washing_machine = None
        if 'Waschmaschinen' in description:
            washing_machine = True

        if room_count is None:
            room_count = 1
        
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source) 

        item_loader.add_value("external_id", external_id) 
        item_loader.add_value("position", self.position)
        item_loader.add_value("title", title)
        item_loader.add_value("description", description) 

        item_loader.add_value("city", city) 
        item_loader.add_value("zipcode", zipcode) 
        item_loader.add_value("address", address) 
        item_loader.add_value("latitude", latitude) 
        item_loader.add_value("longitude", longitude) 
        item_loader.add_value("floor", floor)
        item_loader.add_value("property_type", "apartment")
        item_loader.add_value("square_meters", square_meters) 
        item_loader.add_value("room_count", room_count) 
        item_loader.add_value("bathroom_count", bathroom_count) 

        item_loader.add_value("available_date", available_date) 

        item_loader.add_value("elevator", elevator)
        item_loader.add_value("washing_machine", washing_machine)

        item_loader.add_value("pets_allowed", pets_allowed)
        item_loader.add_value("furnished", furnished) 
        item_loader.add_value("terrace", terrace)
        
        item_loader.add_value("images", images) # Array
        item_loader.add_value("external_images_count", len(images))

        item_loader.add_value("rent", rent) 
        item_loader.add_value("deposit", deposit) 
        item_loader.add_value("currency", "EUR") 


        item_loader.add_value("energy_label", energy_label) 

        item_loader.add_value("landlord_name", "ARIVORA") 
        item_loader.add_value("landlord_phone", "+49 30 347421625")
        item_loader.add_value("landlord_email", "info@arivora.de") 

        self.position += 1
        yield item_loader.load_item()
