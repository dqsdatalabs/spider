# -*- coding: utf-8 -*-
# Author: Ahmed Hegab
import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import json
import requests

class firstclasshouse_it_PySpider_italySpider(scrapy.Spider):
    name = "firstclasshouse_it"
    start_urls = ['https://www.firstclasshouse.it/r-immobili/?Codice=&Motivazione%5B%5D=2&localita=&Regione%5B%5D=0&Provincia%5B%5D=0&Comune%5B%5D=0&Tipologia%5B%5D=1&Tipologia%5B%5D=36&Tipologia%5B%5D=43&Prezzo_a=&Locali_da=&Camere_da=&Bagni_da=&Totale_mq_da=&Totale_mq_a=&cf=yes&map_circle=0&map_polygon=0&map_zoom=0']
    allowed_domains = ["firstclasshouse.it"]
    country = 'Italy' # Fill in the Country's name
    locale = 'it' # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing' 

    position = 1

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)


    def parse(self, response, **kwargs):
        urls = response.css("#elencoImmo > ul > li > div > figure > a::attr(href)").extract()
        ids = response.css(".codice::text").extract()
        square_meterss = response.css("#elencoImmo > ul > li > div > a > div.boxdettagli > div.icone > div:nth-child(1) > span::text").extract()
        property_types = response.css("#elencoImmo > ul > li > div > a > div.boxdettagli > div.info > div.titolo::text").extract()
        cityy = response.css("#elencoImmo > ul > li > div > a > div.boxdettagli > div.info > div.dove::text").extract()
        for i in range(len(ids)):
            if cityy[i] == " " or cityy[i] == '\r\n                                                            ':
                cityy.pop(i)
        for i in range(len(ids)):
            if cityy[i] == " " or cityy[i] == '\r\n                                                            ':
                cityy.pop(i)
        for i in range(len(ids)):
            if cityy[i] == " " or cityy[i] == '\r\n                                                            ':
                cityy.pop(i)        
        for i in range(len(ids)):
            if cityy[i] == " " or cityy[i] == '\r\n                                                            ':
                cityy.pop(i)

        
        for i in range(len(urls)):
            square_meters = int(square_meterss[i].split(' ')[0])
            extrenal_id = ids[i].split('. ')[1]
            if 'Appartamento' in property_types[i]:
                property_type = 'apartment'
            else:
                property_type = 'house'
            city = cityy[i].strip()

            yield Request(url=urls[i],
            callback=self.populate_item,
            meta={
                'property_type':property_type,
                'city':city,
                'external_id':extrenal_id,
                'square_meters':square_meters,
            })
            
            

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        square_meters = response.meta.get('square_meters')
        property_type = response.meta.get('property_type')
        city = response.meta.get('city')
        external_id = response.meta.get('external_id')
        title = response.css('body > section > div.realestate-scheda > div.interno-scheda > h1::text').get()
        description = ''
        descriptions = response.css("body > section > div.realestate-scheda > div.interno-scheda > div.corposx > div > div:nth-child(2) > div.testo.woww.bounceInUp > p *::text").extract()
        for i in range(len(descriptions)):
            description = description + ' ' + descriptions[i]
        rent = response.css("body > section > div.realestate-scheda > div.interno-scheda > div.corposx > div > div:nth-child(2) > div.prezzo.woww.bounceInRight *::text").get()
        if '-' in rent:
            rent = response.css("body > section > div.realestate-scheda > div.interno-scheda > div.corposx > div > div:nth-child(2) > div.prezzo.woww.bounceInRight > small::text").get()
        if '.' in rent:
            rent = rent.replace('.','')
        rent = int(rent.split('€ ')[1])

        room_info = response.css("body > section > div.realestate-scheda > div.interno-scheda > div.corposx > div > div:nth-child(6) > div > div.schedaMobile.woww.bounceInLeft > div > strong::text").extract()
        room_count = 0
        bathroom_count = None
        for i in range(len(room_info)):
            if 'Camere: ' in room_info[i]:
                room_count = int(room_info[i].split('Camere: ')[1])
            if 'Bagni: ' in room_info[i]:
                bathroom_count = int(room_info[i].split('Bagni: ')[1])
        if room_count == 0:
            room_count = 1

        images = response.xpath("//meta[@itemprop='image']/@content").extract()

        latlng = response.css("body > section > div:nth-child(7) > div:nth-child(2)").get()

        latitude = latlng.split('"latitude" content="')[1].split('"')[0]
        longitude = latlng.split('"longitude" content="')[1].split('"')[0]
        responseGeocode = requests.get(f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
        responseGeocodeData = responseGeocode.json()
        zipcode = responseGeocodeData['address']['Postal']
        address = responseGeocodeData['address']['Match_addr']
        dishwasher = None
        if 'lavastoviglie' in description:
            dishwasher = True
        washing_machine = None
        if 'lavasciuga' in description:
            washing_machine = True
        furnished = None
        if 'arredamento' in description:
            furnished = True

        item_loader.add_value("external_link", response.url) # String
        item_loader.add_value("external_source", self.external_source) # String

        item_loader.add_value("external_id", external_id) # String
        item_loader.add_value("position", self.position) # Int
        item_loader.add_value("title", title) # String
        item_loader.add_value("description", description) # String

        # # Property Details
        item_loader.add_value("city", city) # String
        item_loader.add_value("zipcode", zipcode) # String
        item_loader.add_value("address", address) # String
        item_loader.add_value("latitude", latitude) # String
        item_loader.add_value("longitude", longitude) # String
        #item_loader.add_value("floor", floor) # String
        item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters) # Int
        item_loader.add_value("room_count", room_count) # Int
        item_loader.add_value("bathroom_count", bathroom_count) # Int

        #item_loader.add_value("available_date", available_date) # String => date_format

        #item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        item_loader.add_value("furnished", furnished) # Boolean
        #item_loader.add_value("parking", parking) # Boolean
        #item_loader.add_value("elevator", elevator) # Boolean
        #item_loader.add_value("balcony", balcony) # Boolean
        #item_loader.add_value("terrace", terrace) # Boolean
        #item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        item_loader.add_value("washing_machine", washing_machine) # Boolean
        item_loader.add_value("dishwasher", dishwasher) # Boolean

        # # Images
        item_loader.add_value("images", images) # Array
        item_loader.add_value("external_images_count", len(images)) # Int
        #item_loader.add_value("floor_plan_images", floor_plan_images) # Array

        # # Monetary Status
        item_loader.add_value("rent", rent) # Int
        #item_loader.add_value("deposit", deposit) # Int
        #item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        #item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", "EUR") # String

        #item_loader.add_value("water_cost", water_cost) # Int
        #item_loader.add_value("heating_cost", heating_cost) # Int

        #item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", 'FIRST CLASS HOUSE') # String
        item_loader.add_value("landlord_phone", '0577.111144') # String
        item_loader.add_value("landlord_email", 'info@firstclasshouse.it') # String

        self.position += 1
        yield item_loader.load_item()
