# -*- coding: utf-8 -*-
# Author: Ahmed Hegab
import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import json
from ..helper import *

class traenkner_immobilien_de_PySpider_germanySpider(scrapy.Spider):
    name = "traenkner_immobilien_de"
    start_urls = ['https://traenkner-immobilien.de/immobilien-bremerhaven/mietobjekte.php']
    allowed_domains = ["traenkner-immobilien.de"]
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
        urls = response.css('#inhalt > div.results > table > tbody > tr > td.img > div > div > a::attr(href)').extract()
        external_ids = response.css('#inhalt > div.results > table > thead > tr > th > div > p.idtitle::text').extract()
        for i in range(len(urls)):
            urls[i] = urls[i].replace('.','')
            urls[i] = 'https://traenkner-immobilien.de/immobilien-bremerhaven' + urls[i]
            external_id = external_ids[i]
            yield Request(url = urls[i],
            callback=self.populate_item,
            meta={
                'external_id':external_id
            }
            )
        
    
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        rent = response.css('.preise_column::text').get()
        rent = rent.replace('€','')
        if '.' in rent:
            rent = int(rent.replace('.','').split(',')[0])
        else:
            rent = int(rent.split(',')[0])
        if rent > 0:
            external_id = response.meta.get('external_id').split('ID: ')[1]
            square_meters = response.css('.info tr:nth-child(3) td+ td::text').get()
            square_meters = int(square_meters.split(' ')[0])
            description = response.css('tr:nth-child(1) .label+ td::text').get()
            title = response.css('#inhalt > div.detailbox > h1::text').get()
            room_count = response.css('.info tr:nth-child(4) td+ td::text').get()
            if ',5' in room_count:
                room_count = int(room_count.split(',')[0]) + 1
            else:
                room_count = int(room_count.split(',')[0])

            images = response.css('#inhalt > div.detailbox > div > div.gallery > a::attr(href)').extract()
            for i in range(len(images)):
                images[i] = 'https://traenkner-immobilien.de/immobilien-bremerhaven/' + images[i]

            balcony = response.css('.info tr:nth-child(5) td+ td::text').get()
            elevator = response.css('.info tr:nth-child(7) td+ td::text').get()
            
            if 'Ja' in balcony:
                balcony = True
            else:
                balcony = False
            if 'Ja' in elevator:
                elevator = True
            else:
                elevator = False
            parking = None
            try:
                x = int(response.css('.info tr:nth-child(9) td+ td::text').get())
                y = int(response.css('.info tr:nth-child(10) td+ td::text').get())
                if x+y > 0:
                    parking = True
                else:
                    parking = False
            except:
                pass
            
            floor = response.css('#inhalt > div.detailbox > div > div.info > table > tbody > tr:nth-child(6) > td:nth-child(2)::text').get()

            utilities = response.css('tr:nth-child(3) .label+ td::text').get()
            utilities = int(utilities.replace('€','').split(',')[0])

            list = response.css('.info td ::text').extract()
            list = ' '.join(list)
            list = remove_white_spaces(list)

            location = None
            if 'Lage: ' in list:
                location = list.split('Lage: ')[1].split(' Adresse:')[0]
            address = None
            if 'Adresse: ' in list:
                address = list.split('Adresse: ')[1].split(' Woh')[0]

            address = location + address
            latitude, longitude, zipcode = None
            if address != 'Johannessen':
                longitude, latitude = extract_location_from_address(address)
                zipcode, city, bobo = extract_location_from_coordinates(longitude, latitude)
                longitude = str(longitude)
                latitude = str(latitude)
            else:
                city = 'Sttutgart'
            contact = response.css('.ansprechpartner p ::text').extract()
            contact = ' '.join(contact)
            contact = remove_white_spaces(contact)
            landlord_name = None
            if 'Ansprechpartner: ' in contact:
                landlord_name = contact.split('Ansprechpartner: ')[1].split(' Tel.:')[0]
            landlord_phone = None
            if 'Tel.: ' in contact:
                landlord_phone = contact.split('Tel.: ')[1].split(' EMail:')[0]
            landlord_email = None
            if 'EMail: ' in contact:
                landlord_email = contact.split('EMail: ')[1]

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
            item_loader.add_value("floor", floor) # String
            item_loader.add_value("property_type", "apartment") # String => ["apartment", "house", "room", "student_apartment", "studio"]
            item_loader.add_value("square_meters", square_meters) # Int
            item_loader.add_value("room_count", room_count) # Int
            #item_loader.add_value("bathroom_count", bathroom_count) # Int

            #item_loader.add_value("available_date", available_date) # String => date_format

            #item_loader.add_value("pets_allowed", pets_allowed) # Boolean
            #item_loader.add_value("furnished", furnished) # Boolean
            item_loader.add_value("parking", parking) # Boolean
            item_loader.add_value("elevator", elevator) # Boolean
            item_loader.add_value("balcony", balcony) # Boolean
            #item_loader.add_value("terrace", terrace) # Boolean
            #item_loader.add_value("swimming_pool", swimming_pool) # Boolean
            #item_loader.add_value("washing_machine", washing_machine) # Boolean
            #item_loader.add_value("dishwasher", dishwasher) # Boolean

            # # Images
            item_loader.add_value("images", images) # Array
            item_loader.add_value("external_images_count", len(images)) # Int
            #item_loader.add_value("floor_plan_images", floor_plan_images) # Array

            # # Monetary Status
            item_loader.add_value("rent", rent) # Int
            #item_loader.add_value("deposit", deposit) # Int
            #item_loader.add_value("prepaid_rent", prepaid_rent) # Int
            item_loader.add_value("utilities", utilities) # Int
            item_loader.add_value("currency", "EUR") # String

            #item_loader.add_value("water_cost", water_cost) # Int
            #item_loader.add_value("heating_cost", heating_cost) # Int

            #item_loader.add_value("energy_label", energy_label) # String

            # # LandLord Details
            item_loader.add_value("landlord_name", landlord_name) # String
            item_loader.add_value("landlord_phone", landlord_phone) # String
            item_loader.add_value("landlord_email", landlord_email) # String

            self.position += 1
            yield item_loader.load_item()
