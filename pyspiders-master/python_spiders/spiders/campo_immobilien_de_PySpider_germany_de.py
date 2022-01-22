# -*- coding: utf-8 -*-
# Author: Ahmed Hegab
import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import json
from ..helper import extract_location_from_address, extract_location_from_coordinates
from ..helper import *

class campo_immobilien_de_PySpider_germanySpider(scrapy.Spider):
    name = "campo_immobilien_de"
    start_urls = ['https://www.campo-immobilien.de/Immobilien']
    allowed_domains = ["campo-immobilien.de"]
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
        urls = response.css('#content a::attr(href)').extract()
        for i in range(len(urls)):
            urls[i] = 'https://www.campo-immobilien.de/' + urls[i]
            if 'https://www.campo-immobilien.de/Immobilien-Verkauf' in urls[i]:
                urls[i] = None
            included = response.css('#content > div > div > div.property-listing > ul > li:nth-child('+str(i+1)+') > div.col-md-8 > div.property-info > div > span::text').get()
            if 'Kaltmiete' in included:
                rent = included.split(' ')[0]
                if ',' in rent:
                    rent = rent.split(',')[0]
                rent = int(rent)
                square_meters = response.css('#content > div > div > div.property-listing > ul > li:nth-child('+str(i+1)+') > div.col-md-8 > div.property-amenities.clearfix > span.area > strong::text').get()
                if ',' in square_meters:
                    square_meters = square_meters.split(',')[0]
                square_meters = int(square_meters)
                room_count = response.css('#content > div > div > div.property-listing > ul > li:nth-child('+str(i+1)+') > div.col-md-8 > div.property-amenities.clearfix > span.baths > strong::text').get()
                if ',' in room_count:
                    room_count = int(room_count.split(',')[0])+1
                else:
                    room_count = int(room_count)
                title = response.css('#content > div > div > div.property-listing > ul > li:nth-child('+str(i+1)+') > div.col-md-8 > div.property-info > h3 > a::text').get()
                if 'reserviert' not in title.lower() and 'vermietet' not in title.lower(): #'ladengeschäft' not in title.lower() and 'bürofläche' not in title.lower():
                    yield Request(url= urls[i],
                    callback= self.populate_item,
                    meta=
                    {
                        'rent':rent,
                        'square_meters':square_meters,
                        'room_count':room_count,
                        'title':title
                    }
                    )
       
       
    
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        rent = response.meta.get('rent')
        room_count = response.meta.get('room_count')
        square_meters = response.meta.get('square_meters')
        title = response.meta.get('title')
        address = response.css('#cphContent_lblAdresse::text').get()
        description = response.css('#cphContent_lblObjektbeschreibung::text').get()

        longitude, latitude = extract_location_from_address(address)       
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)

        images = response.css('#property-thumbs > ul > li > img::attr(src)').extract()
        for i in range(len(images)):
            images[i] = 'https://www.campo-immobilien.de' + images[i]
            images[i] = images[i].replace('..','')

        property_type = 'apartment'
        try:
            property_type = response.css('#cphContent_lblWohnungstyp::text').get()
        except:
            pass
        try:
            if 'Haushälfte' in property_type or 'Architektenhaus freistehend' in property_type:
                property_type = 'house'
            elif 'Büro' in property_type:
                property_type = 'office'
            else:
                property_type = 'apartment'
        except:
            pass
        if property_type == 'apartment' or property_type =='house':
            bathroom_count = None
            try:
                bathroom_count = int(response.css('#cphContent_lblBadezimmer::text').get())
            except:
                pass
            parking = 0
            try:
                parking = int(response.css('#cphContent_lblAnzahlGarage::text').get())
            except:
                pass
            if parking > 0:
                parking = True
            else:
                parking = False

            balcony = None
            terrace = None
            balcony_terrace = response.css('#cphContent_trBalkonTerrasse > td:nth-child(1)::text').get()
            if balcony_terrace is not None:
                balcony = True
                terrace = True
            
            pets_allowed = None
            try:
                pets_allowed = response.css('#cphContent_lblHaustiere::text').get()
            except:
                pass
            try:
                if 'nach Vereinbarung' in pets_allowed:
                    pets_allowed = True
                else:
                    pets_allowed = False
            except:
                pass

            elevator = None
            try:
                elevator = respons.css('#cphContent_trPersonenaufzug > td:nth-child(1)::text').get()
            except:
                pass
            if elevator is not None:
                elevator = True
            landlord_email = response.css('#cphContent_ctl00 > div:nth-child(1) > div.featured-block.accordion-group > a > strong::text').get()
            landlord_name = response.css('#cphContent_ucText6_Label1::text').get()

            list = response.css('td ::text').extract()
            list = ' '.join(list)
            list = remove_white_spaces(list)
            print(list)
            heating_cost = None
            if 'Heizkosten: ' in list:
                heating_cost = list.split('Heizkosten: ')[1].split(',')[0]
                heating_cost = int(''.join(x for x in heating_cost if x.isdigit()))
            utilities = None
            if 'Nebenkosten: ' in list:
                utilities = list.split('Nebenkosten: ')[1].split(',')[0]
                utilities = int(''.join(x for x in utilities if x.isdigit()))
            elif 'Heizkosten sind in Nebenkosten enthalten:' in list:
                if 'Gesamtmiete: ' in list:
                    total_rent = list.split('Gesamtmiete: ')[1].split(',')[0]
                    total_rent = int(''.join(x for x in total_rent if x.isdigit()))
                    utilities = total_rent-rent

            deposit = None
            if 'Kaution oder Genossenschaftsanteile: ' in list:
                deposit = int(list.split('Kaution oder Genossenschaftsanteile: ')[1].split(' ')[0])
                deposit = deposit * rent

            parking = None
            if 'stellplatz' in description.lower() or 'stellplatz' in list.lower():
                parking = True
            washing_machine = None
            if 'waschmasch' in description.lower() or 'waschmasch' in list.lower():
                washing_machine = True
            dishwasher = None
            if 'geschirr' in description.lower() or 'geschirr' in list.lower():
                dishwasher = True
            terrace = None
            if 'terras' in description.lower() or 'terras' in list.lower():
                terrace = True
            elevator = None
            if 'aufzug' in description.lower() or 'aufzug' in list.lower():
                elevator = True
            balcony = None
            if 'balkon' in description.lower() or 'balkon' in list.lower():
                balcony = True
            furnished = None
            if 'renoviert' in description.lower() or 'renoviert' in list.lower():
                furnished = True


            item_loader.add_value("external_link", response.url) # String
            item_loader.add_value("external_source", self.external_source) # String

            #item_loader.add_value("external_id", external_id) # String
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

            item_loader.add_value("pets_allowed", pets_allowed) # Boolean
            item_loader.add_value("furnished", furnished) # Boolean
            item_loader.add_value("parking", parking) # Boolean
            item_loader.add_value("elevator", elevator) # Boolean
            item_loader.add_value("balcony", balcony) # Boolean
            item_loader.add_value("terrace", terrace) # Boolean
            #item_loader.add_value("swimming_pool", swimming_pool) # Boolean
            item_loader.add_value("washing_machine", washing_machine) # Boolean
            item_loader.add_value("dishwasher", dishwasher) # Boolean

            # # Images
            item_loader.add_value("images", images) # Array
            item_loader.add_value("external_images_count", len(images)) # Int
            #item_loader.add_value("floor_plan_images", floor_plan_images) # Array

            # # Monetary Status
            item_loader.add_value("rent", rent) # Int
            item_loader.add_value("deposit", deposit) # Int
            #item_loader.add_value("prepaid_rent", prepaid_rent) # Int
            item_loader.add_value("utilities", utilities) # Int
            item_loader.add_value("currency", "EUR") # String

            #item_loader.add_value("water_cost", water_cost) # Int
            item_loader.add_value("heating_cost", heating_cost) # Int

            #item_loader.add_value("energy_label", energy_label) # String

            # # LandLord Details
            item_loader.add_value("landlord_name", landlord_name) # String
            item_loader.add_value("landlord_phone", "0511 954 88 724") # String
            item_loader.add_value("landlord_email", landlord_email) # String

            self.position += 1
            yield item_loader.load_item()
