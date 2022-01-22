# -*- coding: utf-8 -*-
# Author: Muhammad Ahmad Hesham
import re
from math import ceil

import scrapy
from scrapy import Request
from dateutil.parser import parse
from datetime import datetime

from ..helper import remove_unicode_char, extract_number_only, extract_location_from_coordinates, description_cleaner
from ..loaders import ListingLoader


class HuberinternationaleimmobilienSpider(scrapy.Spider):
    name = "HuberInternationaleImmobilien"
    start_urls = ['https://smartsite2.myonoffice.de/kunden/huberinternationaleimmobilien/49/neue-seite-55.xhtml']
    allowed_domains = []
    country = 'germany' # Fill in the Country's name
    locale = 'de' # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing' 
    keywords = {
        'pets_allowed': ['Haustiere erlaubt'],
        'furnished': ['m bliert', 'ausstattung'],
        'parking': ['garage', 'Stellplatz' 'Parkh user','Parkplatz'],
        'elevator': ['fahrstuhl', 'aufzug'],
        'balcony': ['balkon'],
        'terrace': ['terrasse'],
        'swimming_pool': ['baden', 'schwimmen', 'schwimmbad', 'pool', 'Freibad'],
        'washing_machine': ['waschen', 'w scherei', 'waschmaschine','waschk che'],
        'dishwasher': ['geschirrspulmaschine', 'geschirrsp ler',],
        'floor' : ['etage'],
        'bedroom': ['Schlafzimmer']
    }
    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    # 2. SCRAPING level 2
    def parse(self, response, **kwargs):
        for url in response.css("div.obj-title a::attr(href)").getall():
            yield scrapy.Request(response.urljoin(url), callback=self.populate_item, dont_filter = True) 

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        list_items = response.css('.data-1col  tr')
        list_items_dict = {}
        for list_item in list_items:
            headers = list_item.css('td:nth-child(1) ::text').extract()
            values = list_item.css('td:nth-child(2) ::text').extract()
            if len(headers) == len(values):
                for n,header in enumerate(headers):
                    list_items_dict[header] = values[n]

        property_type = None
        if 'Objektart' in list_items_dict.keys():
            property_type = list_items_dict['Objektart']
            if 'wohnung' in property_type.lower():
                property_type = 'apartment'
            elif 'familienhaus' in property_type.lower():
                property_type = 'house'
            elif 'souterrain' in property_type.lower():
                property_type = 'apartment'
            elif 'apartment' in property_type.lower():
                property_type = 'apartment'
            elif 'dach­geschoss' in property_type.lower():
                property_type = 'apartment'
            elif 'zimmer' in property_type.lower():
                property_type = 'room'
            else: return
            
            
        external_id = response.css("td:contains('ImmoNr') + td span::text").get()
        
        title = response.css('h1::text').extract_first()
        available_date = None
        possible_date = title.split(' ')
        for date in possible_date:
            if 'sofort' in date.lower():
                available_date = datetime.now().strftime("%Y-%m-%d")
                continue
            else:
                try:
                    available_date = parse(date).strftime("%Y-%m-%d")
                except:
                    pass
        
        heating_cost = None               
        description = ((((' '.join(response.css('.freetext span span::text').extract()).replace('\n','')).replace('\t','')).replace('\r','')))
        latitude = response.css("input.sysLat::attr(value)").get()
        longitude = response.css("input.sysLang::attr(value)").get()
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)

        if property_type is not None:
            square_meters = None  # METERS #int(response.css('::text').extract_first())
            if 'Wohnfläche' in list_items_dict.keys():
                square_meters = list_items_dict['Wohnfläche']
                square_meters = int(ceil(float(extract_number_only(square_meters))))
            room_count = None  # int(response.css('::text').extract_first())
            if 'Anzahl Zimmer' in list_items_dict.keys():
                room_count = list_items_dict['Anzahl Zimmer']
                room_count = int(ceil(float(extract_number_only(room_count))))
                if room_count == 0:
                    room_count = 1
            else:
                room_count =1
            bathroom_count = None
            if 'badezimmer' in description.lower():
                bathroom_count = 1

            description = response.css("div.obj-description span::text").getall()
            description = " ".join(description)
            description = description_cleaner(description)

            images =response.css('.fotorama div::attr(data-img)').extract()
            rent = None  # int(response.css('::text').extract_first())
            if 'Kaltmiete' in list_items_dict.keys():
                rent = list_items_dict['Kaltmiete']
                rent = int(ceil(float(extract_number_only(rent))))
            if rent is not None:


                deposit = None
                if 'Kaution' in list_items_dict.keys():
                    deposit = list_items_dict['Kaution']
                    deposit = int(ceil(float(extract_number_only(deposit))))
                    if len(str(deposit)) == 1:
                        deposit = deposit * rent

                utilities = None
                if 'Nebenkosten' in list_items_dict.keys():
                    utilities = list_items_dict['Nebenkosten']
                    utilities = int(ceil(float(extract_number_only(utilities))))
                energy_label = None
                if 'Energieeffizienzklasse' in list_items_dict.keys():
                    energy_label = list_items_dict['Energieeffizienzklasse']

                if energy_label is None:
                    try:
                        energy_label = response.css("div.obj-description.freetext").extract()[0].split('Klasse:')[1].split('<br>')[0].strip()
                    except:
                        pass
                
                floor = None  # response.css('::text').extract_first()
                if 'Etage' in list_items_dict.keys():
                    floor = list_items_dict['Etage']

                pets_allowed = None
                if any(word in remove_unicode_char(description.lower()) for word in self.keywords['pets_allowed']):
                    pets_allowed = True

                furnished = None
                if any(word in remove_unicode_char(description.lower()) for word in self.keywords['furnished']):
                    furnished = True

                parking = None
                if any(word in remove_unicode_char(description.lower()) for word in self.keywords['parking']):
                    parking = True

                elevator = None
                if any(word in remove_unicode_char(description.lower()) for word in self.keywords['elevator']):
                    elevator = True
                if len([True for x in response.css("td>span::text").getall() if 'aufzug' in x]) > 0:
                    elevator = True

                balcony = None
                if any(word in remove_unicode_char(description.lower()) for word in self.keywords['balcony']):
                    balcony = True

                terrace = None
                if any(word in remove_unicode_char(description.lower()) for word in self.keywords['terrace']):
                    terrace = True

                swimming_pool = None
                if any(word in remove_unicode_char(description.lower()) for word in self.keywords['swimming_pool']):
                    swimming_pool = True

                washing_machine = None
                if any(word in remove_unicode_char(description.lower()) for word in self.keywords['washing_machine']):
                    washing_machine = True

                dishwasher = None
                if any(word in remove_unicode_char(description.lower()) for word in self.keywords['dishwasher']):
                    dishwasher = True



                landlord_name = response.css("div.name strong::text").get()
                landlord_phone = response.css("div.contact-info strong:contains('Telefon:') + span::text").get()
                landlord_email = response.css("strong:contains('E-Mail:') + span a::text").get()
                
                # # MetaData
                item_loader.add_value("external_link", response.url)  # String
                item_loader.add_value("external_source", self.external_source)  # String

                item_loader.add_value("external_id", external_id) # String
                item_loader.add_value("position", self.position)  # Int
                item_loader.add_value("title", title) # String
                item_loader.add_value("description", description) # String

                # # Property Details
                item_loader.add_value("city", city) # String
                item_loader.add_value("zipcode", zipcode) # String
                item_loader.add_value("address", address) # String
                item_loader.add_value("latitude", latitude) # String
                item_loader.add_value("longitude", longitude) # String
                item_loader.add_value("floor", floor) # String
                item_loader.add_value("property_type", property_type) # String
                item_loader.add_value("square_meters", square_meters) # Int
                item_loader.add_value("room_count", room_count) # Int
                item_loader.add_value("bathroom_count", bathroom_count) # Int

                item_loader.add_value("available_date", available_date) # String => date_format also "Available", "Available Now" ARE allowed

                item_loader.add_value("pets_allowed", pets_allowed) # Boolean
                item_loader.add_value("furnished", furnished) # Boolean
                item_loader.add_value("parking", parking) # Boolean
                item_loader.add_value("elevator", elevator) # Boolean
                item_loader.add_value("balcony", balcony) # Boolean
                item_loader.add_value("terrace", terrace) # Boolean
                item_loader.add_value("swimming_pool", swimming_pool) # Boolean
                item_loader.add_value("washing_machine", washing_machine) # Boolean
                item_loader.add_value("dishwasher", dishwasher) # Boolean
                #
                # # Images
                item_loader.add_value("images", images) # Array
                item_loader.add_value("external_images_count", len(images)) # Int
                # item_loader.add_value("floor_plan_images", floor_plan_images) # Array

                # # Monetary Status
                item_loader.add_value("rent", rent) # Int
                item_loader.add_value("deposit", deposit) # Int
                # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
                item_loader.add_value("utilities", utilities) # Int
                item_loader.add_value("currency", "EUR") # String

                # item_loader.add_value("water_cost", water_cost) # Int
                item_loader.add_value("heating_cost", heating_cost) # Int

                item_loader.add_value("energy_label", energy_label) # String

                # # LandLord Details
                item_loader.add_value("landlord_name", landlord_name) # String
                item_loader.add_value("landlord_phone", landlord_phone) # String
                item_loader.add_value("landlord_email", landlord_email) # String

                self.position += 1
                yield item_loader.load_item()
