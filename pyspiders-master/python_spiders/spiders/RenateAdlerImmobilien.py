# -*- coding: utf-8 -*-
# Author: Muhammad Ahmad Hesham
from math import ceil

import scrapy
from scrapy import Request

from ..helper import energy_label_extractor, remove_unicode_char, extract_number_only, extract_location_from_address, extract_location_from_coordinates
from dateutil.parser import parse
from datetime import datetime
from ..loaders import ListingLoader


class RenateadlerimmobilienSpider(scrapy.Spider):
    name = "RenateAdlerImmobilien"
    start_urls = ['https://portal.immobilienscout24.de/ergebnisliste/99615405']
    country = 'germany' # Fill in the Country's name
    locale = 'de' # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'
    keywords = {
        'pets_allowed': ['Haustiere erlaubt'],
        'furnished': ['m bliert', 'ausstattung'],
        'parking': ['garage', 'Stellplatz' 'Parkh user'],
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
        rentals = response.css('.result__list__element__infos--figcaption a::attr(href)').extract()
        for rental in rentals:
            yield Request(url='https://portal.immobilienscout24.de/'+rental,
                          callback=self.populate_item)
        next_page = response.css('a.is24portale-next-rel--link::attr(href)').get()
        if next_page != None:
            yield Request(url='https://portal.immobilienscout24.de/'+next_page, callback=self.parse)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        external_id = response.css('.form+ .expose--text p+ p::text').extract_first()
        external_id = (external_id.split(': '))[-1]
        title = response.css('.is24__block__responsive--col1 h4::text').extract_first()
        description = ' '.join([x for x in response.css('.expose--text:nth-child(8) p::text , .expose--text:nth-child(9) h4+ p::text , .expose--text:nth-child(7) p::text').extract() if 'adler' not in x])
        address = response.css('.expose--text__address p::text').extract_first()
        longitude, latitude = extract_location_from_address(address)
        zipcode, city, _ = extract_location_from_coordinates(longitude, latitude)
        li = response.css('.expose--text li')
        item_list = {}
        for item in li:
            header = item.css('p:nth-child(1)::text').extract_first()
            value = item.css('p:nth-child(2)::text').extract_first()
            item_list[header] = value
            
        property_type = 'apartment'
        if 'Wohnungstyp:' in item_list.keys():
            property_type = item_list['Wohnungstyp:']
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
        
        energy_label = None
        if 'Energieeffizienzklasse:' in item_list.keys():
            energy_label = item_list['Energieeffizienzklasse:']
            
        square_meters = None
        if 'Wohnfläche ca.:' in item_list.keys():
            square_meters = item_list['Wohnfläche ca.:']
            square_meters = int(ceil(float(extract_number_only(square_meters))))

        room_count = None
        if 'Schlafzimmer:' in item_list.keys():
            room_count = item_list['Schlafzimmer:']
            room_count = int(ceil(float(extract_number_only(room_count))))

        bathroom_count = None  # int(response.css('::text').extract_first())
        if 'Badezimmer:' in item_list.keys():
            bathroom_count = item_list['Badezimmer:']
            bathroom_count = int(ceil(float(extract_number_only(bathroom_count))))

        available_date = None  # response.css('.availability .meta-data::text').extract_first()
        if 'Bezugsfrei ab:' in item_list.keys() and item_list['Bezugsfrei ab:'] != 'nach Vereinbarung':
            val = item_list['Bezugsfrei ab:']
            if 'sofort' in val.lower():
                    available_date = datetime.now().strftime("%Y-%m-%d")
            elif 'vermietet' in val.lower():
                return
            else:
                try:
                    available_date = parse(val.split(' ')[0]).strftime("%Y-%m-%d")
                except:
                    available_date = None  

        images = response.css('.sp-slide a::attr(href)').extract()
        images = [ i for i in images if not 'ab7d3eed-394b-4cf5-afac-975fd8945c51-1430833160.jpg' in i]
        images = ['https:'+i for i in images]
        floor_plan_images = response.css('.expose__column__text--image::attr(src)').extract()
        floor_plan_images = [ i for i in floor_plan_images if not 'ab7d3eed-394b-4cf5-afac-975fd8945c51-1430833160.jpg' in i]
        floor_plan_images = ['https:'+i for i in floor_plan_images]

        rent = None  # int(response.css('::text').extract_first())
        if 'Kaltmiete:' in item_list.keys():
            rent = item_list['Kaltmiete:']
            rent = int(ceil(float((rent.split(',')[0]).replace('.',''))))
        if 'RESERVIERT' in title.upper().replace('-', '').replace(' ', '') or 'VERMIETET' in title.upper().replace('-', '').replace(' ', ''):
            return


        deposit = None
        if 'Kaution oder Genossenschaftsanteile:' in item_list.keys():
            deposit = get_price(item_list['Kaution oder Genossenschaftsanteile:'])
            if deposit < 10:
                deposit = deposit * rent

        utilities = None
        if 'Nebenkosten:' in item_list.keys():
            utilities = item_list['Nebenkosten:']
            utilities = int(ceil(float(extract_number_only(utilities.split(',')[0]))))

        heating_cost = None
        if 'Heizkosten:' in item_list.keys():
            heating_cost = item_list['Heizkosten:']
            heating_cost = int(ceil(float(extract_number_only(heating_cost.split(',')[0]))))


        floor = None
        if 'Etage:' in item_list.keys():
            floor = item_list['Etage:']

        pets_allowed = None
        if 'Haustiere:' in item_list.keys():
            pets_allowed = item_list['Haustiere:']
            if pets_allowed.lower() == 'nein':
                pets_allowed = False
            else:
                pets_allowed = True

        furnished = True


        parking = None
        if 'Garage/ Stellplatz - Miete:' in item_list.keys():
            parking = item_list['Garage/ Stellplatz - Miete:']
            if parking:
                parking = True
            else:
                parking = False
        if rent == None:
            return
        if room_count == None:
            room_count = 1

        elevator = None
        if any(word in description.lower() for word in self.keywords['elevator']):
            elevator = True

        balcony = None
        if any(word in description.lower() for word in self.keywords['balcony']):
            balcony = True

        terrace = None
        if any(word in description.lower() for word in self.keywords['terrace']):
            terrace = True

        swimming_pool = None
        if any(word in description.lower() for word in self.keywords['swimming_pool']):
            swimming_pool = True

        washing_machine = None
        if any(word in description.lower() for word in self.keywords['washing_machine']):
            washing_machine = True

        dishwasher = None
        if any(word in description.lower() for word in self.keywords['dishwasher']):
            dishwasher = True
            
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
        item_loader.add_value("latitude", str(latitude)) # String
        item_loader.add_value("longitude", str(longitude)) # String
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

        # # Images
        item_loader.add_value("images", images) # Array
        item_loader.add_value("external_images_count", len(images)) # Int
        item_loader.add_value("floor_plan_images", floor_plan_images) # Array

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
        item_loader.add_value("landlord_name", 'Frau Renate Adler') # String
        item_loader.add_value("landlord_phone", '+49 (2131) - 60 49 62') # String
        item_loader.add_value("landlord_email", 'info@renate-adler-immobilien.info') # String

        self.position += 1
        yield item_loader.load_item()


def get_price(val):
    v = int(float(extract_number_only(val, thousand_separator=',', scale_separator='.')))
    v2 = int(float(extract_number_only(val, thousand_separator='.', scale_separator=',')))
    price = min(v, v2)
    if price < 10:
        price = max(v, v2)
    return price