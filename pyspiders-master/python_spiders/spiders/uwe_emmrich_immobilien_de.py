# -*- coding: utf-8 -*-
# Author: Adham Mansour
import re
from math import ceil

import scrapy
from scrapy import Request

from ..helper import remove_unicode_char, extract_number_only, remove_white_spaces, extract_location_from_address, \
    extract_location_from_coordinates
from ..loaders import ListingLoader


class Uwe_emmrich_immobilienDeSpider(scrapy.Spider):
    name = 'uwe_emmrich_immobilien_de'
    allowed_domains = ['uwe-emmrich-immobilien.de']
    start_urls = ['http://uwe-emmrich-immobilien.de/page/angbody.php?KaufMiete=miete&Html_SchalterGrafik=wohn']  # https not http
    country = 'germany'
    locale = 'de'
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
        rentals = response.css('a.objhead::attr(href)').extract()
        rentals = list(dict.fromkeys(rentals))
        titles = response.css('a.objhead::text').extract()
        for n,rental in enumerate(rentals):
            yield Request(url='http://uwe-emmrich-immobilien.de/page/'+rental.replace('exframe','exbody'),
                          callback=self.populate_item,
                          meta={'title' : titles[n]})

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        all_text = remove_white_spaces((((' '.join(response.css('table ::text').extract()).replace('\n','')).replace('\t','')).replace('\r','')))

        external_id = re.findall('Objektnr.: (\d+) ',all_text)
        if external_id:
            external_id = external_id[0]
        else:
            external_id = None

        if 'title' in response.meta.keys():
            title = response.meta['title']

        description = re.findall('Objektbeschreibung: ([\w|\s|\\|\.|\-|,|\(|\)|/]+)',all_text)
        if description:
            description = description[0]
            description = description.replace('. Lage','.')
        else:
            description = None

        address = re.findall('Adresse: (\d+ \w+)',all_text)
        if address:
            address = address[0]
            longitude, latitude = extract_location_from_address(address)
            zipcode, city, address = extract_location_from_coordinates(longitude, latitude)
        else:
            address = None
            latitude = None
            longitude = None
            city = None
            zipcode = None
        # #try to get it the rest of the address attribute from geocode

        property_type = 'apartment'
        trs= response.css('table:nth-child(3) tr')
        trs_dict = {}
        for i in trs:
            if len(i.css('td')) == 2:
                headers = i.css('td:nth-child(1)::text').extract()
                headers = [remove_white_spaces((((j.replace('\n','')).replace('\t','')).replace('\r',''))) for j in headers]
                values = i.css('td:nth-child(2)::text').extract()
                values = [remove_white_spaces((((k.replace('\n', '')).replace('\t', '')).replace('\r', ''))) for k in values]
                for n,header in enumerate(headers):
                    trs_dict[header] = values[n]

        square_meters = None
        if 'Wohnfläche:' in trs_dict.keys():
            square_meters = trs_dict['Wohnfläche:']
            square_meters = int(ceil(float(extract_number_only(square_meters))))

        room_count = None
        if 'Anzahl Zimmer:' in trs_dict.keys():
            room_count = trs_dict['Anzahl Zimmer:']
            room_count = int(ceil(float(extract_number_only(room_count))))

        bathroom_count = None  # int(response.css('::text').extract_first())
        if 'Anzahl Badezimmer:' in trs_dict.keys():
            bathroom_count = trs_dict['Anzahl Badezimmer:']
            bathroom_count = int(ceil(float(extract_number_only(bathroom_count))))

        #if one image is present
        images = response.css('.image img::attr(src)').extract()
        if 'admin/pic' in images[0]:
            images = []
        if len(response.css('.image:nth-child(1) a')):
            images = images + [f'http://uwe-emmrich-immobilien.de/immotrans/images/{external_id}_bild{i}.jpg' for i in range(1,len(response.css('.image:nth-child(1) a')))]
        rent = None  # int(response.css('::text').extract_first())
        if 'Kaltmiete:' in trs_dict.keys():
            rent = trs_dict['Kaltmiete:']
            rent = int(ceil(float(extract_number_only(rent))))

        deposit = None
        if 'Kaution:' in trs_dict.keys():
            deposit = trs_dict['Kaution:']
            if not deposit.isnumeric():
                deposit = 3 * rent
            else:
                deposit = (ceil(float(extract_number_only(deposit))))

        prepaid_rent = None
        utilities = None
        if 'Nebenkosten:' in trs_dict.keys():
            utilities = trs_dict['Nebenkosten:']
            utilities = int(ceil(float(extract_number_only(utilities))))

        floor = None  # response.css('::text').extract_first()
        if 'Etage:' in trs_dict.keys():
            floor = trs_dict['Etage:']

        amenities = remove_white_spaces((((' '.join(response.css('tr:nth-child(10) td::text').extract()).replace('\n','')).replace('\t','')).replace('\r','')))

        if 'Haustiere erlaubt' in amenities:
            pets_allowed = True
        elif 'Haustiere verboten' in amenities:
            pets_allowed = False
        else:
            pets_allowed = None

        parking = None
        if any(word in description.lower() for word in self.keywords['parking']) or any(
                word in amenities.lower() for word in self.keywords['parking']):
            parking = True

        elevator = None
        if any(word in description.lower() for word in self.keywords['elevator']) or any(
                word in amenities.lower() for word in self.keywords['elevator']):
            elevator = True

        balcony = None
        if any(word in description.lower() for word in self.keywords['balcony']) or any(
                word in amenities.lower() for word in self.keywords['balcony']):
            balcony = True

        terrace = None
        if any(word in description.lower() for word in self.keywords['terrace']) or any(
                word in amenities.lower() for word in self.keywords['terrace']):
            terrace = True

        swimming_pool = None
        if any(word in description.lower() for word in self.keywords['swimming_pool']) or any(
                word in amenities.lower() for word in self.keywords['swimming_pool']):
            swimming_pool = True

        washing_machine = None
        if any(word in description.lower() for word in self.keywords['washing_machine']) or any(
                word in amenities.lower() for word in self.keywords['washing_machine']):
            washing_machine = True

        dishwasher = None
        if any(word in description.lower() for word in self.keywords['dishwasher']) or any(
                word in amenities.lower() for word in self.keywords['dishwasher']):
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

        # item_loader.add_value("available_date", available_date) # String => date_format also "Available", "Available Now" ARE allowed

        item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        # item_loader.add_value("furnished", furnished) # Boolean
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
        # item_loader.add_value("floor_plan_images", floor_plan_images) # Array

        # # Monetary Status
        item_loader.add_value("rent", rent) # Int
        item_loader.add_value("deposit", deposit) # Int
        # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", "EUR") # String

        # item_loader.add_value("water_cost", water_cost) # Int
        # item_loader.add_value("heating_cost", heating_cost) # Int

        # item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", 'Uwe Emmrich Immobilien') # String
        item_loader.add_value("landlord_phone", '06048 / 7574') # String
        item_loader.add_value("landlord_email", 'info@uwe-emmrich-immobilien.de') # String

        self.position += 1
        yield item_loader.load_item()
