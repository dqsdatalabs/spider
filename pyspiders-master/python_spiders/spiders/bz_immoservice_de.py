# -*- coding: utf-8 -*-
# Author: Adham Mansour
from math import ceil

import scrapy
from scrapy import Request

from ..helper import remove_unicode_char, extract_number_only, remove_white_spaces, extract_location_from_address, \
    extract_location_from_coordinates
from ..loaders import ListingLoader


class Bz_immoserviceDeSpider(scrapy.Spider):
    name = 'bz_immoservice_de'
    start_urls = ['https://516373.flowfact-sites.net/immoframe/?country=Deutschland&typefilter=1AB70647-4B47-41E2-9571-CA1CA16E0308%7C0',
                  'https://516373.flowfact-sites.net/immoframe/?country=Deutschland&typefilter=E4DE337C-2DE8-4560-9D5F-1C33A96037B6%7C0']  # https not http
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
        rentals = response.css('h3 a::attr(href)').extract()
        for rental in rentals:
            yield Request(url=rental,
                          callback=self.populate_item)
        pages = response.css('div#pagination a')
        for page in pages:
            page_text = page.css('::text').extract_first()
            if page_text == '»':
                next_page = page.css('::attr(href)').extract_first()
                yield Request(url=next_page,
                              callback=self.parse)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        list_items = response.css('.grid1.bnone.detaillist tr')
        list_items_dict = {}
        for list_item in list_items:
            headers = list_item.css('td:nth-child(1)::text').extract()
            values = list_item.css('td:nth-child(2)::text').extract()

            if len(headers) == len(values):
                for n, header in enumerate(headers):
                    list_items_dict[header] = values[n]

        external_id = None # response.css('::text').extract_first()
        if 'Kennung' in list_items_dict.keys():
            external_id = list_items_dict['Kennung']
            external_id = remove_white_spaces(external_id)

        title = response.css('#estate_information span::text').extract_first()
        description = ((((' '.join(response.css('.content:nth-child(4) .pb1 ::text, .content:nth-child(3) .pb1 ::text, .content:nth-child(2) .pb1 ::text').extract()).replace('\n','')).replace('\t','')).replace('\r','')))
        address = None  # response.css('::text').extract_first()
        if 'Lage' in list_items_dict.keys():
            address = list_items_dict['Lage']
        longitude, latitude = extract_location_from_address(address)
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)

        property_type = None  # response.css('::text').extract_first()
        if 'Objektart' in list_items_dict.keys():
            property_type = list_items_dict['Objektart']
            if 'haus' in property_type.lower():
                property_type = 'house'
            else:
                property_type = 'apartment'

        square_meters = None
        if 'Wohnfläche' in list_items_dict.keys():
            square_meters = list_items_dict['Wohnfläche']
            square_meters = int(ceil(float(extract_number_only(square_meters))))

        room_count = None
        if 'Zimmer' in list_items_dict.keys():
            room_count = list_items_dict['Zimmer']
            room_count = int(ceil(float(extract_number_only(room_count))))
        available_date = None  # response.css('.availability .meta-data::text').extract_first()
        if 'Verfügbar ab' in list_items_dict.keys():
            available_date = list_items_dict['Verfügbar ab']
            available_date = remove_white_spaces(available_date).split('.')
            if len(available_date) == 3:
                available_date = available_date[-1] + '-' + available_date[1] + '-' + available_date[0]
            else:
                available_date = 'available now'

        images = response.css('.image_box7::attr(style)').extract()
        images = [((i.split('11007'))[1]) for i in images]
        images = [('11007'+i).replace(');','') for i in images]
        images = ['http://'+i for i in images]
        deposit = None
        if 'Kaution' in list_items_dict.keys():
            deposit = list_items_dict['Kaution']
            deposit = int(ceil(float(extract_number_only(deposit))))
        utilities = None
        if 'Nebenkosten' in list_items_dict.keys():
            utilities = list_items_dict['Nebenkosten']
            utilities = int(ceil(float(extract_number_only(utilities))))
        rent = None  # int(response.css('::text').extract_first())
        if 'Warmmiete' in list_items_dict.keys():
            rent = list_items_dict['Warmmiete']
            rent = int(ceil(float(extract_number_only(rent))))
        elif 'Nettokaltmiete' in list_items_dict.keys():
            rent = list_items_dict['Nettokaltmiete']
            rent = int(ceil(float(extract_number_only(rent))))
        elif 'Miete zzgl. NK' in list_items_dict.keys():
            rent = list_items_dict['Miete zzgl. NK']
            rent = int(ceil(float(extract_number_only(rent))))
            if utilities:
                rent = rent - utilities
        energy_label = None
        if 'Energieeffizienzklasse' in list_items_dict.keys():
            energy_label = list_items_dict['Energieeffizienzklasse']

        floor = None  # response.css('::text').extract_first()
        if 'Etage' in list_items_dict.keys():
            floor = list_items_dict['Etage']

        pets_allowed = None
        if 'Haustiere' in list_items_dict.keys():
            pets_allowed = list_items_dict['Haustiere']
            if pets_allowed.lower() == 'nein':
                pets_allowed = False
            elif pets_allowed.lower() == 'ja':
                pets_allowed = True
            else:
                pets_allowed = None


        furnished = None
        if any(word in remove_unicode_char(description.lower()) for word in self.keywords['furnished']):
            furnished = True

        parking = None
        if 'Garagenmiete' in list_items_dict.keys():
            parking = True
        #
        elevator = None
        if 'Personenaufzug' in list_items_dict.keys():
            elevator = list_items_dict['Personenaufzug']
            if elevator.lower() == 'nein':
                elevator = False
            elif elevator.lower() == 'ja':
                elevator = True
        #
        balcony = None
        terrace = None
        if 'Balkon/Terrasse' in list_items_dict.keys():
            balcony = list_items_dict['Balkon/Terrasse']
            if balcony.lower() == 'nein':
                balcony = False
                terrace = False
            elif balcony.lower() == 'ja':
                balcony = True
                terrace = True
        else:
            if any(word in remove_unicode_char(description.lower()) for word in self.keywords['balcony']):
                balcony = True
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

        landlord_name = response.css('strong::text').extract_first()
        landlord_email = 'bernd.barthmus@bz-immoservice.de'
        landlord_phone = '+49 911 52859402'

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
        # item_loader.add_value("bathroom_count", bathroom_count) # Int

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
        # item_loader.add_value("floor_plan_images", floor_plan_images) # Array

        # # Monetary Status
        item_loader.add_value("rent", rent) # Int
        item_loader.add_value("deposit", deposit) # Int
        # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", "EUR") # String

        # item_loader.add_value("water_cost", water_cost) # Int
        # item_loader.add_value("heating_cost", heating_cost) # Int

        item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name) # String
        item_loader.add_value("landlord_phone", landlord_phone) # String
        item_loader.add_value("landlord_email", landlord_email) # String

        self.position += 1
        yield item_loader.load_item()
