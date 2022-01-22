# -*- coding: utf-8 -*-
# Author: Adham Mansour
from datetime import datetime
from math import ceil

import dateparser
import scrapy
from scrapy import Request

from ..helper import remove_unicode_char, extract_number_only, extract_location_from_address, \
    extract_location_from_coordinates
from ..loaders import ListingLoader


class Agenda_immobilienDeSpider(scrapy.Spider):
    name = 'agenda_immobilien_de'
    start_urls = ['https://cs.immopool.de/CS/getListe']  # https not http
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
        'swimming_pool': [' baden', 'schwimmen', 'schwimmbad', 'pool', 'Freibad'],
        'washing_machine': ['waschen', 'w scherei', 'waschmaschine','waschk che'],
        'dishwasher': ['geschirrspulmaschine', 'geschirrsp ler',],
        'floor' : ['etage'],
        'bedroom': ['Schlafzimmer']
    }
    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        formdata = {
            "kdnr": '173267',
            "isIframe": '0',
            "targetFrame": 'contentService',
            "firmaVerkn": '0',
            "mkz":'',
            "ohneRefPreis": '1',
            "zweitAnbieter": '0',
            "zweitGenerell": '0',
            "boerseMakler": '0',
            "refNurEinBild": '0',
            "refKeinExpose": '0',
            "start": 'false',
            "version": '3',
            "pageSize": '100',
            "pageIndex": '0',
            "objkat": '2',
            "zusatzid": '0',
            "vermarktung": '1',
            "geosl":'',
            "ortsBez":'',
            "umkreis": '0',
            "umkreisBez":'',
            "preisAb":'',
            "preisBis":'',
            "flaecheAb":'',
            "flaecheBis":'',
            "zimmerAb":'',
            "baujahrAb":'',
            "baujahrBis":'',
            "grdstAb":'',
            "grdstBis":'',
            "objart":'',
            "sortOrder": '0_1',
            "istMerkListe": '0',
            "referenz": '0',
            "mediatyp": '0',
            "refKeinExpose": '0',
            "geosl": '0',
        }
        for url in self.start_urls:
            yield scrapy.FormRequest(url, callback=self.parse,formdata=formdata)

    # 2. SCRAPING level 2
    def parse(self, response, **kwargs):
        rentals = response.css('.list3-row.rel a::attr(data-id)').extract()
        for rental in rentals:
            formdata = {
                'onlinenr': rental,
                'anzahlDS': '19',
                'rowNumber': '1',
                'listtyp': '0',
                'isIframe': '0',
                'targetFrame': 'contentService',
                'kdnr': '173267',
                'zweitAnbieter': '0',
                'zweitGenerell': '0',
                'boerseMakler': '0',
            }
            yield scrapy.FormRequest(url='https://cs.immopool.de/CS/getExpose',
                          callback=self.populate_item,formdata=formdata)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        external_id = response.css('.expose-internenr:contains("Obj")+ .left::text').extract_first()
        title =response.css('h2.text-overflow::text').extract_first()
        description = ((((' '.join(response.css('.expose-daten-beschreibung::text').extract()).replace('\n','')).replace('\t','')).replace('\r','')))
        address = None  # response.css('::text').extract_first()
        latitude = None
        longitude = None
        for i in response.css('input'):
            if i.css('::attr(name)').extract_first() == 'geoKoordsLat':
                latitude = i.css('::attr(value)').extract_first()
            elif i.css('::attr(name)').extract_first() == 'geoKoordsLng':
                longitude = i.css('::attr(value)').extract_first()
            elif i.css('::attr(name)').extract_first() == 'geoOrt':
                address = i.css('::attr(value)').extract_first()
                zipcode = extract_number_only(address)
        city = None
        if latitude is not None and longitude is not None:
            external_zip, city, address = extract_location_from_coordinates(longitude, latitude)
            if external_zip != '':
                zipcode = external_zip
        elif address is not None:
            longitude, latitude = extract_location_from_address(address)
            external_zip, city, address = extract_location_from_coordinates(longitude, latitude)
            if external_zip != '':
                zipcode = external_zip
        if zipcode == address:
            address = address +', '+ city
        property_type = 'apartment'
        if 'haus' in title.lower() or 'villa' in title.lower():
                property_type = 'house'
        list_items_dict = {}
        headers = response.css('.expose-daten-label ::text').extract()
        values = response.css('.expose-daten-val ::text').extract()
        if len(headers) == len(values):
            for n, header in enumerate(headers):
                list_items_dict[header] = values[n]
        square_meters = None  # METERS #int(response.css('::text').extract_first())
        if 'Wohnfläche m²' in list_items_dict.keys():
            square_meters = list_items_dict['Wohnfläche m²']
            square_meters = int(ceil(float(extract_number_only(square_meters))))
        room_count = None  # int(response.css('::text').extract_first())
        if 'Anzahl Zimmer' in list_items_dict.keys():
            room_count = list_items_dict['Anzahl Zimmer']
            room_count= int(ceil(float(extract_number_only(room_count))))
        bathroom_count = None  # int(response.css('::text').extract_first())

        available_date = None  # response.css('.availability .meta-data::text').extract_first()
        if 'Frei ab' in list_items_dict.keys():
            available_date = list_items_dict['Frei ab']
        if available_date != 'nach Vereinbarung' and available_date is not None:
            if available_date.lower() != 'sofort':
                available_date = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if available_date > datetime.now():
                    available_date = available_date.strftime("%Y-%m-%d")
                else:
                    available_date = None
            else:
                available_date = 'available now'
        else:
            available_date = None
        images = response.css('.fotorama a::attr(href)').extract()
        floor_plan_images = None  # response.css('.one-photo-wrapper a::attr(href)').extract()
        rent = None  # int(response.css('::text').extract_first())
        if 'Mietpreis EUR' in list_items_dict.keys():
            rent = list_items_dict['Mietpreis EUR']
            rent = int(ceil(float(extract_number_only(rent))))
        deposit = None
        if 'Kaution' in list_items_dict.keys():
            deposit = list_items_dict['Kaution']
            deposit = int(ceil(float(extract_number_only(deposit))))
        prepaid_rent = None
        utilities = None
        if 'Nebenkosten EUR' in list_items_dict.keys():
            utilities = list_items_dict['Nebenkosten EUR']
            utilities = int(ceil(float(extract_number_only(utilities))))
        water_cost = None
        heating_cost = None
        energy_label = None
        if 'Energieeffizienzklasse' in list_items_dict.keys():
            energy_label = list_items_dict['Energieeffizienzklasse']
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

        balcony = None
        if 'Anzahl Balkone' in list_items_dict.keys():
            balcony = list_items_dict['Anzahl Balkone']
            balcony = int(ceil(float(extract_number_only(balcony))))
            if balcony > 0:
                balcony = True
            else:
                balcony = False
        elif any(word in remove_unicode_char(description.lower()) for word in self.keywords['balcony']):
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

        landlord_name = response.css('.name strong::text').extract_first()
        landlord_email = 'info@agenda-immobilien.de'
        landlord_phone = ((response.css('.expose-anbieter-ansprechpartner-daten div:contains("Mobil")::text').extract_first()).split(': '))[1]

        # # MetaData
        item_loader.add_value("external_link", 'https://agenda-immobilien.de/?Immobilienangebote#'+str(self.position))  # String
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
