# -*- coding: utf-8 -*-
# Author: Adham Mansour
from math import ceil

import scrapy
from scrapy import Request

from ..helper import remove_unicode_char, extract_number_only, extract_location_from_address, \
    extract_location_from_coordinates
from ..loaders import ListingLoader


def list_items_dict_extract(attr_dict_string, list_items_dict, type):
    if type == 'num':
        if attr_dict_string in list_items_dict.keys():
            attribute = list_items_dict[attr_dict_string]
            if len(attribute) == 3:
                attribute = int(ceil(float((attribute.replace(',', '.')))))
            else:
                attribute = int(ceil(float(extract_number_only(attribute))))
            return attribute
        else:
            return None
    elif type == 'str':
        if attr_dict_string in list_items_dict.keys():
            attribute = list_items_dict[attr_dict_string]
            return attribute
        else:
            return None
    else:
        raise Exception("Type provided is incorrect. available types are 'num' and 'str'")


class Shh_immobilienDeSpider(scrapy.Spider):
    name = 'shh_immobilien_de'
    allowed_domains = ['shh-immobilien.de']
    start_urls = ['https://shh-immobilien.de/']  # https not http
    country = 'germany'
    locale = 'de'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'
    keywords = {
        'pets_allowed': ['Haustiere erlaubt'],
        'furnished': ['m bliert', 'ausstattung'],
        'parking': ['garage', 'Stellplatz' 'parkh user', 'Parkplatz'],
        'elevator': ['fahrstuhl', 'aufzug'],
        'balcony': ['balkon'],
        'terrace': ['terrasse'],
        'swimming_pool': [' baden', 'schwimmen', 'schwimmbad', 'pool', 'Freibad'],
        'washing_machine': ['waschen', 'w scherei', 'waschmaschine', 'waschk che'],
        'dishwasher': ['geschirrspulmaschine', 'geschirrsp ler', ],
        'floor': ['etage'],
        'bedroom': ['Schlafzimmer']
    }
    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.cityparse)

    # 2. SCRAPING level 2
    def cityparse(self, response, **kwargs):
        cities = response.css('map area::attr(alt)').extract()
        for city in cities:
            alt_city = city.replace('-', '_')
            external_link = f'https://www.shh-immobilien.de/miete_{alt_city}_alle/?objektkategorie=Miete&objekttyp=&objektort={alt_city}'
            yield Request(url=external_link,
                          callback=self.rentalparse)

    # 2. SCRAPING level 3
    def rentalparse(self, response, **kwargs):
        rentals = response.css('strong a::attr(href)').extract()
        for rental in rentals:
            if 'objekte' in rental:
                yield Request(url=rental,
                              callback=self.populate_item)

    # 3. SCRAPING level 4
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        if not 'V E R M I E T E T' in response.text and not 'vermietet' in response.text and not 'VERMIETET' in response.text:
            list_items = response.css('#datentabelle tr')
            list_items_dict = {}
            for list_item in list_items:
                head_val = list_item.css('td')
                if len(head_val) == 2:
                    head_val = (head_val.css(' ::text').extract())
                    headers = head_val[0]
                    values = head_val[1]
                    list_items_dict[headers] = values
            title = response.css('h2::text').extract_first()
            external_id = (((response.css('td:contains("bjekt Nr.")::text').extract())[1]).split('Nr. '))[1]
            description = (((((response.text).replace('\n', '')).replace('\t', '')).replace('\r', '')))
            city = response.css(
                '#contentlinks > table:nth-child(1)  tr > td:nth-child(1) > a:nth-child(1)::text').extract_first()
            address = city + ', Germany'
            longitude, latitude = extract_location_from_address(address)
            zipcode, city, address = extract_location_from_coordinates(longitude, latitude)
            city = response.css(
                '#contentlinks > table:nth-child(1)  tr > td:nth-child(1) > a:nth-child(1)::text').extract_first()
            address = city + ', Germany'

            property_type = 'apartment'
            if 'haus' in title.lower():
                property_type = 'house'
            square_meters = list_items_dict_extract('Wohnfläche', list_items_dict, 'num')
            room_count = list_items_dict_extract('Zimmer', list_items_dict, 'num')
            bathroom_count = list_items_dict_extract('Badezimmer', list_items_dict, 'num')
            if bathroom_count is None:
                if 'bad' in description.lower():
                    bathroom_count = 1
            available_date = None

            images = response.css('#bildertabelle tr > td:nth-child(2) > table tr > td a::attr(href)').extract()
            rent = list_items_dict_extract('Miete inkl. Nebenkosten', list_items_dict, 'num')
            deposit = list_items_dict_extract('Kaution', list_items_dict, 'num')
            utilities = list_items_dict_extract('Netto-Kaltmiete zzgl. Nebenkosten', list_items_dict, 'num')
            if utilities:
                utilities = rent - utilities
            energy_label = None

            floor = list_items_dict_extract('Etage', list_items_dict, 'str')

            pets_allowed = list_items_dict_extract('Haustiere', list_items_dict, 'str')
            if pets_allowed:
                if pets_allowed.lower() == 'ja':
                    pets_allowed = True
                elif pets_allowed.lower() == 'nein':
                    pets_allowed = False
                else:
                    pets_allowed = None
            elif any(word in remove_unicode_char(description.lower()) for word in self.keywords['pets_allowed']):
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

            landlord_name = 'SHH real estate'
            landlord_email = 'info@shh-immobilien.de'
            landlord_phone = '0431 - 80 66 31 88'
            description = ((((' '.join(
                response.css('tr:contains("Objektbeschreibung") + tr td::text').extract()).replace('\n', '')).replace(
                '\t', '')).replace('\r', '')))
            # # MetaData
            item_loader.add_value("external_link", response.url)
            item_loader.add_value("external_source", self.external_source)

            item_loader.add_value("external_id", external_id)
            item_loader.add_value("position", self.position)
            item_loader.add_value("title", title)
            item_loader.add_value("description", description)

            # # Property Details
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode)
            item_loader.add_value("address", address)
            item_loader.add_value("latitude", str(latitude))
            item_loader.add_value("longitude", str(longitude))
            item_loader.add_value("floor", floor)
            item_loader.add_value("property_type", property_type)
            item_loader.add_value("square_meters", square_meters)
            item_loader.add_value("room_count", room_count)
            item_loader.add_value("bathroom_count", bathroom_count)

            item_loader.add_value("available_date",
                                  available_date)

            item_loader.add_value("pets_allowed", pets_allowed)
            item_loader.add_value("furnished", furnished)
            item_loader.add_value("parking", parking)
            item_loader.add_value("elevator", elevator)
            item_loader.add_value("balcony", balcony)
            item_loader.add_value("terrace", terrace)
            item_loader.add_value("swimming_pool", swimming_pool)
            item_loader.add_value("washing_machine", washing_machine)
            item_loader.add_value("dishwasher", dishwasher)

            # # Images
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))
            # item_loader.add_value("floor_plan_images", floor_plan_images) # Array

            # # Monetary Status
            item_loader.add_value("rent", rent)
            item_loader.add_value("deposit", deposit)
            # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
            item_loader.add_value("utilities", utilities)
            item_loader.add_value("currency", "EUR")

            item_loader.add_value("energy_label", energy_label)

            item_loader.add_value("landlord_name", landlord_name)
            item_loader.add_value("landlord_phone", landlord_phone)
            item_loader.add_value("landlord_email", landlord_email)

            self.position += 1
            yield item_loader.load_item()
