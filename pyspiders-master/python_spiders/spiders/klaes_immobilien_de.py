# -*- coding: utf-8 -*-
# Author: Adham Mansour
import re
from math import ceil

import scrapy
from scrapy import Request

from ..helper import remove_unicode_char, extract_number_only, remove_white_spaces
from ..loaders import ListingLoader


def list_items_dict_extract(attr_dict_string, list_items_dict, type):
    if type == 'num':
        if attr_dict_string in list_items_dict.keys():
            attribute = list_items_dict[attr_dict_string]
            if len(attribute) == 3:
                attribute = int(ceil(float((attribute))))
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


class Klaes_immobilienDeSpider(scrapy.Spider):
    name = 'klaes_immobilien_de'
    start_urls = ['https://klaes-immobilien.de/Immobilien?search=&type=rent&objektArtSearch=all&zipcode=&city=']
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
            yield scrapy.Request(url, callback=self.parse)

    # 2. SCRAPING level 2
    def parse(self, response, **kwargs):
        rentals = response.css('.immoPrev')
        for rental in rentals:
            title = rental.css('.immoHead h4::text').extract_first()
            if not 'vermietet' in title.lower():
                external_link = rental.css('div.exposeLink > a:nth-child(1)::attr(href)').extract_first()
                yield Request(url=response.urljoin(external_link),
                              callback=self.populate_item)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        if not 'vermietet' in (response.text).lower():
            title = response.css('h4::text').extract_first()
            list_items = response.css('.left')
            list_items_dict = {}
            for list_item in list_items:
                headers = remove_white_spaces(list_item.css('.expFactsTitle::text').extract_first())
                values = remove_white_spaces(list_item.css('.expFactsValue::text').extract_first())
                list_items_dict[headers] = values
            external_id = None
            description = ((' '.join(response.css('h5+ p::text').extract())))
            other_attr = (re.findall('(\w+\/?\w*):((?: |[a-zA-Z]|,|\d|\.)*)', description))
            for item in other_attr:
                list_items_dict[item[0]] = item[1]

            property_type = 'apartment'
            square_meters = list_items_dict_extract('Wohnfläche :', list_items_dict, 'num')
            room_count = list_items_dict_extract('Zimmer :', list_items_dict, 'num')
            bathroom_count = None
            if 'bad' in description.lower():
                bathroom_count = 1
            images = response.css('.expImages img::attr(src)').extract()
            floor_plan_images = response.css('.expImages img')
            floor_plan_images = [i.css('::attr(src)').extract_first() for i in floor_plan_images if
                                 i.css('::attr(title)').extract_first() == 'Layout']
            rent = list_items_dict_extract('Nettokaltmiete', list_items_dict, 'num')
            if rent is None:
                rent = list_items_dict_extract('Nettokaltmiete/Monat', list_items_dict, 'num')

            deposit = list_items_dict_extract('Kaution', list_items_dict, 'num')
            utilities = list_items_dict_extract('Nebenkostenvorauszahlung', list_items_dict, 'num')
            energy_label = list_items_dict_extract('Effizienzklasse', list_items_dict, 'str')

            floor = list_items_dict_extract('Etage', list_items_dict, 'str')

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

            description = ((((' '.join(response.css('h5:contains("Objekt") + p::text').extract()).replace('\n',
                                                                                                          '')).replace(
                '\t', '')).replace('\r', '')))

            # # MetaData
            item_loader.add_value("external_link", response.url)
            item_loader.add_value("external_source", self.external_source)

            item_loader.add_value("external_id", external_id)
            item_loader.add_value("position", self.position)
            item_loader.add_value("title", title)
            item_loader.add_value("description", description)

            # item_loader.add_value("city", city)
            # item_loader.add_value("zipcode", zipcode)
            # item_loader.add_value("address", address)
            # item_loader.add_value("latitude", str(latitude))
            # item_loader.add_value("longitude", str(longitude))
            item_loader.add_value("floor", floor)
            item_loader.add_value("property_type", property_type)
            item_loader.add_value("square_meters", square_meters)
            item_loader.add_value("room_count", room_count)
            item_loader.add_value("bathroom_count", bathroom_count)

            # item_loader.add_value("available_date",available_date)

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
            item_loader.add_value("floor_plan_images", floor_plan_images)

            # # Monetary Status
            item_loader.add_value("rent", rent)
            item_loader.add_value("deposit", deposit)
            # item_loader.add_value("prepaid_rent", prepaid_rent)
            item_loader.add_value("utilities", utilities)
            item_loader.add_value("currency", "EUR")

            # item_loader.add_value("water_cost", water_cost)
            # item_loader.add_value("heating_cost", heating_cost)

            item_loader.add_value("energy_label", energy_label)

            # # LandLord Details
            item_loader.add_value("landlord_name", 'Armin Klaes Immobilien IVD')
            item_loader.add_value("landlord_phone", '02233 - 9666 170')
            item_loader.add_value("landlord_email", 'info@klaes-immobilien.de')

            self.position += 1
            yield item_loader.load_item()
