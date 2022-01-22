# -*- coding: utf-8 -*-
# Author: Adham Mansour
import re
from math import ceil

import scrapy
from scrapy import Request


from ..helper import remove_unicode_char, extract_number_only, extract_location_from_address, \
    extract_location_from_coordinates, remove_white_spaces
from ..loaders import ListingLoader


class DedimmobilidiprestigioItSpider(scrapy.Spider):
    name = 'dedimmobilidiprestigio_it'
    allowed_domains = ['dedimmobilidiprestigio.it']
    start_urls = ['https://dedimmobilidiprestigio.it/vetrina.aspx?c=0&sez=0']  # https not http
    country = 'italy'
    locale = 'it'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'
    keywords = {
        'pets_allowed': ['domestici','domestici','animali ammessi'],
        'furnished': ['arredato'],
        'parking': ['parcheggio','parco','box auto','autorimessa'],
        'elevator': ['ascensore', 'sollevamento', 'passaggio', 'portanza', 'strappo', 'montacarichi'],
        'balcony': ['balcone',' balconata'],
        'terrace': ['terrazza','terrazzo'],
        'swimming_pool': ['piscina','pool'],
        'washing_machine': ['lavatrice','rondella','rondello'],
        'dishwasher': ['lavastoviglie','lavapiatti']
    }
    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    # 2. SCRAPING level 2
    def parse(self, response, **kwargs):
        rentals = response.css('.list img::attr(onclick)').extract()
        for rental in rentals:
            external_link = 'https://dedimmobilidiprestigio.it'+(((rental.split("href='"))[1])[:-1])
            yield Request(url=external_link,
                          callback=self.populate_item)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        external_id = ((response.url).split('cod='))[1]
        title = response.css('#titolo b::text').extract_first()
        if 'ufficio' not in title.lower() and 'capannone' not in title.lower() and 'commerciale' not in title.lower():
            description = ((((' '.join(response.css('#descrizione ::text,.title+ span ::text').extract()).replace('\n','')).replace('\t','')).replace('\r','')))
            description =remove_white_spaces(description.replace('descrizione immobile',''))
            address = re.findall('(?:in via |in |al |ad )(.+)',(title.lower()).replace('in affitto',''))
            latitude = None
            longitude = None
            city = None
            zipcode = None
            longitude, latitude = extract_location_from_address(address[0]+',italy')
            zipcode, city, address = extract_location_from_coordinates(longitude, latitude)

            property_type = 'apartment'
            if 'studio' in title.lower():
                property_type = 'studio'
            square_meters = (response.css('#mq b::text').extract_first())
            if square_meters:
                square_meters = int(ceil(float(extract_number_only(square_meters))))
                if square_meters == 0:
                    square_meters = None

            list_items = response.css('#dettagli .inline-block div')
            list_items_dict = {}
            for list_item in list_items:
                headers = list_item.css('::text').extract()
                list_items_dict[headers[0]] = headers[1]

            room_count = None  # int(response.css('::text').extract_first())
            if 'numero camere letto: ' in list_items_dict.keys():
                room_count = list_items_dict['numero camere letto: ']
                room_count = int(ceil(float(extract_number_only(room_count))))
            else:
                room_count = 1
            bathroom_count = None  # int(response.css('::text').extract_first())
            if 'numero bagni: ' in list_items_dict.keys():
                bathroom_count = list_items_dict['numero bagni: ']
                bathroom_count = int(ceil(float(extract_number_only(bathroom_count))))


            images = response.css('#slider a::attr(href)').extract()
            floor_plan_images = None  # response.css('.one-photo-wrapper a::attr(href)').extract()
            rent =(response.css('#prezzo::text').extract_first())
            if rent:
                rent = int(ceil(float(extract_number_only(rent))))
                energy_label = None
                if 'classe energetica: ' in list_items_dict.keys():
                    energy_label = list_items_dict['classe energetica: ']
                floor = None  # response.css('::text').extract_first()
                if 'numero piano: ' in list_items_dict.keys():
                    floor = list_items_dict['numero piano: ']
                pets_allowed = None
                if any(word in remove_unicode_char(description.lower()) for word in self.keywords['pets_allowed']):
                    pets_allowed = True

                furnished = None
                if 'arredato: ' in list_items_dict.keys():
                    furnished = list_items_dict['arredato: ']
                    if furnished == 'assente':
                        furnished = False
                    else:
                        furnished = True

                parking = None
                if 'posti auto: ' in list_items_dict.keys():
                    parking = True

                elevator = None
                if any(word in remove_unicode_char(description.lower()) for word in self.keywords['elevator']):
                    elevator = True

                balcony = None
                if 'numero balconi: ' in list_items_dict.keys():
                    balcony = list_items_dict['numero balconi: ']
                    if int(balcony) > 0:
                        balcony = True
                    else:
                        balcony = False
                #
                terrace = None
                if 'numero terrazzi: ' in list_items_dict.keys():
                    terrace = list_items_dict['numero terrazzi: ']
                    if int(terrace) > 0:
                        terrace = True
                    else:
                        terrace = False

                swimming_pool = None
                if any(word in remove_unicode_char(description.lower()) for word in self.keywords['swimming_pool']):
                    swimming_pool = True

                washing_machine = None
                if 'lavatrice: ' in list_items_dict.keys():
                    washing_machine = list_items_dict['lavatrice: ']
                    if int(washing_machine) > 0:
                        washing_machine = True
                    else:
                        washing_machine = False

                dishwasher = None
                if 'lavastoviglie: ' in list_items_dict.keys():
                    dishwasher = list_items_dict['lavastoviglie: ']
                    if int(dishwasher) > 0:
                        dishwasher = True
                    else:
                        dishwasher = False


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
                # item_loader.add_value("deposit", deposit) # Int
                # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
                # item_loader.add_value("utilities", utilities) # Int
                item_loader.add_value("currency", "EUR") # String

                # item_loader.add_value("water_cost", water_cost) # Int
                # item_loader.add_value("heating_cost", heating_cost) # Int

                item_loader.add_value("energy_label", energy_label) # String

                # # LandLord Details
                item_loader.add_value("landlord_name", 'd&d Immobili di prestigio di Flavio d’Ecclesiis') # String
                item_loader.add_value("landlord_phone", '081 2355865') # String
                item_loader.add_value("landlord_email", 'info@dedimmobilidiprestigio.it') # String

                self.position += 1
                yield item_loader.load_item()
