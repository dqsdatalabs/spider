# -*- coding: utf-8 -*-
# Author: Adham Mansour
import json
import re
from math import ceil

import scrapy
from scrapy import Request

from ..helper import remove_unicode_char, extract_number_only, extract_location_from_address
from ..loaders import ListingLoader


class WiwogDeSpider(scrapy.Spider):
    name = 'wiwog_de'
    start_urls = ['https://wiwog.ivm-professional.de/modules/json/rest_search.php']  # https not http
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
        parsed_response = json.loads(response.body)
        for flat in parsed_response['flats']:
            rent = flat['flat_rent']  # int(response.css('::text').extract_first())
            if rent != '0':
                rent = int(ceil(float((rent))))
                item_loader = ListingLoader(response=response)
                external_link = f'https://www.wiwog.de/beispiel-seite/expose/?flat_id={flat["flat_id"]}'
                external_id = flat['flat_keynumber']
                title = flat['exposetitle']
                description = flat['district_description']+' '+ flat['note'] # ((((' '.join(response.css('::text').extract()).replace('\n','')).replace('\t','')).replace('\r','')))
                city = flat['flat_city']
                zipcode = flat['flat_zip']
                address = flat['flat_street']+', '+city+', '+zipcode+', '+'germany'  # response.css('::text').extract_first()
                longitude, latitude = extract_location_from_address(address)
                property_type = 'apartment'  # response.css('::text').extract_first()
                square_meters = flat['flat_space']
                square_meters = int(ceil(float((square_meters))))# METERS #int(response.css('::text').extract_first())
                room_count = int(ceil(float(flat['flat_rooms']))) # int(response.css('::text').extract_first())
                bathroom_count = flat['portal_badanzahl']
                if bathroom_count is not None:
                    bathroom_count = int(ceil(float(bathroom_count)))
                    if bathroom_count == 0:
                        bathroom_count =None
                if 'badezimmer' in description:
                    bathroom_count = 1

                available_date = flat['flat_rent_date']
                if available_date =="0000-00-00":
                    available_date = None

                images = flat['flat_image']
                images = ['https://wiwog.ivm-professional.de/_img/flats/'+ images.replace(' ','%20')]
                extra_images = flat['gallery_img']
                extra_images = re.findall('%22([.\d\w_-]+(?:.jpg|.JPG|.jpeg|.JPEG|.png|.PNG))',extra_images)
                extra_images = [f'https://wiwog.ivm-professional.de/_img/gallery/{flat["flat_id"]}/img_'+i for i in extra_images]
                images = images + extra_images
                images = [i.encode("utf-8") for i in images]
                images = [i.decode("utf-8") for i in images]
                images = [i for i in images if 'https://wiwog.ivm-professional.de/_img/flats/1051%20Stra\u00c3' not in i ]
                floor_plan_images = flat['flat_plot']
                if floor_plan_images != '':
                    floor_plan_images = 'https://wiwog.ivm-professional.de/_img/plots/' +floor_plan_images
                else:
                    floor_plan_images = None

                deposit = int(ceil(float((flat['flat_deposit']))))
                utilities = flat['flat_charges']
                utilities = int(ceil(float((utilities))))
                heating_cost = flat['flat_heating']
                heating_cost = int(ceil(float((heating_cost))))
                if heating_cost == 0:
                    heating_cost = None
                floor = flat['flat_floor']  # response.css('::text').extract_first()

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

                landlord_name = flat['arranger_name']
                landlord_email = flat['arranger_phone']
                landlord_phone = flat['arranger_email']

                # # MetaData
                item_loader.add_value("external_link", external_link)  # String
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
                #
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

                # item_loader.add_value("energy_label", energy_label) # String

                # # LandLord Details
                item_loader.add_value("landlord_name", landlord_name) # String
                item_loader.add_value("landlord_phone", landlord_phone) # String
                item_loader.add_value("landlord_email", landlord_email) # String

                self.position += 1
                yield item_loader.load_item()
