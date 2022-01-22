from operator import le
from requests.api import get, post
from requests.models import Response
import scrapy
from scrapy import Request, FormRequest
from scrapy.http.request import form
from scrapy.utils.response import open_in_browser
from scrapy.loader import ItemLoader
from scrapy.selector import Selector
from scrapy.utils.url import add_http_if_no_scheme

from ..loaders import ListingLoader
from ..helper import *
from ..user_agents import random_user_agent

import requests
import re
import time
from urllib.parse import urlparse, urlunparse, parse_qs
import json
import math


class Immosuche_degewo_Spider(scrapy.Spider):

    name = 'immosuche_degewo'
    execution_type = 'testing'
    
    country = 'germany'
    locale = 'de'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"

    position = 1

    def start_requests(self):
        for page in range(1,5):
            url = f'https://immosuche.degewo.de/de/search.json?utf8=%E2%9C%93&property_type_id=1&categories[]=1&property_number=&address[raw]=&address[street]=&address[city]=&address[zipcode]=&address[district]=&district=&price_switch=false&price_switch=on&price_from=&price_to=&price_from=&price_to=&price_radio=null&price_from=&price_to=&qm_radio=null&qm_from=&qm_to=&rooms_radio=null&rooms_from=&rooms_to=&features[]=&wbs_required=&order=rent_total_without_vat_asc&page={page}'
      
            yield FormRequest(
                url, callback=self.parseDetails,dont_filter=True
            )

    def parseDetails(self, response):

        apartments = apartments = json.loads(response.text)['immos']

        for apartment in apartments:
            title = apartment['headline']
            external_id = apartment['id']
            external_link = 'https://immosuche.degewo.de/de/properties/' + \
                apartment['id']

            latitude = str(apartment['location']['lat'])
            longitude = str(apartment['location']['lon'])
            city = apartment['city']
            zipcode = apartment['zipcode']
            address = apartment['full_address']
            property_type = 'apartment'
            energy_label=apartment['energy_efficiency_class']
            square_meters = int(float(apartment['living_space']))
            floor = str(int(float(apartment['floor'])))
            description = apartment['description']
            description =str(description) +"\n"+ str(apartment['other_information'])
            rent = apartment['rent_total_with_vat']
            rent = re.search(r'\d+', rent.replace(',00', '').replace('.', ''))[0]
            room_count = apartment['number_of_rooms'][0]
            utilities = int(float(apartment['service_charges_cold']))
            available_date = 'Available Now' if 'sofort' in apartment['available_from'].lower() else apartment['available_from'] 
            images = []
            for img in apartment['external_data']:
                filename = img['filename']
                if 'jpg' in filename:
                    images.append(f'https://immosuche.degewo.de/images/properties/full/1520x1140/{filename}')
            

            landlord_name = apartment['authority']['surname']
            landlord_email = apartment['authority']['email']
            landlord_phone = apartment['authority']['telephone']

            if int(rent) > 0 and int(rent) < 20000:
                item_loader = ListingLoader(response=response)

                # # MetaData
                item_loader.add_value("external_link", external_link)  # String
                item_loader.add_value(
                    "external_source", self.external_source)  # String

                item_loader.add_value("external_id", external_id)  # String
                item_loader.add_value("position", self.position)  # Int
                item_loader.add_value("title", title)  # String
                item_loader.add_value("description", description)  # String

                # # Property Details
                item_loader.add_value("city", city)  # String
                item_loader.add_value("zipcode", zipcode)  # String
                item_loader.add_value("address", address)  # String
                item_loader.add_value("latitude", latitude)  # String
                item_loader.add_value("longitude", longitude)  # String
                item_loader.add_value("floor", floor)  # String
                item_loader.add_value("property_type", property_type)  # String
                item_loader.add_value("square_meters", square_meters)  # Int
                item_loader.add_value("room_count", room_count)  # Int
                #item_loader.add_value("bathroom_count", bathroom_count)  # Int

                item_loader.add_value("available_date", available_date)

                self.get_features_from_description(
                    str(description)+str(apartment['furnishings_text']),response,item_loader)

                # # Images
                item_loader.add_value("images", images)  # Array
                item_loader.add_value("external_images_count", len(images))  # Int
                # item_loader.add_value("floor_plan_images", floor_plan_images) # Array

                # # Monetary Status
                item_loader.add_value("rent", rent)  # Int
                # item_loader.add_value("deposit", deposit) # Int
                # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
                item_loader.add_value("utilities", utilities)  # Int
                item_loader.add_value("currency", "EUR")  # String

                # item_loader.add_value("water_cost", water_cost) # Int
                # item_loader.add_value("heating_cost", heating_cost) # Int

                item_loader.add_value("energy_label", energy_label)  # String

                # # LandLord Details
                item_loader.add_value(
                    "landlord_name", landlord_name)  # String
                item_loader.add_value(
                    "landlord_phone", landlord_phone)  # String
                item_loader.add_value(
                    "landlord_email", landlord_email)  # String

                self.position += 1
                yield item_loader.load_item()

    Amenties = {
            'pets_allowed':['pets'],
            'furnished':['furnish','MÖBLIERTES'.lower()],
            'parking':['parking','garage'],
            'elevator':['elevator','aufzug'],
            'balcony':['balcon','balkon'],
            'terrace':['terrace'],
            'swimming_pool':['pool'],
            'washing_machine':[' washer','laundry','washing_machine','waschmaschine'],
            'dishwasher':['dishwasher','geschirrspüler']
        }

    def get_features_from_description(self, description, response, item_loader):
        description     = description.lower()
        pets_allowed    = True if any(x in description for x in self.Amenties['pets_allowed']) else False
        furnished       = True if any(x in description for x in self.Amenties['furnished']) else False
        parking         = True if any(x in description for x in self.Amenties['parking']) else False
        elevator        = True if any(x in description for x in self.Amenties['elevator']) else False
        balcony         = True if any(x in description for x in self.Amenties['balcony']) else False
        terrace         = True if any(x in description for x in self.Amenties['terrace']) else False
        swimming_pool   = True if any(x in description for x in self.Amenties['swimming_pool']) else False
        washing_machine = True if any(x in description for x in self.Amenties['washing_machine']) else False
        dishwasher      = True if any(x in description for x in self.Amenties['dishwasher']) else False

        item_loader.add_value("pets_allowed", pets_allowed)  # Boolean
        item_loader.add_value("furnished", furnished)  # Boolean
        item_loader.add_value("parking", parking)  # Boolean
        item_loader.add_value("elevator", elevator)  # Boolean
        item_loader.add_value("balcony", balcony)  # Boolean
        item_loader.add_value("terrace", terrace)  # Boolean
        item_loader.add_value("swimming_pool", swimming_pool)  # Boolean
        item_loader.add_value("washing_machine", washing_machine)  # Boolean
        item_loader.add_value("dishwasher", dishwasher)  # Boolean
        return pets_allowed, furnished, parking, elevator, balcony, terrace, swimming_pool, washing_machine, dishwasher