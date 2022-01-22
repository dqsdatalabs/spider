# -*- coding: utf-8 -*-
# Author: Abanoub Moris
import scrapy
from parsel import Selector
from scrapy.http.request import Request
from ..loaders import ListingLoader
from ..helper import *
import re
import json


class TriconresidentialSpider(scrapy.Spider):

    name = "triconresidential"

    country = 'canada'  # Fill in the Country's name
    locale = 'en'  # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'
    #allowed_domains = ['triconresidential.com']
    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):

        url = f'https://triconresidential.com/api/v1/apartments/the-selby'
        yield Request(url,
                      callback=self.parseApartment,
                      dont_filter=True)

    def parseApartment(self, response):

        apartments = json.loads(response.text)
        title = apartments['name']
        description = apartments['description']
        landlord_name=apartments['community_contact_name']
        landlord_email=apartments['community_contact_email']
        landlord_phone=apartments['community_contact_phone']
        latitude = apartments['lat']
        longitude = apartments['lng']
        property_type = 'apartment'
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)
        images = []
        images.append(apartments['site_map_background']['sizes']['large']['url'])
        ameninites = " ".join(apartments['amenities'])
        for img in apartments['images']:
            images.append(img['sizes']['large']['url'])


        for apartment in apartments['floorplans']:
            external_id = apartment['unit_type_codes'][0]
            floor_plan_images = apartment['sizes']['large']['url']
            for unit in apartments['units']:
                if unit['unit_type_code']==external_id:
                    bathroom_count = re.search(r'\d+',unit['baths'])[0]
                    room_count = unit['beds']
                    square_meters = unit['sqft']
                    rent = unit['min_rent'].replace('.00','')
                    available_date = unit['when_available']
                    external_link=unit['apply_url']
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
                        item_loader.add_value("latitude", str(latitude))  # String
                        item_loader.add_value("longitude", str(longitude))  # String
                        # item_loader.add_value("floor", floor)  # String
                        item_loader.add_value("property_type", property_type)  # String
                        item_loader.add_value("square_meters", square_meters)  # Int
                        item_loader.add_value("room_count", room_count)  # Int
                        item_loader.add_value("bathroom_count", bathroom_count)  # Int

                        item_loader.add_value("available_date", available_date)
                        #print(ameninites)
                   
                        self.get_features_from_description(
                            description+" "+ameninites, response, item_loader)

                        # # Images
                        item_loader.add_value("images", images)  # Array
                        item_loader.add_value(
                            "external_images_count", len(images))  # Int
                        item_loader.add_value(
                            "floor_plan_images", floor_plan_images)  # Array

                        # # Monetary Status
                        item_loader.add_value("rent", rent)  # Int
                        #item_loader.add_value("deposit", deposit)  # Int
                        # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
                        # item_loader.add_value("utilities", utilities)  # Int
                        item_loader.add_value("currency", "CAD")  # String

                        # item_loader.add_value("water_cost", water_cost) # Int
                        # item_loader.add_value("heating_cost", heating_cost) # Int

                        # item_loader.add_value("energy_label", energy_label)  # String

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
        'pets_allowed': ['pet'],
        'furnished': ['furnish', 'MÖBLIERTES'.lower()],
        'parking': ['parking', 'garage', 'parcheggio'],
        'elevator': ['elevator', 'aufzug'],
        'balcony': ['balcon', 'balkon'],
        'terrace': ['terrace', 'terrazz'],
        'swimming_pool': ['pool'],
        'washing_machine': [' washer', 'laundry', 'washing_machine', 'waschmaschine'],
        'dishwasher': ['dishwasher', 'geschirrspüler']
    }

    def get_features_from_description(self, description, response, item_loader):
        description = description.lower()
        pets_allowed = True if any(
            x in description for x in self.Amenties['pets_allowed']) else False
        furnished = True if any(
            x in description for x in self.Amenties['furnished']) else False
        parking = True if any(
            x in description for x in self.Amenties['parking']) else False
        elevator = True if any(
            x in description for x in self.Amenties['elevator']) else False
        balcony = True if any(
            x in description for x in self.Amenties['balcony']) else False
        terrace = True if any(
            x in description for x in self.Amenties['terrace']) else False
        swimming_pool = True if any(
            x in description for x in self.Amenties['swimming_pool']) else False
        washing_machine = True if any(
            x in description for x in self.Amenties['washing_machine']) else False
        dishwasher = True if any(
            x in description for x in self.Amenties['dishwasher']) else False

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
