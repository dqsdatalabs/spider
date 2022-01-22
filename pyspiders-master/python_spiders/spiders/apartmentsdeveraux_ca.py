# -*- coding: utf-8 -*-
# Author: Abanoub Moris

import json
import re

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader

class Apartments_deveraux_caSpider(Spider):
    name = 'apartmentsdeverauxca'
    country='canada'
    locale='en' 
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type='testing'
    position = 1
    def start_requests(self):
        yield Request(url='https://api.theliftsystem.com/v2/search?locale=en&client_id=219&auth_token=sswpREkUtyeYjeoahA2i&city_id=845&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=0&max_rate=2500&min_sqft=0&max_sqft=10000&show_custom_fields=true&show_promotions=true&show_amenities=true&region=&keyword=false&property_types=apartments,+houses&ownership_types=&exclude_ownership_types=&custom_field_key=&custom_field_values=&order=min_rate+ASC&limit=66&neighbourhood=&amenities=&promotions=&city_ids=2356,3377,1872,845&pet_friendly=&offset=0&count=false',
                      callback=self.parse,
                      method='GET')

    def parse(self, response):
        parsed_response_properties = json.loads(response.body)
        for property_data in parsed_response_properties:
            yield Request(url=property_data["permalink"], callback=self.populate_item, meta = {"property_data": property_data})

    def populate_item(self, response):
        property_data = response.meta.get("property_data")
        available_date = property_data["availability_status_label"]
      
        if( available_date == "No Vacancy"):
            return
        property_type = "apartment"
        title = property_data["name"]
        
        
        currency = "CAD"


        address = property_data["address"]['address']
        city = property_data["address"]['city']
        zipcode = property_data["address"]["postal_code"]

        images_to_add = []
        images = response.css("div.gallery-image div.cover::attr(style)").getall()
        for image_src in images:
            image_src = image_src.split("background-image:url('")[1]
            image_src = image_src.split("');")[0]
            images_to_add.append(image_src)

        landlord_name = property_data['contact']['name']
        landlord_phone = property_data['contact']['email']
        landlord_email = property_data['contact']['phone']

        pets_allowed = property_data["pet_friendly"]
        if( pets_allowed == "n/a"):
            pets_allowed = None

        description = property_data['details']["overview"]
        description = re.sub("</?[a-z]+>", "",description)
        latitude = property_data["geocode"]["latitude"]
        longitude = property_data["geocode"]["longitude"]

        external_link = property_data["permalink"]
        external_id = str(property_data["id"])
        
        amenities = response.css("div.amenity-group div.amenity-holder::text").getall()
        amenities = " ".join(amenities)
        amenities = re.sub("\s+", " ", amenities)
        amenities = amenities.lower()

        i=1

        for suit in response.css(".widget .suite"):
            rent = re.search(r'\d+',suit.css('.suite-price .value::text').get())[0]

            room_count = re.search(r'\d+',suit.css('.suite-beds .value::text').get())[0]
            room_count = str(int(room_count))
            if(room_count == "0"):
                room_count = "1"

            bathroom_count = re.search(r'\d+',suit.css('.suite-bath .value::text').get())[0]
            rex = re.findall(r'\d+',suit.css('.suite-sqft .value::text').get())
            square_meters=0
            for s in rex:
                square_meters+=int(s)
            if square_meters>0:
                square_meters=int(square_meters/len(rex))
            
            floor_plan_images = suit.css('.floorplans-link a::attr(href)').getall()


            item_loader = ListingLoader(response=response)

            if int(rent) > 0 and int(rent) < 20000:

                # # MetaData
                item_loader.add_value("external_link", response.url+'#'+str(i))  # String
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

                self.get_features_from_description(description+amenities, response, item_loader)

                # # Images
                item_loader.add_value("images", images_to_add)  # Array
                item_loader.add_value(
                    "external_images_count", len(images_to_add))  # Int
                item_loader.add_value("floor_plan_images", floor_plan_images)  # Array

                # # Monetary Status
                item_loader.add_value("rent", rent)  # Int
                # item_loader.add_value("deposit", deposit)  # Int
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
                item_loader.add_value("landlord_email", landlord_email)  # String
                self.position+=1
                i+=1
            
                yield item_loader.load_item()
    
    Amenties = {
        'pets_allowed': ['pet'],
        'furnished': ['furnish', 'MÖBLIERTES'.lower()],
        'parking': ['parking', 'garage', 'parcheggio'],
        'elevator': ['elevator', 'aufzug', 'ascenseur'],
        'balcony': ['balcon', 'balkon'],
        'terrace': ['terrace', 'terrazz', 'terras'],
        'swimming_pool': ['pool', 'piscine'],
        'washing_machine': [' washer', 'laundry', 'washing_machine', 'waschmaschine', 'laveuse'],
        'dishwasher': ['dishwasher', 'geschirrspüler', 'lave-vaiselle', 'lave vaiselle']
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

