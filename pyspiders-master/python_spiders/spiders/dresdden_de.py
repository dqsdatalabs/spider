# -*- coding: utf-8 -*-
# Author: Abanoub Moris
import json
import scrapy
from parsel import Selector
from scrapy.http.request import Request
from ..loaders import ListingLoader
from ..helper import *
import re


class DresddenSpider(scrapy.Spider):

    name = "dresdden_de"
    country = 'germany'  # Fill in the Country's name
    locale = 'de'  # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'
    position = 1    

    # 1. SCRAPING level 1
    def start_requests(self):
        for i in range(1,5):
            url = f'https://www.dresdden.de/object_list.php?search_type=Alle_Miete&search_rooms=0&offset={i}'
            yield Request(url, dont_filter=True, callback=self.parseApartment)

    # 2. SCRAPING level 2

    def parse(self, response):
        pass

    def parseApartment(self, response):
        
        apartments = response.css(".obj_list_content_col")
        for apartment in apartments:
            row = apartment.css(".obj_list_content *::text").getall()
            external_id = str(re.search(r'\d+',row[0])[0])
            rx = re.search(r'\d+',row[2])
            floor=''
            if rx:
                floor = str(rx[0])

            rx = re.search(r'\d+',row[3])
            square_meters=0
            if rx:
                square_meters = str(rx[0])

            rx = re.search(r'\d+',row[4])
            room_count=1
            if rx:
                room_count = str(rx[0])

            rent = re.search(r'\d+',row[5].replace(',00','').replace(',','').replace('.',''))[0]

            external_link = 'https://www.dresdden.de/' + apartment.css(".obj_list_content:nth-child(2) a::attr(href)").get()

            r = Selector(requests.get(external_link).text)
            address = remove_white_spaces(r.css("#adresse::text").get().replace('\n',''))
            title = "property for Rent at "+address
            available_date = r.css(".print_descrip_value:contains(rei) .print_descrip_value_right::text").get()
            description = remove_white_spaces(" ".join(r.css("#obj_description *::text").getall()))
            description = re.sub(
                r'email.+|call.+|contact.+|apply.+|\d+.\d+.\d+.\d+', "", description.lower())
            property_type = 'apartment' if 'wohnung' in description else 'house'

            longitude,latitude='',''
            longitude,latitude = extract_location_from_address(address)

            zipcode, city, addres = '', '', ''
            try:
        
                zipcode, city, addres = extract_location_from_coordinates(
                    longitude, latitude)
            except:
                pass

            images = [x for x in r.css("#this_object img::attr(src)").getall() if 'png' not in x]

            property_type = 'apartment' if 'apartment' in description.lower(
            ) or 'apartment' in title.lower()  else property_type


            if int(rent) > 0 and int(rent) < 20000:
                item_loader = ListingLoader(response=response)

                # # MetaData
                item_loader.add_value("external_link", external_link)  # String
                item_loader.add_value(
                    "external_source", self.external_source)  # String

                item_loader.add_value("external_id", str(external_id))  # String
                item_loader.add_value("position", self.position)  # Int
                item_loader.add_value("title", title)  # String
                item_loader.add_value("description", description)  # String

                # # Property Details
                item_loader.add_value("city", city)  # String
                item_loader.add_value("zipcode", zipcode)  # String
                item_loader.add_value("address", address)  # String
                item_loader.add_value("latitude", str(latitude))  # String
                item_loader.add_value("longitude", str(longitude))  # String
                item_loader.add_value("floor", floor)  # String
                item_loader.add_value("property_type", property_type)  # String
                item_loader.add_value("square_meters", square_meters)  # Int
                item_loader.add_value("room_count", room_count)  # Int
                #item_loader.add_value("bathroom_count", bathroom_count)  # Int

                item_loader.add_value("available_date", available_date)

                self.get_features_from_description(
                    description+" ".join(response.css(".amenities-list *::text").getall()), response, item_loader)

                # # Images
                item_loader.add_value("images", images)  # Array
                item_loader.add_value(
                    "external_images_count", len(images))  # Int
                # item_loader.add_value("floor_plan_images", floor_plan_images)  # Array

                # # Monetary Status
                item_loader.add_value("rent", rent)  # Int
                #item_loader.add_value("deposit", deposit)  # Int
                # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
                # item_loader.add_value("utilities", utilities)  # Int
                item_loader.add_value("currency", "EUR")  # String

                # item_loader.add_value("water_cost", water_cost) # Int
                # item_loader.add_value("heating_cost", heating_cost) # Int

                # item_loader.add_value("energy_label", energy_label)  # String

                # # LandLord Details
                item_loader.add_value(
                    "landlord_name", 'Dresdden')  # String
                item_loader.add_value(
                    "landlord_phone",'+49 174 - 304 5000')  # String
                item_loader.add_value("landlord_email", ' info@dresdden.de')  # String

                self.position += 1
                yield item_loader.load_item()

    Amenties = {
        'pets_allowed': ['pet'],
        'furnished': ['furnish', 'MÖBLIERTES'.lower()],
        'parking': ['parking', 'garage', 'parcheggio'],
        'elevator': ['elevator', 'aufzug', 'ascenseur','lift','aufzüg'],
        'balcony': ['balcon', 'balkon'],
        'terrace': ['terrace', 'terrazz', 'terras','terrass'],
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