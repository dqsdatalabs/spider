# -*- coding: utf-8 -*-
# Author: Abanoub Moris
import scrapy
from parsel import Selector
from scrapy.http.request import Request
from ..loaders import ListingLoader
from ..helper import *
import re


class KpigroupSpider(scrapy.Spider):

    name = "kpigroup"
    country = 'canada'  # Fill in the Country's name
    locale = 'en'  # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'
    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):

        urls = ['https://kpigroup.ca/residential-property-management']
        for url in urls:
            yield Request(url,
                          callback=self.parse,
                          dont_filter=True)

    # 2. SCRAPING level 2

    def parse(self, response):

        apartments = response.css(".card a::attr(href)").getall()
        for url in apartments:
            yield Request(url, dont_filter=True, callback=self.parseApartment)

    def parseApartment(self, response):

        title = response.css(".container h1::text").get()
        rex = re.search(
            r'\$\d+', "".join(response.css(".py-2 *::text").getall()).replace(',',''))
        rent = ''
        if rex:
            rent = rex[0].replace('$','')

        rex = re.search(
            r'beds.\d+', ("".join(response.css(".py-2 *::text").getall()).lower().replace(',','')))

        room_count = ''
        if rex:
            room_count = re.search(r'\d+', rex[0])[0]


        rex = re.search(
            r'baths.\d+', ("".join(response.css(".py-2 *::text").getall()).lower()))
        bathroom_count = ''
        if rex:
            bathroom_count = re.search(r'\d+', rex[0])[0]


        description = remove_white_spaces(
            "".join(response.css(".tab-pane[id='tabs-1'] *::text").getall()))
        description = re.sub(
            r'email.+|call.+|contact.+|apply.+|\d+.\d+.\d+.\d+', "", description.lower())

        images = response.css(".carousel-item img::attr(src)").getall()

        address = response.css(
            ".tab-pane[id='tabs-2'] .row:contains('ddress') .col::text").get()
        longitude, latitude = '', ''
        zipcode, city, addres = '', '', ''
        try:
            longitude, latitude = extract_location_from_address(address)
            zipcode, city, addres = extract_location_from_coordinates(
                longitude, latitude)
        except:
            pass

        property_type = 'apartment' if 'appartment' in description.lower(
        ) or 'appartment' in title.lower() or 'condo' in title.lower() or 'apartment' in response.css(".tab-pane[id='tabs-2'] .row:contains('ype') .col::text").get().lower() else 'house'
        
        external_id = response.url.split('/')[-1]
        if int(rent) > 0 and int(rent) < 20000:
            item_loader = ListingLoader(response=response)

            # # MetaData
            item_loader.add_value("external_link", response.url)  # String
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
            # item_loader.add_value("square_meters", square_meters)  # Int
            item_loader.add_value("room_count", room_count)  # Int
            item_loader.add_value("bathroom_count", bathroom_count)  # Int

            #item_loader.add_value("available_date", available_date)

            self.get_features_from_description(
                description+" ".join(response.css(".dd2-all-features li *::text").getall()), response, item_loader)

            # # Images
            item_loader.add_value("images", images)  # Array
            item_loader.add_value(
                "external_images_count", len(images))  # Int
            # item_loader.add_value("floor_plan_images", floor_plan_images)  # Array

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
                "landlord_name", 'Krown property managment')  # String
            item_loader.add_value(
                "landlord_phone", '709-738-6474')  # String
            # item_loader.add_value("landlord_email", landlord_email)  # String

            self.position += 1
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
