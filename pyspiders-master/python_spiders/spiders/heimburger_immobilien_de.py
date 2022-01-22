# -*- coding: utf-8 -*-
# Author: Abanoub Moris
import scrapy
from scrapy.http.request import Request
from ..loaders import ListingLoader
from ..helper import *
import re
import json


class Heimburger_immobilienSpider(scrapy.Spider):

    name = "heimburger_immobilien"
    country = 'germany'  # Fill in the Country's name
    locale = 'de'  # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'

    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        url = f'https://www.heimburger-immobilien.de/de/mieten'

        yield Request(url,
                      callback=self.parseApartment,
                      dont_filter=True)

    # 2. SCRAPING level 2

    def parse(self, response):
        pass

    def parseApartment(self, response):

        apartments = response.css(".element.immobilie")
        for apartment in apartments:
            title = apartment.css(".left .subhead::text").get()

            url = apartment.css("::attr(onclick)").get().replace(
                'location.href=', '').replace('\'', '')
            rent = apartment.css(".left .headline span::text").get()
            try:
                rent = re.search(
                    r'\d+', rent.replace(',00', '').replace('.', ''))[0]
            except:
                continue

            datausage = {
                'title': title,
                'rent': rent,
            }

            yield Request(url, meta=datausage, callback=self.populate_item, dont_filter=True)

    # 3. SCRAPING level 3
    def populate_item(self, response):

        title = response.meta['title']
        rent = response.meta['rent']

        square_meters = response.css(
            ".data:contains('ohnfläche') .value::text").get().replace(' qm', '')
        room_count = response.css(".data:contains('immer') .value::text").get()

        room_count = response.css(
            ".data:contains('Zimmer') .value::text").get()
        bathroom_count = response.css(
            ".data:contains('adezimmer') .value::text").get()

        address = 'Ladenburger Straße 21 69120 Heidelberg'
        latlng = extract_location_from_address(address)
        latitude = str(latlng[1])
        longitude = str(latlng[0])
        zipcode, city, address = extract_location_from_coordinates(
            longitude, latitude)

        description = remove_white_spaces(
            "".join(response.css(".mid .text p::text").getall()))

        address = 'Ladenburger Straße 21 69120 Heidelberg'

        images = response.css('picture img::attr(data-src)').getall()
        floor_plan_images = response.css(
            '.grundrisse.slick-initialized.slick-slider picture img::attr(data-src)').getall()
        for img in floor_plan_images:
            images.remove(img)
        if int(rent) > 0 and int(rent) < 20000:
            item_loader = ListingLoader(response=response)

            # # MetaData
            item_loader.add_value("external_link", response.url)  # String
            item_loader.add_value(
                "external_source", self.external_source)  # String

            # item_loader.add_value("external_id", external_id)  # String
            item_loader.add_value("position", self.position)  # Int
            item_loader.add_value("title", title)  # String
            item_loader.add_value("description", description)  # String

            # # Property Details
            item_loader.add_value("city", city)  # String
            item_loader.add_value("zipcode", zipcode)  # String
            item_loader.add_value("address", address)  # String
            item_loader.add_value("latitude", latitude)  # String
            item_loader.add_value("longitude", longitude)  # String
            # item_loader.add_value("floor", floor)  # String
            item_loader.add_value("property_type", 'apartment')  # String
            item_loader.add_value("square_meters", square_meters)  # Int
            item_loader.add_value("room_count", room_count)  # Int
            item_loader.add_value("bathroom_count", bathroom_count)  # Int

            #item_loader.add_value("available_date", available_date)

            self.get_features_from_description(
                description+" ".join(response.css(".wpb_wrapper ul li::text,.wpb_wrapper ul li span::text").getall()), response, item_loader)

            # # Images
            item_loader.add_value("images", images)  # Array
            item_loader.add_value("external_images_count", len(images))  # Int
            item_loader.add_value("floor_plan_images",
                                  floor_plan_images)  # Array

            # # Monetary Status
            item_loader.add_value("rent", rent)  # Int
            # item_loader.add_value("deposit", deposit) # Int
            # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
            # item_loader.add_value("utilities", utilities)  # Int
            item_loader.add_value("currency", "EUR")  # String

            # item_loader.add_value("water_cost", water_cost) # Int
            # item_loader.add_value("heating_cost", heating_cost) # Int

            # item_loader.add_value("energy_label", energy_label)  # String

            # # LandLord Details
            item_loader.add_value(
                "landlord_name", 'Heimburger')  # String
            item_loader.add_value(
                "landlord_phone", '+49 (0) 6221 / 589 0 658')  # String
            item_loader.add_value(
                "landlord_email", '')  # String

            self.position += 1
            yield item_loader.load_item()

    Amenties = {
        'pets_allowed': ['pets'],
        'furnished': ['furnish', 'MÖBLIERTES'.lower()],
        'parking': ['parking', 'garage'],
        'elevator': ['elevator', 'aufzug'],
        'balcony': ['balcon', 'balkon'],
        'terrace': ['terrace'],
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
