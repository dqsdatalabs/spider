# -*- coding: utf-8 -*-
# Author: Abanoub Moris
import scrapy
from scrapy.http.request import Request
from ..loaders import ListingLoader
from ..helper import *
import re
import json


class LakesideimmobiliareSpider(scrapy.Spider):

    name = "lakesideimmobiliare"
    start_urls = [
        'https://www.lakesideimmobiliare.com/it/proprieta/?area=0&category=0&rooms=0&price-min=0&price-max=0&order=desc&contract%5B%5D=2&terms=']
    country = 'italy'  # Fill in the Country's name
    locale = 'it'  # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'

    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        yield Request(self.start_urls[0], callback=self.parseApartment, dont_filter=True)

    # 2. SCRAPING level 2

    def parse(self, response):
        pass

    def parseApartment(self, response,):

        apartments = response.css('a.overlay-link::attr(href)').getall()
        for apartment in apartments:
            url = 'https://www.lakesideimmobiliare.com'+apartment

            yield Request(url, callback=self.populate_item, dont_filter=True)

    # 3. SCRAPING level 3

    def populate_item(self, response):

        title = response.css('.info-row-content h2::text').getall()[0]
        rent = response.css('.info-row-content h2::text').getall()[1]

        if 'appartament' in title.lower():
            property_type = 'apartment'
        else:
            property_type = 'house'

        rent = re.findall(r'\d+', rent.replace('.', ''))[-1]

        description = remove_white_spaces("".join(response.css(
            ".description .ellipsis-text p *::text").getall()))

        external_id = response.css(
            ".details__row.clearfix .box:contains('ID') .main::text").get()
        room_count = response.css(
            ".details__row.clearfix .box:contains('CAMERE') .main::text").get()
        bathroom_count = response.css(
            ".details__row.clearfix .box:contains('BAGNI') .main::text").get()
        square_meters = response.css(
            ".details__row.clearfix .box:contains('Area') .main::text").get()

        latitude = response.css('.init-map.clearfix::attr(data-lat)').get()
        longitude = response.css('.init-map.clearfix::attr(data-lng)').get()
        zipcode, city, address = extract_location_from_coordinates(
            longitude, latitude)

        available_date = re.search(r'Disponibile da ([\w]+ \d+)', description)
        if available_date:
            available_date = available_date.groups()[0]
        energy_label = re.search(r'Classe energetica ([\w])', description)
        if energy_label:
            energy_label = energy_label.groups()[0]
        if 'VA' in energy_label:
            energy_label = ''

        images = response.css('.item img::attr(src)').getall()
        images = ['https://www.lakesideimmobiliare.com'+x for x in images]

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
            item_loader.add_value("latitude", latitude)  # String
            item_loader.add_value("longitude", longitude)  # String
            # item_loader.add_value("floor", floor) # String
            item_loader.add_value("property_type", property_type)  # String
            item_loader.add_value("square_meters", square_meters)  # Int
            item_loader.add_value("room_count", room_count)  # Int
            item_loader.add_value("bathroom_count", bathroom_count)  # Int

            # String => date_format also "Available", "Available Now" ARE allowed
            item_loader.add_value("available_date", available_date)

            self.get_features_from_description(description, item_loader)

            # # Images
            item_loader.add_value("images", images)  # Array
            item_loader.add_value("external_images_count", len(images))  # Int
            # item_loader.add_value("floor_plan_images", floor_plan_images) # Array

            # # Monetary Status
            item_loader.add_value("rent", rent)  # Int
            # item_loader.add_value("deposit", deposit) # Int
            # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
            # item_loader.add_value("utilities", utilities) # Int
            item_loader.add_value("currency", "EUR")  # String

            # item_loader.add_value("water_cost", water_cost) # Int
            # item_loader.add_value("heating_cost", heating_cost) # Int

            item_loader.add_value("energy_label", energy_label)  # String

            # # LandLord Details
            item_loader.add_value(
                "landlord_name", 'Iscriviti alla nostra Newsletter')  # String
            item_loader.add_value(
                "landlord_phone", '(+39) 031 447 4059')  # String
            item_loader.add_value(
                "landlord_email", 'argegno@lakesideimmobiliare.com')  # String

            self.position += 1
            yield item_loader.load_item()

    def get_features_from_description(self, description, item_loader):
        description = description.lower()
        pets_allowed = 'NULLVALUE' in description
        furnished = 'arredato' in description and 'non arredato' not in description
        parking = 'NULLVALUE' in description
        elevator = 'ascensore' in description
        balcony = 'balcon' in description
        terrace = 'terrazz' in description
        swimming_pool = 'NULLVALUE' in description
        washing_machine = 'NULLVALUE' in description
        dishwasher = 'NULLVALUE' in description

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

    def get_property_type(self, property_type, description):

        if property_type and ('appartamento' in property_type.lower() or 'appartamento' in description.lower()):
            property_type = 'apartment'
        elif property_type and 'ufficio' in property_type.lower():
            property_type = ""
        else:
            if not property_type:
                property_type = ''
            else:
                property_type = 'house'
        return property_type
