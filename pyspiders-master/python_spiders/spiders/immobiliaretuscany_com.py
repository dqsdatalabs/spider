# -*- coding: utf-8 -*-
# Author: Abanoub Moris
import scrapy
from scrapy.http.request import Request
from ..loaders import ListingLoader
from ..helper import *
import re
import json


class ImmobiliaretuscanySpider(scrapy.Spider):

    name = "immobiliaretuscany"
    start_urls = [
        'https://www.immobiliaretuscany.com/it/immobili?contratto%5B0%5D=2&contratto%5B1%5D=5']
    country = 'italy'  # Fill in the Country's name
    locale = 'it'  # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'

    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        for url in self.start_urls:
            yield Request(url,
                callback=self.parseApartment,
                dont_filter=True)

    # 2. SCRAPING level 2

    def parse(self, response):
        pass

    def parseApartment(self, response,):

        apartments = response.css('.listing-content')
        for apartment in apartments:
            title = apartment.css('h4 a::attr(title)').get()
            url = 'https://www.immobiliaretuscany.com' + \
                apartment.css('h4 a::attr(href)').get()
            external_id = apartment.css("a:contains(Rif)::text").get().replace(
                'Rif: ', '').replace('R', '').replace('/', "")
            rent = apartment.css(".listing-price::text").get()
            rent = re.search(
                r'\d+', rent.replace(',00', '').replace('.', ''))[0]

            datausage = {
                'title': title,
                'rent': rent,
                'external_id': external_id
            }

            yield Request(url, meta=datausage, callback=self.populate_item, dont_filter=True)

    # 3. SCRAPING level 3

    def populate_item(self, response):

        title = response.meta['title']
        rent = response.meta['rent']
        external_id = response.meta['external_id']

        utilities = response.css(".property-features li:contains(Spese) span::text").get()
        if utilities:
            utilities = re.search(r'\d+',utilities.replace(',00','').replace('.',''))[0]

        '''if 'appartament' in title.lower():
            property_type = 'apartment'
        else:
            property_type = 'house'''

        description = remove_white_spaces(
            "".join(response.css('.property-description div::text').getall()))

        room_count = response.css(
            ".property-features li:contains(Locali) span::text").get()
        bathroom_count = response.css(
            ".property-main-features li:contains(Bagni) span::text").get()
        square_meters = response.css(
            ".property-main-features li:contains(Superficie) span::text").get()
        if square_meters:
            square_meters = re.search(r'\d+', square_meters)[0]
        else:
            square_meters = ''

        floor = remove_unicode_char(response.css(".property-features li:contains(Piano) span::text").get())[0]
        energy_label = response.css(".property-features li:contains(energ) span::text").get()

        floor_plan_images =response.css('#detail-media .slick-list a::attr(href)').getall()

        images = response.css('.gallery-images')[0].css('img::attr(src)').getall()

        
      ######################################

        city = response.css(
            ".details tr:contains('Localit') td:nth-child(2) a::text").get()
        '''latitude = response.css('.init-map.clearfix::attr(data-lat)').get()
        longitude = response.css('.init-map.clearfix::attr(data-lng)').get()
        zipcode, city, address = extract_location_from_coordinates(
            longitude, latitude)'''

        #available_date = re.search(r'Disponibile da ([\w]+ \d+)', description)
        # if available_date:
        #    available_date = available_date.groups()[0]

        ######################################
        

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
            item_loader.add_value("city", 'Siena')  # String
            # item_loader.add_value("zipcode", zipcode)  # String
            # item_loader.add_value("address", address)  # String
            # item_loader.add_value("latitude", latitude)  # String
            # item_loader.add_value("longitude", longitude)  # String
            item_loader.add_value("floor", floor)  # String
            item_loader.add_value("property_type", 'apartment')  # String
            item_loader.add_value("square_meters", square_meters)  # Int
            item_loader.add_value("room_count", room_count)  # Int
            item_loader.add_value("bathroom_count", bathroom_count)  # Int

            # String => date_format also "Available", "Available Now" ARE allowed
            #item_loader.add_value("available_date", available_date)

            self.get_features_from_description(
                description, response, item_loader)

            # # Images
            item_loader.add_value("images", images)  # Array
            item_loader.add_value("external_images_count", len(images))  # Int
            item_loader.add_value("floor_plan_images", floor_plan_images) # Array

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
                "landlord_name", 'Immobiliaretuscany')  # String
            item_loader.add_value(
                "landlord_phone", '3481552675')  # String
            item_loader.add_value(
                "landlord_email", 'info@immobiliaretuscany.com')  # String

            self.position += 1
            yield item_loader.load_item()

    def get_features_from_description(self, description, response, item_loader):
        description = description.lower()
        pets_allowed = 'NULLVALUE' in description or True if response.css(
            ".details tr:contains(NULLVALUE) td:nth-child(2):contains('si')") else False
        furnished = 'arredato' in description and 'non arredato' not in description or True if response.css(
            ".property-features li:contains(rredato) span:contains(rredato)") else False
        parking = 'NULLVALUE' in description or True if response.css(
            ".details tr:contains('osto auto') td:nth-child(2):contains('si')") else False
        elevator = 'Ascensore' in description or True if response.css(
            ".property-features li:contains(Ascensore) span:contains(s)") else False
        balcony = 'balcon' in description or True if response.css(
            ".details tr:contains(NULLVALUE) td:nth-child(2):contains('si')") else False
        terrace = 'terrazz' in description or True if response.css(
            ".details tr:contains(errazz) td:nth-child(2):contains('si')") else False
        swimming_pool = 'NULLVALUE' in description or True if response.css(
            ".details tr:contains(NULLVALUE) td:nth-child(2):contains('si')") else False
        washing_machine = 'NULLVALUE' in description or True if response.css(
            ".details tr:contains(NULLVALUE) td:nth-child(2):contains('si')") else False
        dishwasher = 'NULLVALUE' in description or True if response.css(
            ".details tr:contains(NULLVALUE) td:nth-child(2):contains('si')") else False

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
