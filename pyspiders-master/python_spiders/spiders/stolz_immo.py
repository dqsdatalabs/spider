# -*- coding: utf-8 -*-
# Author: Ahmed Shahien
import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import requests
from ..helper import *
import datetime
import dateparser


class StolzImmoSpider(scrapy.Spider):
    name = "stolz_immo"
    start_urls = ['https://www.stolz-immobilien.de/angebote/?mt=rent&address&sort=date%7Cdesc']
    allowed_domains = ["stolz-immobilien.de"]
    country = 'germany'  # Fill in the Country's name
    locale = 'de'  # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'

    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    # 2. SCRAPING level 2
    def parse(self, response, **kwargs):
        property_urls = response.css('#immobilien .mb-3 a::attr(href)').extract()
        banners = response.css('.immolisting__banner ').extract()
        for index, property_url in enumerate(property_urls):
            if 'vermietet' not in banners[index]:
                yield Request(url=property_url, callback=self.populate_item)
        try:
            next_page = response.css('a.next::attr(href)')[0].get()
            yield Request(url=next_page, callback=self.parse)
        except:
            pass

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        title = response.css('.is-style-subline::text')[0].extract()
        if 'Garagenplatz' in title or 'Tiefgaragenstellplatz' in title:
            return
        else:
            property_type = 'apartment'

        balcony = None
        if 'balkon' in title.lower():
            balcony = True

        address = response.css('.row:nth-child(2) .mb-0 li::text')[0].extract().strip()
        longitude, latitude = extract_location_from_address(address)
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)
        latitude = str(latitude)
        longitude = str(longitude)

        square_meters = response.css('.text-dark::text')[0].extract().strip()
        square_meters = int(square_meters[:-2])
        room_count = response.css('.text-dark::text')[1].extract().strip()
        room_count = int(room_count)

        description = response.css('.vue-tabs::attr(data-tabs)')[0].extract()
        description = ''.join(description)
        description = description.split(',"rawValue":"')[1].split('.","')[0]

        if 'balkon' in description.lower():
            balcony = True
        washing_machine = None
        if 'Wasch' in description:
            washing_machine = True

        rent = response.css('h2::text')[0].extract().strip()
        rent = int(rent[:-1].strip())
        list = response.css('.immo-expose__list-price--list span::text').extract()
        list = ' '.join(list)
        heating_cost = int(list.split('Nebenkosten ')[1].split(' €')[0])
        external_id = list.split('Objekt-Nr ')[1].split(' ')[0]
        available_date = list.split('verfügbar ab ')[1].split(' Unterkellert')[0]
        available_date = dateparser.parse(available_date)
        available_date = available_date.strftime("%Y-%m-%d")
        floor = None
        if 'Etage' in list:
            floor = list.split('Etage ')[1].split(' ')[0]
        elevator = None
        if 'Fahrstuhl' in list:
            elevator = True
        parking = None
        if 'Stellplatzmiete' in list:
            parking = True

        energy_label = response.css('.text-small li:nth-child(1) .value::text')[0].extract().strip()
        energy_label = int(energy_label[:3])
        if energy_label >= 250:
            energy_label = 'H'
        elif energy_label >= 225 and energy_label <= 250:
            energy_label = 'G'
        elif energy_label >= 160 and energy_label <= 175:
            energy_label = 'F'
        elif energy_label >= 125 and energy_label <= 160:
            energy_label = 'E'
        elif energy_label >= 100 and energy_label <= 125:
            energy_label = 'D'
        elif energy_label >= 75 and energy_label <= 100:
            energy_label = 'C'
        elif energy_label >= 50 and energy_label <= 75:
            energy_label = 'B'
        elif energy_label >= 25 and energy_label <= 50:
            energy_label = 'A'
        elif energy_label >= 1 and energy_label <= 25:
            energy_label = 'A+'

        images = response.css('.lightgallery a::attr(href)').extract()

        # # MetaData
        item_loader.add_value("external_link", response.url)  # String
        item_loader.add_value("external_source", self.external_source)  # String

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
        item_loader.add_value("property_type",
                              property_type)  # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters)  # Int
        item_loader.add_value("room_count", room_count)  # Int
        # item_loader.add_value("bathroom_count", bathroom_count) # Int

        item_loader.add_value("available_date", available_date)  # String => date_format

        # item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        # item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking)  # Boolean
        item_loader.add_value("elevator", elevator)  # Boolean
        item_loader.add_value("balcony", balcony)  # Boolean
        # item_loader.add_value("terrace", terrace) # Boolean
        # item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        item_loader.add_value("washing_machine", washing_machine) # Boolean
        # item_loader.add_value("dishwasher", dishwasher) # Boolean

        # # Images
        item_loader.add_value("images", images) # Array
        item_loader.add_value("external_images_count", len(images)) # Int
        # item_loader.add_value("floor_plan_images", floor_plan_images) # Array

        # # Monetary Status
        item_loader.add_value("rent", rent)  # Int
        # item_loader.add_value("deposit", deposit) # Int
        # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        # item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", "EUR")  # String

        # item_loader.add_value("water_cost", water_cost) # Int
        item_loader.add_value("heating_cost", heating_cost)  # Int

        item_loader.add_value("energy_label", energy_label)  # String

        # # LandLord Details
        item_loader.add_value("landlord_name", 'Stolz Immobilien e.K.') # String
        item_loader.add_value("landlord_phone", '07531 23459') # String
        item_loader.add_value("landlord_email", 'mailto:kontakt@stolz-immobilien.de') # String

        self.position += 1
        yield item_loader.load_item()
