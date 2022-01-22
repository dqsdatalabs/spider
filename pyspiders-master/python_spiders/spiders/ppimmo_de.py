# -*- coding: utf-8 -*-
# Author: Fill with the developer's Name
import scrapy
from ..loaders import ListingLoader
from parsel import Selector
from ..helper import *


class PpimmoDeSpider(scrapy.Spider):
    name = 'ppimmo_de'
    start_urls = ['https://www.ppimmo.de/immobilienangebote/vermietung-wohnen/']
    allowed_domains = ['ppimmo.de']
    country = 'germany'
    locale = 'de'
    external_source = '{}_PySpider_{}_{}'.format(name.capitalize(), country, locale)
    execution_type = 'testing'
    position = 1

    def start_requests(self):
        for i in (1, 2, 3):
            url = self.start_urls[0] + 'page/' + str(i) + '/#immobilien'
            yield scrapy.Request(url, callback=(self.parse))

    def parse(self, response, **kwargs):
        for url in response.css('.h3::attr(href)').extract():
            yield scrapy.Request(url, callback=(self.populate_item))

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        if 'SONSTIGE'.lower() in response.css('.badge-secondary::text').get().lower():
            return

        #landlord_info
        landlord_name = response.css('p.text-primary::text').get()
        landlord_number = response.css('.text-primary+ p::text').get().strip()
        landlord_email = response.css('.btn-primary::attr(href)').get().split(': ')[1]

        # Title, external_id, rent
        property_type = 'apartment'
        title = response.css('h1::text').get()
        external_id = response.css('.lh-large::text').get().strip().split(': ')[1]
        rent = int(float(response.css('.font-weight-semibold::text').get().split('\xa0')[0].replace('.', '').replace(',', '.')))

        # disable javascript, search for any word, look for the class name, select it with parsel.Selector
        # and use it as response
        text = response.css('.vue-tabs::text').get()
        s = Selector(text)

        # longitude, latitude, zipcode, city, address
        loc = s.css('p::text').getall()[-1]
        longitude, latitude = extract_location_from_address(loc)
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)

        # description
        energy_label = None
        text = response.css('.vue-tabs::text').extract()[1]
        s = Selector(text)
        description = ' '.join(s.css('p::text').getall()[:-3])
        if 'Energieeffizienzklasse' in description:
            energy_label = description.split('Energieeffizienzklasse: ')[1][0:2].strip()

        description = description_cleaner(description)

        # info
        text = response.css('.vue-tabs::text').extract()[0]
        s = Selector(text)
        info = dict(zip([i.strip() for i in s.css('.key::text').getall() if i not in ['Heizkosten in NK. enthalten:', 'WBS erforderlich:']], [i.strip() for i in s.css('.value::text').getall()]))
        room_count = bathroom_count = 1
        floor = square_meters = deposit = heating_cost = available_date = None

        for i in info.keys():
            if 'Zimmer' in i:
                room_count = int(float(info[i].replace(',', '.')))
            if 'Anzahl Badezimmer:' in i:
                bathroom_count = int(float(info[i].replace(',', '.')))
            if 'Lage' in i and 'Etage' in i:
                floor = info[i]
            if 'Wohnfläche' in i:
                square_meters = int(float(info[i].split('\xa0')[0].replace('.', '').replace(',', '.')))
            if 'Kaution' in i:
                deposit = int(float(info[i].split('\xa0')[0].replace('.', '').replace(',', '.')))
            if 'Warmmiete' in i:
                heating_cost = int(float(info[i].split('\xa0')[0].replace('.', '').replace(',', '.'))) - rent
            if 'verfügbar ab' in i:
                if '.' in info[i]:
                    available_date = '-'.join(info[i].split('.')[::-1])
            if 'Energieeffizienzklasse' in i:
                if len(info[i])< 2:
                    energy_label = info[i]

        # details
        text = response.css('.vue-tabs::text').extract()[0]
        s = Selector(text)
        not_included = s.css('.no::text').getall()
        details = [i for i in [i for i in s.css('li::text').getall() if '\n' not in i] if i not in not_included]

        if 'Vermietet' in details:
            return
        pets_allowed = furnished = parking = elevator = balcony = terrace = swimming_pool = washing_machine = dishwasher = None
        pets_allowed, furnished, parking, elevator, balcony, terrace, swimming_pool, washing_machine, dishwasher = get_amenities(
            description, ' '.join(details) + ' '.join(info.keys()), item_loader)

        # images
        text = response.css('.vue-immo-expose::text').extract()[0]
        images = [i.split('"src":')[1].replace('\\','').replace('"','') for i in text.split("[{")[1].split('}]')[0].split(',') if '"src":' in i]



        if 'Haustiere erlaubt' in not_included:
            pets_allowed = False

        # Enforces rent between 0 and 40,000 please dont delete these lines
        if 0 >= int(rent) > 40000:
            return

        # # MetaData
        item_loader.add_value("external_link", response.url) # String
        item_loader.add_value("external_source", self.external_source) # String

        item_loader.add_value("external_id", external_id) # String
        item_loader.add_value("position", self.position) # Int
        item_loader.add_value("title", title) # String
        item_loader.add_value("description", description) # String

        # # Property Details
        item_loader.add_value("city", city) # String
        item_loader.add_value("zipcode", zipcode) # String
        item_loader.add_value("address", address) # String
        item_loader.add_value("latitude", str(latitude)) # String
        item_loader.add_value("longitude", str(longitude)) # String
        item_loader.add_value("floor", floor) # String
        item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters) # Int
        item_loader.add_value("room_count", room_count) # Int
        item_loader.add_value("bathroom_count", bathroom_count) # Int

        item_loader.add_value("available_date", available_date) # String => date_format

        item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking) # Boolean
        item_loader.add_value("elevator", elevator) # Boolean
        item_loader.add_value("balcony", balcony) # Boolean
        item_loader.add_value("terrace", terrace) # Boolean
        item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        item_loader.add_value("washing_machine", washing_machine) # Boolean
        item_loader.add_value("dishwasher", dishwasher) # Boolean

        # # Images
        item_loader.add_value("images", images) # Array
        item_loader.add_value("external_images_count", len(images)) # Int
        # item_loader.add_value("floor_plan_images", floor_plan_images) # Array

        # # Monetary Status
        item_loader.add_value("rent", rent) # Int
        item_loader.add_value("deposit", deposit) # Int
        #item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        # item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", "EUR") # String

        #item_loader.add_value("water_cost", water_cost) # Int
        item_loader.add_value("heating_cost", heating_cost) # Int

        item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name) # String
        item_loader.add_value("landlord_phone", landlord_number) # String
        item_loader.add_value("landlord_email", landlord_email) # String

        self.position += 1
        yield item_loader.load_item()
