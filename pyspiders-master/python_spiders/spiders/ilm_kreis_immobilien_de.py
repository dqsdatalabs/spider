# -*- coding: utf-8 -*-
# Author: Fill with the developer's Name
import re

import scrapy
from ..loaders import ListingLoader
from ..helper import *


class AndreaFreistImmobilienDeSpider(scrapy.Spider):
    name = "andrea_freist_immobilien_de"
    start_urls = ['https://andrea-freist-immobilien.de/ff/immobilien']
    allowed_domains = ["andrea-freist-immobilien.de"]
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
        all_urls = response.xpath('//*[@id="ff-default"]/div[1]/article[*]/a/@href').extract()
        for url in all_urls:
            yield scrapy.Request(url, callback=self.populate_item)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        # Details
        labels = response.css('.FFestateview-default-details-content-details span:nth-child(1)::text').extract()
        vals = response.xpath('//*[@id="ff-default"]/div[2]/div/div/div[1]/ul/li[*]/span[2]/text()').extract()
        values = []
        for i in vals:
            text = re.sub(r'[\n\t]', '', i).strip()
            if text:
                values.append(text)

        # Check if the property for purchase
        if 'Miete zzgl. NK' not in labels:
            return

        val_labels = dict(zip(labels, values))

        # Rent, heating_cost
        rent = None
        if 'Miete zzgl. NK' in val_labels.keys():
            rent = int(val_labels['Miete zzgl. NK'].split(',')[0])

        heating_cost = None
        if 'Nebenkosten' in val_labels.keys():
            heating_cost = int(val_labels['Nebenkosten'].split(',')[0])

        # Property type
        german_names = {
            'Etagenwohnung': 'apartment',
            'Dachgeschosswohnung': 'apartment',
            'Reihenmittelhaus': 'house',
            'Wohngrundstück': 'house',
            'Einfamilienhaus': 'house',
            'Doppelhaushälfte': 'house',
            'Penthousewohnung': 'house',
            'Zweifamilienhaus': 'house',
            'Wohnung': 'apartment',
            'Haus': 'house'
        }
        property_type = None
        if 'Art' in val_labels.keys():
            if val_labels['Art'] in german_names.keys():
                property_type = german_names[val_labels['Art']]

        # external_id, room_count, square_meters, floor, location
        external_id = None
        if 'Immobilien-ID' in val_labels.keys():
            external_id = val_labels['Immobilien-ID']

        room_count = 1
        if 'Zimmer' in val_labels.keys():
            room_count = int(float(val_labels['Zimmer']))

        square_meters = None
        if 'Wohnfläche' in val_labels.keys():
            square_meters = int(float(val_labels['Wohnfläche']))

        floor = None
        floor_names = {'first': '1',
                       '2nd': '2',
                       'third': '3',
                       '3rd': '3',
                       'forth': '4',
                       '4th': '4',
                       '5th': '5'
                       }
        if 'Etagen' in val_labels.keys():
            if val_labels['Etagen'] in floor_names.keys():
                floor = floor_names[val_labels['Etagen']]
            else:
                floor = val_labels['Etagen']

        zipcode = city = address = longitude = latitude = None
        if 'Lage' in val_labels.keys():
            longitude, latitude = extract_location_from_address(val_labels['Lage'])
            zipcode, city, address = extract_location_from_coordinates(longitude, latitude)

        # images
        raw_images = response.css('img::attr(data-lazy)').extract()
        images = []
        for i in raw_images:
            if 'width/100' in i:
                continue
            else:
                images.append(i)

        # landlord info
        landlord_name = 'Ilm-Kreis-Immobilien'
        landlord_number = response.css('a:nth-child(1) .elementor-icon-list-text::text').get()
        landlord_email = response.css('.elementor-clearfix a::text').get()
        landlord_name = response.css('.FFestateview-default-details-agent-name span::text').get()

        # Title
        title = response.xpath('/html/head/title/text()').get()

        # energy label
        energy_label = re.sub(r'[\n\t]', '', response.css(
            '.FFestateview-default-details-content-energyUsage li:nth-child(3) span+ span::text').get())

        # Description
        description = re.sub(r'[\n\t]', '', ' '.join(response.css(
            '.FFestateview-default-details-content-blank+ .FFestateview-default-details-content-description p::text').extract()))

        # Furnished, elevator
        heads = response.css('h3::text').extract()
        furnished = None
        elevator = None
        if 'Ausstattung' in heads:
            furnished = True
            furnitures = response.css('.ja::text').extract()
            if 'Aufzug ' in furnitures:
                elevator = True

        # Clean description, title
        description = re.sub(r"[_,.*+(){}';@#?!&$/-]+\ *", " ", description)
        description = re.sub(' +', ' ', description)
        title = re.sub(r"[_,.*+(){};@#?!&$/-]+\ *", " ", title)
        title = re.sub(' +', ' ', title)

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
        item_loader.add_value("latitude", str(latitude))  # String
        item_loader.add_value("longitude", str(longitude))  # String
        item_loader.add_value("floor", floor)  # String
        item_loader.add_value("property_type",
                              property_type)  # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters)  # Int
        item_loader.add_value("room_count", room_count)  # Int
        # item_loader.add_value("bathroom_count", bathroom_count) # Int

        # item_loader.add_value("available_date", available_date) # String => date_format

        # item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        item_loader.add_value("furnished", furnished) # Boolean
        # item_loader.add_value("parking", parking) # Boolean
        item_loader.add_value("elevator", elevator) # Boolean
        # item_loader.add_value("balcony", balcony) # Boolean
        # item_loader.add_value("terrace", terrace) # Boolean
        # item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        # item_loader.add_value("washing_machine", washing_machine) # Boolean
        # item_loader.add_value("dishwasher", dishwasher) # Boolean

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
        item_loader.add_value("heating_cost", heating_cost)  # Int

        item_loader.add_value("energy_label", energy_label)  # String

        # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name)  # String
        item_loader.add_value("landlord_phone", landlord_number)  # String
        item_loader.add_value("landlord_email", landlord_email)  # String

        self.position += 1
        yield item_loader.load_item()
