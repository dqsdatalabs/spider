# -*- coding: utf-8 -*-
# Author: Fill with the developer's Name
import re

import scrapy
from ..loaders import ListingLoader
from ..helper import *


class TrialogGmbhComSpider(scrapy.Spider):
    name = "trialog_gmbh_com"
    start_urls = [
        'https://www.trialog-gmbh.com/wohnungen.xhtml?pagetype=object&f[2084-134]=ind_Schl_2544&f[2084-2]=0&f[2084-4]=0&f[2084-6]=miete&f[2084-8]=wohnung&f[2084-126]=miete&f[2084-138]=ind_Schl_2544&p[obj0]=1']
    allowed_domains = ["trialog-gmbh.com"]
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
        pages = ['https://www.trialog-gmbh.com/' + i for i in
                 list(set(response.css('.objlist-jumpbox span a::attr(href)').extract()))]

        for i in range(1, 6):
            url = response.url[:-1]+str(i)
            yield scrapy.Request(url, callback=self.parse_page)

    # 3. SCRAPING level 3
    def parse_page(self, response, **kwargs):
        urls = ['https://www.trialog-gmbh.com/' + i for i in response.css('.objlistitem-title::attr(href)').extract()][4:]
        state = response.xpath('/html/body/div[1]/div[1]/div[3]/div[3]/div[*]/div[1]/a/ul/li/span').extract()
        for i in range(len(state)):
            if 'status-reserved' not in state[i]:
                yield scrapy.Request(urls[i], callback=self.populate_item)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        # Title
        title = response.css('.obj-subtitle::text').get().strip()

        # Description
        description = re.sub(r' +', ' ', re.sub(r'[\n\r]', '', ' '.join(response.xpath('//*[@id="tab-beschreibung"]/p[1]/text()').extract())))

        # Others
        others = re.sub(r' +', ' ', re.sub(r'[\n\r]', '', ' '.join(response.css('p::text').extract())))

        # parking, elevator, balcony, terrace
        balcony = None
        if re.search('balkone', others.lower()):
            balcony = True

        terrace = None
        if re.search('terrasse', others.lower()):
            terrace = True

        parking = None
        if re.search('garage', others.lower()):
            parking = True

        elevator = None
        if re.search('personenaufzug', others.lower()):
            elevator = True

        washing_machine = None
        if re.search('wasch', others.lower()):
            washing_machine = True

        pets_allowed = None
        if re.search('haustier', others.lower()):
            pets_allowed = True


        # Images
        raw_images = response.xpath('//li[*]/a/@href').extract()
        images = []
        for i in raw_images:
            if 'https://' in i:
                images.append(i)

        # details
        labels_vals = dict(
            zip(response.css('.grid-5 strong::text').extract(), response.css('.grid-5 span::text').extract()))

        # property_type, zipcode, city, address, floor, square_meters, room_count, rent
        property_type = labels_vals['Property class'].lower()
        zipcode = labels_vals['ZIP code']
        city = labels_vals['Town']
        street = labels_vals['Street']
        floor = labels_vals['ZIP code']
        square_meters = int(float(labels_vals['Living area'].split()[0].replace('.', '').replace(',', '.')))
        room_count = int(float(labels_vals['Number of rooms']))
        rent = int(float(labels_vals['Basic rent'].split()[0].replace('.', '').replace(',', '.')))
        longitude, latitude = extract_location_from_address(f'{zipcode} {street}')
        _, _, address = extract_location_from_coordinates(longitude, latitude)

        # External_id
        external_id = response.url.split('=')[1]

        # energy_label
        energy_details = response.css('p+ p::text').extract()
        energy_label = None
        for i in energy_details:
            if 'Class:' in i:
                energy_label = energy_details[-1].split()[1]

        # Remove phone, websites, emails
        description = re.sub(r'(\d{3}[-\.\s]??\d{3}[-\.\s]??\d{4}|\(\d{3}\)\s*\d{3}[-\.\s]??\d{4}|\d{3}[-\.\s]??\d{4})',
                             '', description)
        description = re.sub(
            r'[\S]+\.(net|com|org|info|edu|gov|uk|de|ca|jp|fr|au|us|ru|ch|it|nel|se|no|es|mil)[\S]*\s?', '',
            description)

        description = re.sub(r'[\w.+-]+@[\w-]+\.[\w.-]+', '', description)

        # Landlord_info
        landlord_name = response.css('strong .data::text').get()
        landlord_number = response.css('.icon-phone+ .data::text').get()
        landlord_email = response.css('.data a::text').get()

        # # MetaData
        item_loader.add_value("external_link", response.url)  # String
        item_loader.add_value("external_source", self.external_source)  # String

        item_loader.add_value("external_id", external_id) # String
        item_loader.add_value("position", self.position)  # Int
        item_loader.add_value("title", title) # String
        item_loader.add_value("description", description) # String

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

        item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        # item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking) # Boolean
        item_loader.add_value("elevator", elevator) # Boolean
        item_loader.add_value("balcony", balcony) # Boolean
        item_loader.add_value("terrace", terrace) # Boolean
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
        # item_loader.add_value("heating_cost", heating_cost) # Int

        item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name) # String
        item_loader.add_value("landlord_phone", landlord_number) # String
        item_loader.add_value("landlord_email", landlord_email) # String

        self.position += 1
        yield item_loader.load_item()
