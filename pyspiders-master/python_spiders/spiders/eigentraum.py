# -*- coding: utf-8 -*-
# Author: Ahmed Shahien
import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import requests
from ..helper import *
import datetime
import dateparser
from bs4 import BeautifulSoup

class EigentraumeSpider(scrapy.Spider):
    name = "eigentraum"
    start_urls = ['https://www.eigentraum.de/mieten/']
    allowed_domains = ["eigentraum.de"]
    country = 'Germany' # Fill in the Country's name
    locale = 'de' # Fill in the Country's locale, look up the docs if unsure
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
        property_urls = response.css('.property a::attr(href)').extract()
        property_urls = set(property_urls)
        for property_url in property_urls:
            yield Request(url=property_url, callback=self.populate_item)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        list = response.css('.list-group-item ::text').extract()
        rent = response.css('.formatBold .col-xs-7::text')[0].extract()
        deposit = None
        if 'Kaution' in list:
            deposit_index = [i for i, x in enumerate(list) if "Kaution" in x][0]
            deposit = list[deposit_index + 2]
            deposit = int(''.join(x for x in deposit if x.isdigit()))
        utilities = None
        heat_index = [i for i, x in enumerate(list) if "Warmmiete" in x][0]
        heating_cost = list[heat_index + 2]
        heating_cost = int(''.join(x for x in heating_cost if x.isdigit()))
        if 'Heizkosten' in rent:
            rent_index = [i for i, x in enumerate(list) if "Kaltmiete" in x][0]
            rent = list[rent_index + 2]
        if 'Nebenkosten' in list:
            util_index = [i for i, x in enumerate(list) if "Nebenkosten" in x][0]
            utilities = list[util_index + 2]
            utilities = int(''.join(x for x in utilities if x.isdigit()))
        if any(char.isdigit() for char in rent):
            rent = int(''.join(x for x in rent if x.isdigit()))
        else:
            return
        if utilities:
            heating_cost = heating_cost - (rent + utilities)
        else:
            heating_cost = None


        title = response.css('.property-title::text')[0].extract()
        external_id = response.css('.get-hidden-field-objekt-id::text')[0].extract()
        property_type = response.css('.list-group:nth-child(1) .list-group-item:nth-child(2) .col-xs-7::text')[0].extract()
        if 'Wohnung' in property_type:
            property_type = 'apartment'
        address = response.css('.list-group:nth-child(1) .list-group-item:nth-child(3) .col-xs-7 ::text').extract()
        address = ' '.join(address)

        square_index = [i for i, x in enumerate(list) if "Wohnfläche\xa0ca." in x][0]
        square_meters = list[square_index + 2]
        square_meters = int(square_meters[:-2])
        room_index = [i for i, x in enumerate(list) if "Zimmer" in x][0]
        room_count = int(list[room_index + 2])
        bathroom_count = None
        try:
            bath_index = [i for i, x in enumerate(list) if "Badezimmer" in x][0]
            bathroom_count = int(list[bath_index + 2])
        except:
            pass
        available_date = None
        try:
            date_index = [i for i, x in enumerate(list) if "Verfügbar ab" in x][0]
            available_date = list[date_index + 2]
            available_date = available_date.strip()
            available_date = dateparser.parse(available_date)
            available_date = available_date.strftime("%Y-%m-%d")
        except:
            pass
        energy_label = None
        try:
            energy_index = [i for i, x in enumerate(list) if "Energie­effizienz­klasse" in x][0]
            energy_label = list[energy_index + 2]
        except:
            pass

        list = ''.join(list)
        balcony = None
        if 'balkon' in list.lower():
            balcony = True
        terrace = None
        if 'terrass' in list.lower():
            terrace = True
        parking = None
        if 'stellplatzmiete' in list.lower() or 'garage' in list.lower():
            parking = True

        description = response.css('p::text').extract()
        description = ' '.join(description)
        if 'eigentraum' in description:
            info = description.split('eigentraum GmbH & Co. KG')[1].split('Ihr Team der eigentraum GmbH & Co. KG')[0]
            description = description.replace(info, "")

        elevator = None
        if 'aufzug' in description.lower():
            elevator = True
        washing_machine = None
        if 'wasch' in description.lower():
            washing_machine = True

        images = response.css('[id=immomakler-galleria] a::attr(href)').extract()
        landlord_name = response.css('.fn ::text')[0].extract()
        landlord_info = response.css('.desc a ::text').extract()
        landlord_phone = landlord_info[0]
        landlord_email = landlord_info[1]

        soup = BeautifulSoup(response.text, 'html.parser')
        latitude = soup.find("meta", property="place:location:latitude")
        if latitude:
            latitude = latitude["content"]

        longitude = soup.find("meta", property="place:location:longitude")
        if longitude:
            longitude = longitude["content"]

        if not latitude and not longitude:
            longitude, latitude = extract_location_from_address(address)
            longitude = str(longitude)
            latitude = str(latitude)

        zipcode, city, address = extract_location_from_coordinates(longitude,latitude)

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
        item_loader.add_value("latitude", latitude) # String
        item_loader.add_value("longitude", longitude) # String
        #item_loader.add_value("floor", floor) # String
        item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters) # Int
        item_loader.add_value("room_count", room_count) # Int
        item_loader.add_value("bathroom_count", bathroom_count) # Int

        item_loader.add_value("available_date", available_date) # String => date_format

        #item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        #item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking) # Boolean
        item_loader.add_value("elevator", elevator) # Boolean
        item_loader.add_value("balcony", balcony) # Boolean
        item_loader.add_value("terrace", terrace) # Boolean
        #item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        item_loader.add_value("washing_machine", washing_machine) # Boolean
        #item_loader.add_value("dishwasher", dishwasher) # Boolean

        # # Images
        item_loader.add_value("images", images) # Array
        item_loader.add_value("external_images_count", len(images)) # Int
        #item_loader.add_value("floor_plan_images", floor_plan_images) # Array

        # # Monetary Status
        item_loader.add_value("rent", rent) # Int
        item_loader.add_value("deposit", deposit) # Int
        #item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", "EUR") # String

        #item_loader.add_value("water_cost", water_cost) # Int
        item_loader.add_value("heating_cost", heating_cost) # Int

        item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name) # String
        item_loader.add_value("landlord_phone", landlord_phone) # String
        item_loader.add_value("landlord_email", landlord_email) # String

        self.position += 1
        yield item_loader.load_item()
