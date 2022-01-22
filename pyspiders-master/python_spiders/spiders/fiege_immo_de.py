# -*- coding: utf-8 -*-
# Author: Abanoub Moris
import json
import scrapy
from urllib.parse import urlparse, urlunparse, parse_qs
from parsel import Selector
from scrapy.http.request import Request
from ..loaders import ListingLoader
from ..helper import *
import re


class FiegemmoSpider(scrapy.Spider):

    name = "fiege_immo"
    country = 'germany'  # Fill in the Country's name
    locale = 'de'  # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'
    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        url = f'https://fiege-immo.de/immobilien-nutzungsart/wohnen/'
        yield Request(url, callback=self.parse)

    # 2. SCRAPING level 2

    def parse(self, response):
        apartments = response.css(".property-title a::attr(href)").getall()
        for link in apartments:
            yield scrapy.Request(link, callback=self.populate_item)

    def populate_item(self, response):

        title = response.css(".property-title::text").get()
        floorList = "-".join(response.css(".list-group-item .row:contains(Etage) .dd::text").getall())
        floor=''
        if floorList:
            floor=re.search(r'\d+',floorList)[0]
      
        rent = response.css(".list-group-item .row:contains(Kaltmiete) .dd::text").get()
        if rent:
            rent = re.search(r'\d+',rent.replace('.',''))[0]

        deposit = response.css(".list-group-item .row:contains(Kaution) .dd::text").get()
        if deposit:
            deposit = re.search(r'\d+',deposit.replace('.',''))[0]

        utilities = response.css(".list-group-item .row:contains(Nebenkosten) .dd::text").get()
        if utilities:
            utilities = re.search(r'\d+',utilities.replace('.',''))[0]

        square_meters = response.css(".list-group-item .row:contains(Wohnfläche) .dd::text").get()
        if square_meters:
            square_meters=re.search(r'\d+',square_meters)[0]
        
        room_count = response.css(".list-group-item .row:contains(Zimmer) .dd::text").get()
        bathroom_count = response.css(".list-group-item .row:contains(Badezimmer) .dd::text").get()
        if room_count:
            room_count=re.search(r'\d+',room_count)[0]
        else:
            room_count='1'
        if room_count=='0':
            room_count='1'


        description= description_cleaner(" ".join(response.css(".panel-body p::text").getall()))

        images = response.css("#immomakler-galleria a::attr(href)").getall()

        ##################################**RETURN**###################################
        address =  response.css(".property-subtitle::text").get()
        longitude,latitude = extract_location_from_address(address)
        zipcode, city, addres = extract_location_from_coordinates(longitude,latitude)
        #############

        landlord_name='Ute Fiege'
        landlord_phone = '+49 6104 2220'
        landlord_email = 'info@fiege-immo.de'

        property_type = 'apartment' if 'wohnung' in description.lower() or 'wohnung' in " ".join(response.css(".list-group-item:contains('Objekttypen') .dd a::text").getall()).lower() else 'house'

        external_id = re.search(r'\d+',response.css(".list-group-item:contains('Objekt ID') .dd::text").get())[0]
       

        if int(rent) > 0 and int(rent) < 20000:
            item_loader = ListingLoader(response=response)

            # # MetaData
            item_loader.add_value("external_link", response.url)  # String
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
            item_loader.add_value("bathroom_count", bathroom_count)  # Int

            #item_loader.add_value("available_date", available_date)

            get_amenities(description, " ".join(response.css(".list-group .list-group-item::text").getall()), item_loader)

            # # Images
            item_loader.add_value("images", images)  # Array
            item_loader.add_value(
                "external_images_count", len(images))  # Int
            # item_loader.add_value("floor_plan_images", floor_plan_images)  # Array

            # # Monetary Status
            item_loader.add_value("rent", rent)  # Int
            item_loader.add_value("deposit", deposit)  # Int
            # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
            item_loader.add_value("utilities", utilities)  # Int
            item_loader.add_value("currency", "EUR")  # String

            # item_loader.add_value("water_cost", water_cost) # Int
            # item_loader.add_value("heating_cost", heating_cost) # Int
            # item_loader.add_value("energy_label", energy_label)  # String

            # # LandLord Details
            item_loader.add_value(
                "landlord_name", landlord_name)  # String
            item_loader.add_value(
                "landlord_phone", landlord_phone)  # String
            item_loader.add_value("landlord_email", landlord_email)  # String

            self.position += 1
            yield item_loader.load_item()
