# -*- coding: utf-8 -*-
# Author: Abanoub Moris

import json
import scrapy
from parsel import Selector
from scrapy.http.request import Request
from ..loaders import ListingLoader
from ..helper import *
import re


class LombergSpider(scrapy.Spider):

    name = "lomberg"
    country = 'germany'  # Fill in the Country's name
    locale = 'de'  # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'
    position = 1



    # 1. SCRAPING level 1
    def start_requests(self):

        for i in range(1,45):
            url = f'https://www.lomberg.de/angebote/page/{i}/?mt=rent&address&sort=meta_value%7Cdesc#immobilien'
            yield Request(url,dont_filter=True, callback=self.parse)

    # 2. SCRAPING level 2

    def parse(self, response):
        apartments = response.css(".immolisting .wp-block-column a::attr(href)").getall()
        for link in apartments:
            yield scrapy.Request(link, callback=self.populate_item)

    def populate_item(self, response):

        title =  response.css("h1::text").get()
        if 'praxis' in title.lower() or 'lager' in title.lower() or 'einzelhandel' in title.lower():return

        available_date =response.css(".immo-expose__list-price--list li:contains('verfügbar ab') .value::text").get()
        description=''

        for txt in json.loads(response.css(".vue-tabs::attr('data-tabs')").get()):
            description+=txt['rawValue']+' '



        description = description_cleaner(str(description))

        energy_label = response.css(".epass__info-list li:contains('Energieeffizienzklasse') .value::text").get()
        if energy_label:
            energy_label = remove_white_spaces(str(energy_label))


  
        floor = response.css(".immo-expose__list-price--list li:contains('Etage') .value::text").get()
        floor = extract_number_only(floor)
      
        rent = response.css(".immo-expose__list-price--list li:contains('Miete') .value::text").get()
        if rent:
            rx = re.search(r'\d+',rent.replace('.',''))
            if rx:
                rent=rx[0]
            else:
                rent='0'

        rent = extract_number_only(rent)

        deposit = response.css(".row.my-1 *:contains('€')::text").get()
        deposit = extract_number_only(deposit)
      
        utilities = response.css(".immo-expose__list-price--list li:contains('Nebenkosten') .value::text").get()
        utilities = extract_number_only(utilities)
       

        square_meters = response.css(".immo-expose__head--iconFields li:contains('ca.') .value::text").get()
        if square_meters:
            square_meters=re.search(r'\d+',square_meters)[0]
        
        room_count = response.css(".immo-expose__head--iconFields li:contains('Zimmer') .value::text").get()
        bathroom_count = response.css(".table-condensed tr:contains(Badezimmer) span::text").get()
        if room_count:
            room_count=re.search(r'\d+',room_count)[0]
        else:
            room_count='1'
        if room_count=='0':
            room_count='1'

        Amenties = " ".join(response.css(".table-condensed.equipment td::text").getall())
        if not response.css(".table-condensed.equipment td:contains('Haustiere erlaubt') .fa-times"):
            Amenties = Amenties.replace('Haustiere erlaubt','')


        energy_label = response.css(".table-condensed tr:contains(Energieeffizienzklasse) span +::text").get()
        if energy_label:
            energy_label = remove_white_spaces(energy_label)

        images = response.css(".lightgallery a::attr(href)").getall()

        ##################################**RETURN**###################################

        longitude,latitude='',''
        zipcode, city='',''
        address =  remove_white_spaces(response.css(".row:contains(Standort) p::text").get())+', Germany'
        if address:
            longitude,latitude = extract_location_from_address(address)
            zipcode, city, addres = extract_location_from_coordinates(longitude,latitude)

        #############

        landlord_name='lomberg'
        landlord_phone = '0800 - 80 72 000'
        landlord_email = 'info@lomberg.de'

        property_type = 'apartment' if 'wohnung' in description.lower() or 'wohnung' in " ".join(response.css(".list-group-item:contains('Objekttypen') .dd a::text").getall()).lower() else 'house'

        external_id = response.css(".immo-expose__list-price--list li:contains('Objekt-Nr') .value::text").get()
       

        if rent and int(rent) > 0 and int(rent) < 20000:
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

            item_loader.add_value("available_date", available_date)

            get_amenities(description,Amenties, item_loader)

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
            item_loader.add_value("energy_label", energy_label)  # String

            # # LandLord Details
            item_loader.add_value(
                "landlord_name", landlord_name)  # String
            item_loader.add_value(
                "landlord_phone", landlord_phone)  # String
            item_loader.add_value("landlord_email", landlord_email)  # String

            self.position += 1
            yield item_loader.load_item()
