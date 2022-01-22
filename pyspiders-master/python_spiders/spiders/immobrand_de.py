# -*- coding: utf-8 -*-
# Author: Abanoub Moris

import json
import scrapy
from parsel import Selector
from scrapy.http.request import Request
from ..loaders import ListingLoader
from ..helper import *
import re


class ImmobrandSpider(scrapy.Spider):

    name = "immobrand"
    country = 'germany'  # Fill in the Country's name
    locale = 'de'  # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'
    position = 1



    # 1. SCRAPING level 1
    def start_requests(self):

        url = f'https://www.immobrand.de/vermietung/'
        yield Request(url,dont_filter=True, callback=self.parse)

    # 2. SCRAPING level 2

    def parse(self, response):
        apartments = response.css(".oo-detailslink a::attr(href)").getall()
        for link in apartments:
            yield scrapy.Request(link, callback=self.populate_item)

    def populate_item(self, response):
        if 'laden' in response.css(".oo-detailslisttd:contains('Objektart') +::text").get().lower():return
        title =  response.css(".oo-detailsheadline h1::text").get()
        if 'praxis' in title.lower() or 'lager' in title.lower() or 'einzelhandel' in title.lower():return

        description = description_cleaner(" ".join(response.css(".oo-detailsfreetext::text").getall()))

        energy_label = response.css(".epass__info-list li:contains('Energieeffizienzklasse') .value::text").get()
        if energy_label:
            energy_label = remove_white_spaces(str(energy_label))
      
        rent = response.css(".oo-detailslisttd:contains('miete') +:contains('€')::text").get()
        if rent:
            rx = re.search(r'\d+',rent.replace('.',''))
            if rx:
                rent=rx[0]
            else:
                rent='0'

        #deposit = response.css(".row.my-1 *:contains('€')::text").get()
        #deposit = extract_number_only(deposit)
      
        #utilities = response.css(".immo-expose__list-price--list li:contains('Nebenkosten') .value::text").get()
        #utilities = extract_number_only(utilities)
       

        square_meters = response.css(".oo-detailslisttd:contains('Wohnfläche') +:contains('ca.')::text").get()
        if not square_meters:
            square_meters = response.css(".oo-detailslisttd:contains('Nutzfläche') +:contains('ca.')::text").get()

        if square_meters:
            square_meters=re.search(r'\d+',square_meters)[0]
        
        room_count = response.css(".oo-detailslisttd:contains('Zimmer') +::text").get()
        bathroom_count = response.css(".oo-detailslisttd:contains('Badezimmer') +::text").get()
        if room_count:
            room_count=re.search(r'\d+',room_count)[0]
        else:
            room_count='1'
        if room_count=='0':
            room_count='1'

        Amenties = " ".join(response.css(".table-condensed.equipment td::text").getall())
        if not response.css(".table-condensed.equipment td:contains('Haustiere erlaubt') .fa-times"):
            Amenties = Amenties.replace('Haustiere erlaubt','')


        energy_label = response.css(".oo-detailslisttd:contains('Energieeffizienzklasse') +::text").get()
        if energy_label:
            energy_label = remove_white_spaces(energy_label)

        images = response.css(".oo-detailspicture::attr(style)").getall()
        images = [re.search(r'http.+jpg',x)[0] for x in images]

        ##################################**RETURN**###################################

        longitude,latitude='',''
        zipcode, city='',''
        address =  response.css(".oo-detailslisttd:contains('PLZ') +::text").get() +' '+response.css(".oo-detailslisttd:contains('Ort') +::text").get()+', Germany'
        if address:
            longitude,latitude = extract_location_from_address(address)
            zipcode, city, addres = extract_location_from_coordinates(longitude,latitude)

        #############

        landlord_name=remove_white_spaces("".join(response.css(".oo-details-sidebar .oo-aspname *::text").getall()))
        landlord_phone = remove_white_spaces("".join(response.css(".oo-details-sidebar .oo-aspcontact *::text").getall()))
        landlord_email = 'badzwischenahn@immobrand.de'

        property_type = 'apartment' if 'wohnung' in description.lower() or 'wohnung' in response.css(".oo-detailslisttd:contains('Objektart') +::text").get().lower() else 'house'

        external_id = response.css(".oo-detailslisttd:contains('ImmoNr') +::text").get()
       

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
            #item_loader.add_value("floor", floor)  # String
            item_loader.add_value("property_type", property_type)  # String
            item_loader.add_value("square_meters", square_meters)  # Int
            item_loader.add_value("room_count", room_count)  # Int
            item_loader.add_value("bathroom_count", bathroom_count)  # Int

            #item_loader.add_value("available_date", available_date)

            get_amenities(description,Amenties, item_loader)

            # # Images
            item_loader.add_value("images", images)  # Array
            item_loader.add_value(
                "external_images_count", len(images))  # Int
            # item_loader.add_value("floor_plan_images", floor_plan_images)  # Array

            # # Monetary Status
            item_loader.add_value("rent", rent)  # Int
            #item_loader.add_value("deposit", deposit)  # Int
            # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
            #item_loader.add_value("utilities", utilities)  # Int
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
