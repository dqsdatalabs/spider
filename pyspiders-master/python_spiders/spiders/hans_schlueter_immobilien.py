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


class ImmopoolSpider(scrapy.Spider):

    name = "hans_schlueter_immobilien"
    country = 'germany'  # Fill in the Country's name
    locale = 'de'  # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'
    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        url = f'https://hans-schlueter-immobilien.de/Vermietung'
        yield Request(url, callback=self.parse)

    # 2. SCRAPING level 2

    def parse(self, response):
        apartments = response.css("a.object_list_box::attr(href)").getall()
        for link in apartments:
            yield scrapy.Request(link, callback=self.populate_item)

    def populate_item(self, response):

        title = response.css(".row h2::text").get()
      
        rent = response.css(".object_price_item .value::text").get().replace('.','')
        if rent:
            rent = re.search(r'\d+',rent)[0]

        data = response.css(".object_data")[0]
        square_meters = "".join(data.css("li:contains(Wohnfläche) .object_data_item::text").getall())
        if square_meters:
            square_meters=re.search(r'\d+',square_meters)[0]
        
        room_count = "".join(data.css("li:contains(Zimmer) .object_data_item::text").getall())
        if room_count:
            room_count=re.search(r'\d+',room_count)[0]
        else:
            room_count='1'
        if room_count=='0':
            room_count='1'

        available_date = "".join(data.css("li:contains(Frei) .object_data_item::text").getall()).replace('\n','').replace('\r','').replace(' ','') 


        utilities = response.css(".object_data li:contains(Nebenkosten) .price *::text").get()
        if utilities:
            try:
                utilities = re.search(r'\d+',utilities.replace('.',''))[0]
            except:
                pass
        
        description=''
        for txt in response.css(".row p:not(br)::text").getall():
            if len(txt)>50:
                description+=txt+' '
        description = description_cleaner(description)

        images = response.css(".objects_gallery a::attr(href)").getall()
        images = ['https://hans-schlueter-immobilien.de' + x for x in images]

        ##################################**RETURN**###################################
        address =  response.css(".row .offset-md-1 p::text").get()+', '+'Germany'
        longitude,latitude = extract_location_from_address(address)
        zipcode, city, addres = extract_location_from_coordinates(longitude,latitude)
        #############

        landlord_name=response.css(".people_list_item .name::text").get()
        landlord_phone = response.css(".people_list_item .phone *::text").get()
        landlord_email = response.css(".people_list_item .email *::text").get()

        property_type = 'apartment' if 'wohnung' in description.lower() else 'house'

        external_id = re.search(r'\d+',response.url.split('/')[-1])[0]
       

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
            # item_loader.add_value("floor", floor)  # String
            item_loader.add_value("property_type", property_type)  # String
            item_loader.add_value("square_meters", square_meters)  # Int
            item_loader.add_value("room_count", room_count)  # Int
            #item_loader.add_value("bathroom_count", bathroom_count)  # Int

            item_loader.add_value("available_date", available_date)

            get_amenities(description, " ".join(response.css(".object_info.has-no-label dd::text").getall()), item_loader)

            # # Images
            item_loader.add_value("images", images)  # Array
            item_loader.add_value(
                "external_images_count", len(images))  # Int
            # item_loader.add_value("floor_plan_images", floor_plan_images)  # Array

            # # Monetary Status
            item_loader.add_value("rent", rent)  # Int
            #item_loader.add_value("deposit", deposit)  # Int
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
