# -*- coding: utf-8 -*-
# Author: Abanoub Moris

from time import time
import scrapy
from parsel import Selector
from scrapy.http.request import Request
from ..loaders import ListingLoader
from ..helper import *
import re


class Fechner_immobilienSpider(scrapy.Spider):

    name = "fechner_immobilien"
    country = 'germany'  # Fill in the Country's name
    locale = 'de'  # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'
    position = 1



    # 1. SCRAPING level 1
    def start_requests(self):
        urls = ['https://www.fechner-immobilien.com/immobilien',
            'https://www.fechner-immobilien.com/immobilien,o6',
            'https://www.fechner-immobilien.com/immobilien,o12',
            'https://www.fechner-immobilien.com/immobilien,o18']

        for url in urls:
            yield Request(url,dont_filter=True, callback=self.parse)

    # 2. SCRAPING level 2

    def parse(self, response):
        apartments = response.css("a.no-hover::attr(href)").getall()
        apartments = ['https://www.fechner-immobilien.com'+x for x in apartments]
        for link in apartments:
            yield scrapy.Request(link, callback=self.populate_item)

    def populate_item(self, response):

        title =  response.css(".expose h1::text").get()
        if 'reserviert' in title.lower() or 'vermietet' in title.lower():
            return
            
        rex = re.search(r'\d+\.\d+\.\d+',title)
        available_date=''
        if rex:
            available_date=rex[0]
        else:
            available_date=response.css(".table-condensed tr:contains('verfügbar ab') span::text").get()
            if available_date and 'sofort' in available_date:
                available_date='Available Now!'
        
        floor = response.css(".table-condensed tr:contains('Lage im Objekt (Etage)') span::text").get()
        if floor:
            floor=re.search(r'\d+',floor)[0]
      
        rent = response.css(".table-condensed tr:contains(Warmmiete) span::text").get()
        if rent:
            rent = re.search(r'\d+',rent.replace('.',''))[0]
    

        deposit = response.css(".table-condensed tr:contains(Kaution) span::text").get()
        if deposit:
            deposit = re.search(r'\d+',deposit.replace('.',''))[0]

            if not rent:
                rent=deposit

        utilities = response.css(".table-condensed tr:contains(Nebenkosten) span::text").get()
        if utilities:
            utilities = re.search(r'\d+',utilities.replace('.',''))[0]

        heating_cost = response.css(".table-condensed tr:contains(Heizkosten) span::text").get()
        if heating_cost:
            rex = re.search(r'\d+',heating_cost.replace('.',''))
            if rex:
                heating_cost = rex[0]
        else:
            heating_cost=''

        square_meters = response.css(".table-condensed tr:contains(Wohnfläche) span::text").get()
        if square_meters:
            square_meters=re.search(r'\d+',square_meters)[0]
        
        room_count = response.css(".table-condensed tr:contains(Zimmer) span::text").get()
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

        description= description_cleaner(" ".join(response.css(".expose p *::text").getall()))
        if 'warehouse' in description.lower():
            return

        images = response.css("a.sc-media-gallery::attr(href)").getall()

        ##################################**RETURN**###################################


        location = extract_coordinates_regex(response.css("script:contains('displayMap')").get())
        latitude = str(location[0])
        longitude = str(location[1])
        zipcode, city, address = extract_location_from_coordinates(longitude,latitude)

        if not city:
            address =  response.css(".right-box:contains(Adresse) p::text").get()
            if address:
                longitude,latitude = extract_location_from_address(address)
                zipcode, city, addres = extract_location_from_coordinates(longitude,latitude)

        


        #############

        landlord_name=remove_white_spaces(response.css(".right-box strong::text").get())
        if not landlord_name:
            landlord_name='Fechner Immobilien'
        landlord_phone = remove_white_spaces(response.css(".right-box *:contains(Telefon)::text").get())
        if landlord_phone:
            landlord_phone = landlord_phone.replace('Telefon:','')
        else:
            landlord_phone='+49 (841) 88 54 71 10'
        landlord_email = 'aw@fechner-immobilien.com'

        property_type = 'apartment' if 'wohnung' in description.lower() or 'wohnung' in " ".join(response.css(".list-group-item:contains('Objekttypen') .dd a::text").getall()).lower() else 'house'

        external_id = re.search(r'\d+',response.css(".table-condensed tr:contains(Objekt) span::text").get())[0]
       

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
