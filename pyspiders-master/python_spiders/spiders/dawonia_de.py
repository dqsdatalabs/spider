# -*- coding: utf-8 -*-
# Author: Abanoub Moris
import json
import scrapy
from parsel import Selector
from scrapy.http.request import Request
from ..loaders import ListingLoader
from ..helper import *
import re


class DawoniaSpider(scrapy.Spider):

    name = "dawonia_de"
    country = 'germany'  # Fill in the Country's name
    locale = 'de'  # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'
    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        url = f'https://www.dawonia.de/mieten'
        yield Request(url, dont_filter=True, callback=self.parseApartment)

    # 2. SCRAPING level 2

    def parse(self, response):
        pass

    def parseApartment(self, response):

        apartments = response.css(".row:nth-child(2) .teaser-object")
        for apartment in apartments:
            title = remove_white_spaces(apartment.css(
                ".teaser-object__headline::text").get())
            info = (apartment.css(".teaser-object__text p *::text").getall())
            if len(info) < 3:
                continue
            info = "-".join(info).replace('- -', '').split('-')
            room_count, square_meters, rent = '1', '', ''
            for i in info:
                if 'Zimmer' in i:
                    room_count = re.search(r'\d+', i)[0]
                if 'Wohnfl' in i:
                    square_meters = re.search(r'\d+', i)[0]
                if 'altmiete' in i:
                    rent = re.search(r'\d+', i.replace('.', ''))[0]

            external_link = 'https://www.dawonia.de' + \
                apartment.css("._col-7 a::attr(href)").get()

            r = Selector(requests.get(external_link).text)
            address = remove_white_spaces(r.css(".container .row .col-6:contains('Deutschland')::text").get())+', Deutschland'
            description = remove_white_spaces(
                "\n".join(r.css(".container p::text").getall()))
            heating_cost = "".join(r.css(".container .row .col-lg-4 .row:contains('Heizkosten') *::text").getall())
            if heating_cost:
                heating_cost = re.search(r'\d+',heating_cost.replace('.',''))[0]
            
            utilities = "".join(r.css(".container .row .col-lg-4 .row:contains('etriebskosten') *::text").getall())
            if utilities:
                utilities = re.search(r'\d+',utilities.replace('.',''))[0]
            
            
            deposit = "".join(r.css(".container .row .col-lg-4 .row:contains('Kaution') *::text").getall())
            if deposit:
                deposit = re.search(r'\d+',deposit.replace('.',''))[0]

            floor_plan_images = r.css(".cta-button-item a::attr(href)").getall()
            floor_plan_images = ['https://www.dawonia.de'+x for x in floor_plan_images if 'jpg' in x]

            txt = remove_white_spaces(" ".join(r.css(".text-small-2.d-block.break-word-mobile *::text").getall()))
            landlord_email_txt = re.search(r'[\.?\w]+@[\.?\w]+',txt)
            if landlord_email_txt:
                landlord_email=landlord_email_txt[0]
            description = re.sub(
                r'mail.+|call.+|contact.+|kontakt.+|apply.+|\d+.\d+.\d+.\d+', "", description.lower())
            property_type = 'apartment'

            longitude, latitude = '', ''
            longitude, latitude = extract_location_from_address(address)

            zipcode, city, addres = '', '', ''
            try:

                zipcode, city, addres = extract_location_from_coordinates(
                    longitude, latitude)
            except:
                pass

            if not city or city=='':
                city = address.split(',')[0]

            images = r.css(".mobile-fancybox::attr(href)").getall()
            images = ['https://www.dawonia.de'+x for x in images]

            property_type = 'apartment' if 'apartment' in description.lower(
            ) or 'apartment' in title.lower() else property_type

            Amenty = ''
            
            if 'ja' in "".join(r.css(".container .row .col-lg-4 .row:contains('Terrasse') *::text").getall()):
                Amenty += 'terrace '

            if 'ja' in "".join(r.css(".container .row .col-lg-4 .row:contains('Balkon') *::text").getall()):
                Amenty += 'Balkon '

            if 'ja' in "".join(r.css(".container .row .col-lg-4 .row:contains('Aufzug') *::text").getall()):
                Amenty+='elevator '

            if int(rent) > 0 and int(rent) < 20000:
                item_loader = ListingLoader(response=response)

                # # MetaData
                item_loader.add_value("external_link", external_link)  # String
                item_loader.add_value(
                    "external_source", self.external_source)  # String

                # item_loader.add_value("external_id", str(external_id))  # String
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
                # item_loader.add_value("bathroom_count", bathroom_count)  # Int

                item_loader.add_value("available_date", 'immediately')

                self.get_features_from_description(
                    description,Amenty, item_loader)

                # # Images
                item_loader.add_value("images", images)  # Array
                item_loader.add_value(
                    "external_images_count", len(images))  # Int
                item_loader.add_value("floor_plan_images", floor_plan_images)  # Array

                # # Monetary Status
                item_loader.add_value("rent", rent)  # Int
                item_loader.add_value("deposit", deposit)  # Int
                # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
                item_loader.add_value("utilities", utilities)  # Int
                item_loader.add_value("currency", "EUR")  # String

                # item_loader.add_value("water_cost", water_cost) # Int
                item_loader.add_value("heating_cost", heating_cost) # Int

                # item_loader.add_value("energy_label", energy_label)  # String

                # # LandLord Details
                item_loader.add_value(
                    "landlord_name", 'Dawonia')  # String
                item_loader.add_value(
                    "landlord_phone", '+49 89 306 17-0')  # String
                item_loader.add_value(
                    "landlord_email", landlord_email)  # String

                self.position += 1
                yield item_loader.load_item()

    Amenties = {
        'pets_allowed': ['pet'],
        'furnished': ['furnish', 'MÖBLIERTES'.lower()],
        'parking': ['parking', 'garage', 'parcheggio'],
        'elevator': ['elevator', 'aufzug', 'ascenseur', 'lift', 'aufzüg'],
        'balcony': ['balcon', 'balkon'],
        'terrace': ['terrace', 'terrazz', 'terras', 'terrass'],
        'swimming_pool': ['pool', 'piscine','schwimmbad'],
        'washing_machine': [' washer', 'laundry', 'washing_machine', 'waschmaschine', 'laveuse'],
        'dishwasher': ['dishwasher', 'geschirrspüler', 'lave-vaiselle', 'lave vaiselle']
    }

    def get_features_from_description(self, description, response, item_loader):
        description = description.lower() +' '+ response.lower()
        
        pets_allowed = True if any(
            x in description for x in self.Amenties['pets_allowed']) else False
        furnished = True if any(
            x in description for x in self.Amenties['furnished']) else False
        parking = True if any(
            x in description for x in self.Amenties['parking']) else False
        elevator = True if any(
            x in description for x in self.Amenties['elevator']) else False
        balcony = True if any(
            x in description for x in self.Amenties['balcony']) else False
        terrace = True if any(
            x in description for x in self.Amenties['terrace']) else False
        swimming_pool = True if any(
            x in description for x in self.Amenties['swimming_pool']) else False
        washing_machine = True if any(
            x in description for x in self.Amenties['washing_machine']) else False
        dishwasher = True if any(
            x in description for x in self.Amenties['dishwasher']) else False

        item_loader.add_value("pets_allowed", pets_allowed)  # Boolean
        item_loader.add_value("furnished", furnished)  # Boolean
        item_loader.add_value("parking", parking)  # Boolean
        item_loader.add_value("elevator", elevator)  # Boolean
        item_loader.add_value("balcony", balcony)  # Boolean
        item_loader.add_value("terrace", terrace)  # Boolean
        item_loader.add_value("swimming_pool", swimming_pool)  # Boolean
        item_loader.add_value("washing_machine", washing_machine)  # Boolean
        item_loader.add_value("dishwasher", dishwasher)  # Boolean
        return pets_allowed, furnished, parking, elevator, balcony, terrace, swimming_pool, washing_machine, dishwasher
