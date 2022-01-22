# -*- coding: utf-8 -*-
# Author: Ahmed Shahien
import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import requests
from ..helper import *
import datetime
import dateparser


class HeinkeImmoSpider(scrapy.Spider):
    name = "heinke_immo"
    start_urls = ['https://heinke-immobilien.de/immobilien-vermarktungsart/miete/?post_type=immomakler_object&nutzungsart&typ&ort&center&radius=25&objekt-id&collapse=in&von-qm=0.00&bis-qm=285.00&von-zimmer=0.00&bis-zimmer=12.00&von-kaltmiete=0.00&bis-kaltmiete=2900.00&von-kaufpreis=0.00&bis-kaufpreis=1225000.00']
    allowed_domains = ["heinke-immobilien.de"]
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
        property_urls = response.css('.property-title a::attr(href)').extract()
        for property_url in property_urls:
            yield Request(url=property_url, callback=self.populate_item)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        title = response.css('.property-title::text')[0].extract()
        if 'wohnung' in title.lower():
            property_type = 'apartment'
        elif 'house' in title.lower() or 'Stadthaus' in title:
            property_type = 'house'
        else:
            return

        address = response.css('.property-subtitle::text')[0].extract()
        longitude, latitude = extract_location_from_address(address)
        zipcode, city, address = extract_location_from_coordinates(longitude,latitude)
        longitude = str(longitude)
        latitude = str(latitude)
        description = response.css('.property-description p::text').extract()
        description = ' '.join(description)

        details = response.css('.col-sm-pull-7 .row div::text').extract()
        details = ' '.join(details)


        rent = details.split('Kaltmiete ')[1].split(',')[0]
        if any(char.isdigit() for char in rent):
            rent = int(''.join(x for x in rent if x.isdigit()))

        deposit = None
        if 'Kaution' in details:
            deposit = details.split('Kaution ')[1].split(',')[0]
            if any(char.isdigit() for char in deposit):
                deposit = int(''.join(x for x in deposit if x.isdigit()))
        else:
            pass

        heating_cost = None
        if 'Warmmiete' in details:
            heating_cost = details.split('Warmmiete ')[1].split(',')[0]
            if any(char.isdigit() for char in heating_cost):
                heating_cost = int(''.join(x for x in heating_cost if x.isdigit()))
            heating_cost = heating_cost - rent
        else:
            pass

        external_id = details.split('Objekt ID ')[1].split(' Objekttyp')[0]
        room_count = int(details.split('Schlafzimmer ')[1].split(' ')[0])
        bathroom_count = int(details.split('Badezimmer ')[1].split(' ')[0])
        square_meters = details.split('Wohnfläche ca. ')[1].split(' m²')[0]
        if ',' in square_meters:
            square_meters = int(square_meters[:-3]) + 1
        else:
            square_meters = int(square_meters)
        available_date = None
        if 'Verfügbar' in details:
            available_date = details.split('Verfügbar ab ')[1].split(' ')[0]
            if 'sofort' in available_date:
                available_date = None
            else:
                available_date = dateparser.parse(available_date)
                available_date = available_date.strftime("%Y-%m-%d")
        else:
            pass


        floor = None
        if 'Erdgeschosswohnung' in details:
            floor = 'Ground'
        elif 'Etage' in details:
                floor = details.split('Etage ')[1].split(' ')[0]
        else:
            pass
        balcony = None
        if 'Balkon' in details:
            balcony = True
        terrace = None
        if 'Terrass' in details:
            terrace = True
        parking = None
        if 'Stellplatzmiete' in details:
            parking = True

        elevator = None
        try:
            elevator = response.css('.property-features .list-group-item::text').extract()
            elevator = ''.join(elevator)
            if 'Personenaufzug' in elevator:
                elevator = True
            else:
                elevator = None
        except:
            pass

        energy_label = None
        try:
            if 'Energie­effizienz­klasse' in details:
                energy_label = details.split('Energie­effizienz­klasse ')[1].split(' ')[0]
            elif 'Endenergie­bedarf' in details:
                energy_label = details.split('Endenergie­bedarf ')[1].split(',')[0]
                energy_label = int(energy_label)
                if energy_label >= 250:
                    energy_label = 'H'
                elif energy_label >= 225 and energy_label <= 250:
                    energy_label = 'G'
                elif energy_label >= 160 and energy_label <= 175:
                    energy_label = 'F'
                elif energy_label >= 125 and energy_label <= 160:
                    energy_label = 'E'
                elif energy_label >= 100 and energy_label <= 125:
                    energy_label = 'D'
                elif energy_label >= 75 and energy_label <= 100:
                    energy_label = 'C'
                elif energy_label >= 50 and energy_label <= 75:
                    energy_label = 'B'
                elif energy_label >= 25 and energy_label <= 50:
                    energy_label = 'A'
                elif energy_label >= 1 and energy_label <= 25:
                    energy_label = 'A+'
            else:
                pass
        except:
            pass
        try:
            landlord_name = response.css('.fn ::text')[1].extract()
        except:
            landlord_name = 'Heinke Immobilien'
        images = response.css('[id=immomakler-galleria] a::attr(href)').extract()



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
        item_loader.add_value("floor", floor) # String
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
        #item_loader.add_value("washing_machine", washing_machine) # Boolean
        #item_loader.add_value("dishwasher", dishwasher) # Boolean

        # # Images
        item_loader.add_value("images", images) # Array
        # item_loader.add_value("external_images_count", len(images)) # Int
        #item_loader.add_value("floor_plan_images", floor_plan_images) # Array

        # # Monetary Status
        item_loader.add_value("rent", rent) # Int
        item_loader.add_value("deposit", deposit) # Int
        #item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        #item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", "EUR") # String

        #item_loader.add_value("water_cost", water_cost) # Int
        item_loader.add_value("heating_cost", heating_cost) # Int

        item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name) # String
        item_loader.add_value("landlord_phone", '07541-95130') # String
        item_loader.add_value("landlord_email", 'rental@heinke-immobilien.com') # String

        self.position += 1
        yield item_loader.load_item()
