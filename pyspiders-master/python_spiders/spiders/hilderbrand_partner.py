# -*- coding: utf-8 -*-
# Author: Ahmed Shahien
import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import requests
from ..helper import *
from bs4 import BeautifulSoup
import datetime
import dateparser

class HilderbrandPartnerSpider(scrapy.Spider):
    name = "hilderbrand_partner"
    # start_urls = ['https://hildebrand-partner.com/immobilien/wohnung-in-leipzig-mieten-untere11a-we72/']
    start_urls = ['https://hildebrand-partner.com/immobilien-vermarktungsart/miete/page/1/?post_type=immomakler_object&nutzungsart&typ&ort&objekt-id&collapse&von-qm=0.00&bis-qm=185.00&von-zimmer=0.00&bis-zimmer=13.00&von-kaltmiete=0.00&bis-kaltmiete=6000.00&von-kaufpreis=NaN&bis-kaufpreis=NaN']
    allowed_domains = ["hildebrand-partner.com"]
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
        titles = response.css('.property-title a::text').extract()
        for index, property_url in enumerate(property_urls):
            yield Request(url=property_url, callback=self.populate_item, meta={'title': titles[index]})
        try:
            page_next = response.css('a.page-numbers::attr(href)')[-1].extract()
            yield Request(url=page_next, callback=self.parse)
        except:
            pass

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        title = response.meta["title"]
        description = response.css('.panel-body p ::text').extract()
        description = ' '.join(description)
        description = description_cleaner(description)
        address = response.css('.list-group-item:nth-child(3) .col-sm-7 ::text')[0].extract()
        longitude, latitude = extract_location_from_address(address)
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)
        list = response.css('.list-group-item div ::text').extract()
        list = ' '.join(list)
        list = remove_white_spaces(list)

        property_type = 'apartment'
        if 'Objekttyp' in list:
            property_type = list.split('Objekttyp')[1].split(' A')[0]
            if 'wohnung' in property_type.lower():
                property_type = 'apartment'
            elif 'haus' in property_type.lower():
                property_type = 'house'
            else:
                return
        if 'Kaltmiete' in list:
            rent = list.split('Kaltmiete')[1].split(',')[0]
            rent = int(''.join(x for x in rent if x.isdigit()))
        utilities = None
        if 'Nebenkosten' in list:
            utilities = list.split('Nebenkosten')[1].split(',')[0]
            utilities = int(''.join(x for x in utilities if x.isdigit()))
        deposit = None
        if 'Kaution' in list:
            deposit = list.split('Kaution')[1].split(',')[0]
            deposit = int(''.join(x for x in deposit if x.isdigit()))


        available_date = None
        if 'Verfügbar ab' in list:
            available_date = list.split('Verfügbar ab ')[1].split(' ')[0]
            try:
                available_date = available_date.strip()
                available_date = dateparser.parse(available_date)
                available_date = available_date.strftime("%Y-%m-%d")
            except:
                available_date = None

        external_id = None
        if 'Objekt ID' in list:
            external_id = list.split('Objekt ID ')[1].split(' O')[0]
        floor = None
        if 'Etage' in list:
            floor = list.split('Etage ')[1].split(' ')[0]
        room_count = 1
        if ' Zimmer' in list:
            room_count = int(list.split(' Zimmer ')[1].split(' ')[0])
        bathroom_count = 1
        if 'Badezimmer' in list:
            bathroom_count = int(list.split('Badezimmer ')[1].split(' ')[0])
        square_meters = None
        try:
            if 'Wohnfläche ca.' in list:
                square_meters = list.split('Wohnfläche ca. ')[1].split(' m²')[0]
            elif 'Bürofläche ca.' in list:
                square_meters = list.split('Bürofläche ca. ')[1].split(' m²')[0]
            else:
                pass
            if ',' in square_meters:
                square_meters = int(square_meters.split(',')[0])
        except:
            pass

        balcony = None
        if 'balkon' in list.lower() or 'balkon' in title.lower():
            balcony = True
        parking = None
        if 'stellplätze' in list.lower() or 'stellplatz' in list.lower():
            parking = True
        terrace = None
        if 'terrasse' in list.lower():
            terrace = True

        landlord_name = list.split('Name ')[1].split(' Firma')[0]
        landlord_phone = list.split('Tel. Zentrale ')[1].split(' ')[0]
        landlord_email = list.split('E-Mail Zentrale ')[1].split(' ')[0]

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
        #item_loader.add_value("elevator", elevator) # Boolean
        item_loader.add_value("balcony", balcony) # Boolean
        item_loader.add_value("terrace", terrace) # Boolean
        #item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        #item_loader.add_value("washing_machine", washing_machine) # Boolean
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
        #item_loader.add_value("heating_cost", heating_cost) # Int

        #item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name) # String
        item_loader.add_value("landlord_phone", landlord_phone) # String
        item_loader.add_value("landlord_email", landlord_email) # String

        self.position += 1
        yield item_loader.load_item()
