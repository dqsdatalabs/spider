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

class TraumImmoSpider(scrapy.Spider):
    name = "traum_immo"
    start_urls = ['https://www.traum.immobilien/immobilienangebot-vermietung/']
    # start_urls = ['https://www.traum.immobilien/immobilien/teilmoeblierte-2-zimmer-wohnung-im-dachgeschoss/']
    allowed_domains = ["traum.immobilien"]
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
        property_urls = response.css('.inx-property-list > div > div > a::attr(href)').extract()
        property_types = response.css('.inx-property-list-item__property-type div::text').extract()
        for index, property_url in enumerate(property_urls):
            yield Request(url=property_url, callback=self.populate_item, meta={'type': property_types[index]})
        try:
            next_page = response.css('.current+ .page-numbers::attr(href)')[0].get()
            yield Request(url=next_page, callback=self.parse)
        except:
            pass

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        property_type = response.meta["type"]
        if 'haus' in property_type.lower():
            property_type = 'house'
        elif 'wohnung' in property_type.lower():
            property_type = 'apartment'
        elif 'Mühlhausen' in property_type:
            return
        else:
            return
        list = response.css('.inx-detail-list__item ::text').extract()
        # if "Warmmiete:" in list:
        #     warmrent_index = [i for i, x in enumerate(list) if "Warmmiete:" in x][0]
        #     warm_rent = list[warmrent_index + 2].strip()
        rent = None
        if "Kaltmiete:" in list:
            rent_index = [i for i, x in enumerate(list) if "Kaltmiete:" in x][0]
            rent = list[rent_index + 2].strip()
            rent = rent[:-2]
            if ',' in rent:
                rent = rent.split(',')[0]
            rent = int(''.join(x for x in rent if x.isdigit()))
        else:
            return

        utilities = None
        if "monatl. Betriebs-/Nebenkosten:" in list:
            util_index = [i for i, x in enumerate(list) if "monatl. Betriebs-/Nebenkosten:" in x][0]
            utilities = list[util_index + 2].strip()
            utilities = utilities[:-2]
            if ',' in utilities:
                utilities = utilities.split(',')[0]
            utilities = int(''.join(x for x in utilities if x.isdigit()))

        deposit = None
        if "Kaution:" in list:
            deposit_index = [i for i, x in enumerate(list) if "Kaution:" in x][0]
            deposit = list[deposit_index + 2].strip()
            if ',' in deposit:
                deposit = deposit.split(',')[0]
            deposit = int(''.join(x for x in deposit if x.isdigit()))

        heating_cost = None
        if "monatl. Heizkosten:" in list:
            heating_index = [i for i, x in enumerate(list) if "monatl. Heizkosten:" in x][0]
            heating_cost = list[heating_index + 2].strip()
            heating_cost = heating_cost[:-2]
            if ',' in heating_cost:
                heating_cost = heating_cost.split(',')[0]
            heating_cost = int(''.join(x for x in heating_cost if x.isdigit()))

        square_meters = None
        if "Wohnfläche:" in list:
            square_index = [i for i, x in enumerate(list) if "Wohnfläche:" in x][0]
            square_meters = list[square_index + 2].strip()
            square_meters = square_meters.split(' m²')[0]
            if ',' in square_meters:
                square_meters = square_meters.split(',')[0]
            square_meters = int(''.join(x for x in square_meters if x.isdigit()))
        else:
            return

        if "Zimmer insgesamt:" in list:
            room_index = [i for i, x in enumerate(list) if "Zimmer insgesamt:" in x][0]
            room_count = list[room_index + 2].strip()
            room_count = int(room_count[0])
        else:
            room_count = 1
        bathroom_count = None
        if "Badezimmer:" in list:
            bathroom_index = [i for i, x in enumerate(list) if "Badezimmer:" in x][0]
            bathroom_count = list[bathroom_index + 2].strip()
        floor = None
        if "Etage:" in list:
            floor_index = [i for i, x in enumerate(list) if "Etage:" in x][0]
            floor = list[floor_index + 2].strip()
        pets_allowed = None
        if "Haustiere erlaubt:" in list:
            pets_index = [i for i, x in enumerate(list) if "Haustiere erlaubt:" in x][0]
            pets_allowed = list[pets_index + 2].strip()
            if 'nein' in pets_allowed.lower():
                pets_allowed = False
            else:
                pets_allowed = True

        available_date = None
        if "Immobilie ist verfügbar ab:" in list:
            date_index = [i for i, x in enumerate(list) if "Immobilie ist verfügbar ab:" in x][0]
            available_date = list[date_index + 2].strip()
            try:
                available_date = dateparser.parse(available_date)
                available_date = available_date.strftime("%Y-%m-%d")
            except:
                available_date = None
        title = response.css('.uk-padding > .uk-margin-bottom ::text')[0].extract()
        description = response.css('.inx-description-text ::text')[0].extract()
        address = response.css('.uk-width-expand::text')[0].extract()
        longitude, latitude = extract_location_from_address(address)
        zipcode, city, address = extract_location_from_coordinates(longitude,latitude)
        longitude = str(longitude)
        latitude = str(latitude)
        external_id = response.css('.inx-single-property__head-element-title::text')[0].extract()
        images = response.css('.inx-thumbnail-nav__flexible img::attr(src)').extract()
        for image in images:
            if "120x68" in image:
                images = {x.replace('-120x68', '') for x in images}
            else:
                pass

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

        item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        #item_loader.add_value("furnished", furnished) # Boolean
        #item_loader.add_value("parking", parking) # Boolean
        #item_loader.add_value("elevator", elevator) # Boolean
        #item_loader.add_value("balcony", balcony) # Boolean
        #item_loader.add_value("terrace", terrace) # Boolean
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
        item_loader.add_value("heating_cost", heating_cost) # Int

        #item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", 'KOCH IMMOBILIEN') # String
        item_loader.add_value("landlord_phone", '03601 81 28 42') # String
        item_loader.add_value("landlord_email", 'kontakt@Traum.Immobilien') # String

        self.position += 1
        yield item_loader.load_item()
