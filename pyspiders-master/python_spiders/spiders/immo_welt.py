# -*- coding: utf-8 -*-
# Author: Ahmed Shahien
import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import requests
import json
from scrapy.http.request.json_request import JsonRequest
from ..helper import *
from bs4 import BeautifulSoup


class ImmoWeltSpider(scrapy.Spider):
    name = "immo_welt"
    start_urls = ['https://www.immowelt.de/profil/e08556cf5e20cb43abd4f57de256bbf0']
    allowed_domains = ["immowelt.de"]
    country = 'Germany' # Fill in the Country's name
    locale = 'de' # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing' 

    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        yield JsonRequest(url='https://data.immowelt.de/brokerprofile/brokerprofile-ui/graphql',
                          callback=self.parse,
                          data={
                                "query": "query estateList_query($sort: String!, $cursor: Int, $limit: Int, $brokerId: String!) {\n  estateList(sort: $sort, cursor: $cursor, limit: $limit, brokerId: $brokerId) {\n    data {\n      isNew\n      headline\n      globalObjectKey\n      estateType\n      salesType\n      exposeUrl\n      area\n      livingArea\n      imageCount\n      image\n      imageHD\n      city\n      zip\n      showMap\n      street\n      priceName\n      priceValue\n      rooms\n      isDiamond\n      projektDetailLink\n      projektDetailLinkText\n      projektTitel\n    }\n    pagination {\n      countPagination\n      countTotal\n      nextPage\n    }\n  }\n}\n",
                                "variables": {
                                    "brokerId": "e08556cf5e20cb43abd4f57de256bbf0",
                                    "cursor": 0,
                                    "limit": 20,
                                    "sort": "modifiedAt"
                                }
                          },
                          method='POST')

    # 2. SCRAPING level 2
    def parse(self, response, **kwargs):
        parsed_response = json.loads(response.body)
        for item in parsed_response['data']['estateList']['data']:
            url = item['exposeUrl']
            yield Request(url=url,
                          callback=self.populate_item,
                          meta={"item": item}
                          )

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item = response.meta["item"]

        is_rent = item['salesType']
        if 'KAUF' in is_rent:
            return
        property_type = item['estateType']
        if 'Wohnungen' in property_type:
            property_type = 'apartment'
        else:
            return
        title = item['headline']
        external_id = response.css('.self-center .ng-star-inserted::text')[0].extract()
        city = item['city']
        zipcode = item['zip']
        square_meters = item['livingArea']
        try:
            square_meters = int(square_meters)
        except:
            square_meters = None
        room_count = int(item['rooms'])
        rent_type = item['priceName'].strip()
        utilities = None
        heating_cost = None
        parking = None
        if 'Warmmiete:' in rent_type:
            rent = item['priceValue'].strip()
            rent = rent.split(' €')[0]
        else:
            rents = response.css('#aPreise .cell__row ::text').extract()
            rents = ''.join(rents)
            rent = rents.split('Kaltmiete ')[1].split(' €')[0]
            utilities = int(rents.split('Nebenkosten ')[1].split(' €')[0])
            try:
                heating_cost = int(rents.split('Heizkosten ')[1].split(' €')[0])
            except:
                pass
            if 'Stellplatz' in rents:
                parking = True
        if ',' in rent:
            rent = int(rent.split(',')[0]) + 1
        else:
            rent = int(rent)

        deposit = response.css('#aPreise .ng-star-inserted .card-content::text')[0].extract()
        try:
            deposit = int(deposit[0]) * rent
        except:
            deposit = None

        energy_label = None
        try:
            energy_label = response.css('div.energy_information.ng-star-inserted > sd-cell:nth-child(6) > sd-cell-row > sd-cell-col > p:nth-child(2)::text')[0].extract().strip()
            if ',' in energy_label:
                energy_label = int(energy_label.split(',')[0])
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
        except:
            pass

        amenities = response.css('#aImmobilie .card-content .ng-star-inserted ::text , #aImmobilie p ::text').extract()
        floor = None
        if 'Wohnungslage' in amenities:
            floor = amenities[1]
        amenities = '' .join(amenities)
        if 'Bad' in amenities:
            bathroom_count = 1
        else:
            bathroom_count = None
        balcony = None
        if 'Balkon' in amenities:
            balcony = True
        if 'Stellplatz' in amenities:
            parking = True




        soup = BeautifulSoup(response.text, 'html.parser')
        script = soup.find('script', type='application/json').text
        try:
            description = script.split('Objektbeschreibung&q;,&q;Content&q;:&q;')[1].split('&q;,&q;Position&q;')[0]
        except:
            description = script.split('Beschreibung Wohnung&q;,&q;Content&q;:&q;')[1].split('&q;,&q;Position&q;')[0]
        if '-' in description:
            description.replace('-', '')

        latlng = script.split('{&q;Latitude&q;:')[1].split('},&q;ShowPin&q;')[0]
        latlng = latlng.split(',&q;Longitude&q;:')
        latitude = latlng[0]
        longitude = latlng[1]
        zipcode,city,address = extract_location_from_coordinates(longitude,latitude)

        images = response.css('.main_wrapper img::attr(src)').extract()




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

        #item_loader.add_value("available_date", available_date) # String => date_format

        #item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        #item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking) # Boolean
        #item_loader.add_value("elevator", elevator) # Boolean
        item_loader.add_value("balcony", balcony) # Boolean
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

        item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", 'HVH, Sonja Hörner') # String
        item_loader.add_value("landlord_phone", '03671-53722') # String
        #item_loader.add_value("landlord_email", landlord_email) # String

        self.position += 1
        yield item_loader.load_item()
