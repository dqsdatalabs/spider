# -*- coding: utf-8 -*-
# Author: Abanoub Moris
import scrapy
from scrapy.http.request import Request
from ..loaders import ListingLoader
from ..helper import *
import re
import json


class ImmobiliaremassariSpider(scrapy.Spider):

    name = "immobiliaremassari"

    country = 'italy'  # Fill in the Country's name
    locale = 'it'  # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'

    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        for page in range(1, 6):
            url = f'https://www.immobiliaremassari.com/web/immobili.asp?tipo_contratto=A&language=ita&pagref=73517&num_page={page}'

            yield Request(url,
                          callback=self.parseApartment,
                          dont_filter=True)

    # 2. SCRAPING level 2

    def parse(self, response):
        pass

    def parseApartment(self, response):

        apartments = response.css(".property-item")
        for apartment in apartments:
            title = " ".join(apartment.css(
                ".info h4::text,.info h3::text").getall())
            city = response.css('.info h4::text').get()
            if 'garage' in title.lower() or 'ufficio' in title.lower():
                continue
            area = apartment.css(
                ".info .features .area::text").get().split('-')
            room_count = '1'
            try:
                room_count = area[1].replace(' ', '')[0]
            except:
                room_count = '1'

            square_meters = area[0]
            if square_meters:
                square_meters = re.search(r'\d+', square_meters)[0]

            url = 'https://www.immobiliaremassari.com'+apartment.css('::attr(onclick)').get().replace(
                'javascript:document.location.href=', '').replace("\"", '').replace('\'', '')
            rent = apartment.css('.price::text').get()

            if rent:
                rent = re.search(
                    r'\d+', rent.replace(',00', '').replace('.', ''))[0]
            else:
                continue

            datausage = {
                'title': title,
                'room_count': room_count,
                'rent': rent,
                'square_meters': square_meters,
                'city': city
            }

            yield Request(url, meta=datausage, callback=self.populate_item, dont_filter=True)

    # 3. SCRAPING level 3

    def populate_item(self, response):

        if 'commerciale' in response.css(".feature-list li::text").get().lower():
            return
        property_type = 'apartment' if 'appartamento' in response.css(
            ".feature-list li::text").get().lower() else 'house'
        if 'house' in property_type:
            return

        floor = response.css('#det_piano .valore::text').get()
        if floor:
            rex = re.search(r'\d+',floor)
            if rex:
                floor=rex[0]

        title = response.meta['title']
        room_count = response.meta['room_count']
        rent = response.meta['rent']
        square_meters = response.meta['square_meters']
        city = response.meta['city']
        bathroom_count=''
        try:
            bathroom_count = response.css(
            ".feature-list li:contains(agni)::text").get()[0]
        except:
            bathroom_count=''
        
        energy_label = response.css(
            ".feature-list li:contains(energ)::text").get()[-1]

        #external_id = response.css(
        #    "#det_rif .valore").get()

        latlng = extract_location_from_address(city+','+'italy')
        latitude = str(latlng[1])
        longitude = str(latlng[0])
        zipcode, city, address = extract_location_from_coordinates(
            longitude, latitude)

        description = remove_white_spaces(
            "".join(response.css(".lt_content.lt_desc::text").getall()))

        images = response.css(".slides li a::attr(href)").getall()

        if int(rent) > 0 and int(rent) < 20000:
            item_loader = ListingLoader(response=response)

            # # MetaData
            item_loader.add_value("external_link", response.url)  # String
            item_loader.add_value(
                "external_source", self.external_source)  # String

            #item_loader.add_value("external_id", external_id)  # String
            item_loader.add_value("position", self.position)  # Int
            item_loader.add_value("title", title)  # String
            item_loader.add_value("description", description)  # String

            # # Property Details
            item_loader.add_value("city", city)  # String
            item_loader.add_value("zipcode", zipcode)  # String
            item_loader.add_value("address", address)  # String
            item_loader.add_value("latitude", latitude)  # String
            item_loader.add_value("longitude", longitude)  # String
            item_loader.add_value("floor", floor)  # String
            item_loader.add_value("property_type", property_type)  # String
            item_loader.add_value("square_meters", square_meters)  # Int
            item_loader.add_value("room_count", room_count)  # Int
            item_loader.add_value("bathroom_count", bathroom_count)  # Int

            #item_loader.add_value("available_date", available_date)

            self.get_features_from_description(
                description+" ".join(response.css(".lt_content.lt_dettagli strong::text").getall()), response, item_loader)

            # # Images
            item_loader.add_value("images", images)  # Array
            item_loader.add_value("external_images_count", len(images))  # Int
            # item_loader.add_value("floor_plan_images", floor_plan_images) # Array

            # # Monetary Status
            item_loader.add_value("rent", rent)  # Int
            # item_loader.add_value("deposit", deposit) # Int
            # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
            # item_loader.add_value("utilities", utilities)  # Int
            item_loader.add_value("currency", "EUR")  # String

            # item_loader.add_value("water_cost", water_cost) # Int
            # item_loader.add_value("heating_cost", heating_cost) # Int

            item_loader.add_value("energy_label", energy_label)  # String

            # # LandLord Details
            item_loader.add_value(
                "landlord_name", 'Immobiliare Massari')  # String
            item_loader.add_value(
                "landlord_phone", '+39 0187 1581408')  # String
            item_loader.add_value(
                "landlord_email", 'info@immobiliaremassari.com')  # String

            self.position += 1
            yield item_loader.load_item()

    Amenties = {
        'pets_allowed': ['pets'],
        'furnished': ['furnish', 'MÖBLIERTES'.lower()],
        'parking': ['parking', 'garage','parcheggio'],
        'elevator': ['elevator', 'aufzug'],
        'balcony': ['balcon', 'balkon'],
        'terrace': ['terrace','terrazz'],
        'swimming_pool': ['pool'],
        'washing_machine': [' washer', 'laundry', 'washing_machine', 'waschmaschine'],
        'dishwasher': ['dishwasher', 'geschirrspüler']
    }

    def get_features_from_description(self, description, response, item_loader):
        description = description.lower()
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
