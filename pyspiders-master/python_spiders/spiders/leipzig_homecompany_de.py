# -*- coding: utf-8 -*-
# Author: Adham Mansour
import re

import scrapy
from scrapy import Request

from ..helper import remove_unicode_char, extract_number_only, remove_white_spaces, extract_location_from_address, \
    extract_location_from_coordinates
from ..loaders import ListingLoader


class Leipzig_homecompanyDeSpider(scrapy.Spider):
    name = 'leipzig_homecompany_de'
    allowed_domains = ['leipzig.homecompany.de']
    start_urls = ['https://leipzig.homecompany.de/en/suchergebnis/noreset/1']  # https not http
    country = 'germany'
    locale = 'de'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'
    keywords = {
        'pets_allowed': ['pet'],
        'furnished': ['furnish'],
        'parking': ['parking', 'garage'],
        'elevator': ['elevator'],
        'balcony': ['balcon'],
        'terrace': ['terrace'],
        'swimming_pool': ['pool', 'swim'],
        'washing_machine': ['washing', ' washer', 'laundry'],
        'dishwasher': ['dishwasher']
    }
    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    # 2. SCRAPING level 2
    def parse(self, response, **kwargs):
        rentals = response.css('h3 a::attr(href)').extract()
        for rental in rentals:
            yield Request(url='https://leipzig.homecompany.de/' + rental,
                          callback=self.populate_item)
        if response.css('.nextpage'):
            next_page = response.css('.nextpage::attr(href)').extract_first()
            yield Request(url='https://leipzig.homecompany.de/' + next_page,
                          callback=self.parse)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        if not "purchase" in ' '.join(response.css('span.infoTxt').extract()):
            external_id = response.css("label:contains('Obj.-Nr.') + span::text").extract_first()
            title = response.css('div.content-box h1::text').extract_first()
            description = remove_white_spaces((((' '.join(
                response.css("table.detailsTable th:contains('Description') + td p::text").getall()).replace('\n',
                                                                                                             '')).replace(
                '\t', '')).replace('\r', '')))
            latitude = response.css("div.mapembed::attr(data-lat)").extract_first()
            longitude = response.css("div.mapembed::attr(data-lng)").extract_first()
            zipcode, city, address = extract_location_from_coordinates(longitude, latitude)

            amenities = (" ".join(response.css(".feature-box li ::text").extract())).lower()

            property_type = 'apartment'
            square_meters = response.css("label:contains('Living space ca.') + span::text").extract_first()
            if square_meters:
                square_meters = square_meters.split(",")[0]

            room_count = response.css("label:contains('Room(s)') + span::text").extract_first()
            if room_count:
                room_count = room_count.split(",")[0]
            else:
                room_count = 1

            bathroom_count = response.css("strong:contains('bathroom')::text").extract_first()
            if bathroom_count:
                bathroom_count = 1

            available_date = response.css("label:contains('Available from') + span::text").extract_first()

            images = response.css("ul.slides li img::attr(src)").extract()
            images = ['https://leipzig.homecompany.de/' + i for i in images]
            rent = response.css("span.amount::attr(data-price)").extract_first()

            deposit = response.css("div.price:contains('Deposit') span.amount::attr(data-price)").extract_first()
            energy_label = re.findall("Energy class: ([A-Z])", description)
            if (len(energy_label) > 0):
                energy_label = energy_label[0]
            else:
                energy_label = None

            floor = response.css("li:contains('Floor:')::text").extract_first()

            pets_allowed = None
            if any(word in remove_unicode_char(amenities) for word in self.keywords['pets_allowed']):
                pets_allowed = True

            furnished = None
            if any(word in remove_unicode_char(amenities) for word in self.keywords['furnished']):
                furnished = True

            parking = None
            if any(word in remove_unicode_char(amenities) for word in self.keywords['parking']):
                parking = True

            elevator = None
            if any(word in remove_unicode_char(amenities) for word in self.keywords['elevator']):
                elevator = True

            balcony = None
            if any(word in remove_unicode_char(amenities) for word in self.keywords['balcony']):
                balcony = True

            terrace = None
            if any(word in remove_unicode_char(amenities) for word in self.keywords['terrace']):
                terrace = True

            swimming_pool = None
            if any(word in remove_unicode_char(amenities) for word in self.keywords['swimming_pool']):
                swimming_pool = True

            washing_machine = None
            if any(word in remove_unicode_char(amenities) for word in self.keywords['washing_machine']) or any(
                    word in remove_unicode_char(amenities.lower()) for word in self.keywords['washing_machine']):
                washing_machine = True

            dishwasher = None
            if any(word in remove_unicode_char(amenities) for word in self.keywords['dishwasher']) or any(
                    word in remove_unicode_char(amenities.lower()) for word in self.keywords['dishwasher']):
                dishwasher = True

            landlord_name = "leipzig homecompany"
            landlord_phone = "+ 49 (0) 234 - 19445"
            landlord_email = "leipzig@homecompany.de"

            # # MetaData
            item_loader.add_value("external_link", response.url)  # String
            item_loader.add_value("external_source", self.external_source)  # String

            item_loader.add_value("external_id", external_id)  # String
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

            item_loader.add_value("available_date",
                                  available_date)  # String => date_format also "Available", "Available Now" ARE allowed

            item_loader.add_value("pets_allowed", pets_allowed)  # Boolean
            item_loader.add_value("furnished", furnished)  # Boolean
            item_loader.add_value("parking", parking)  # Boolean
            item_loader.add_value("elevator", elevator)  # Boolean
            item_loader.add_value("balcony", balcony)  # Boolean
            item_loader.add_value("terrace", terrace)  # Boolean
            item_loader.add_value("swimming_pool", swimming_pool)  # Boolean
            item_loader.add_value("washing_machine", washing_machine)  # Boolean
            item_loader.add_value("dishwasher", dishwasher)  # Boolean

            # # Images
            item_loader.add_value("images", images)  # Array
            item_loader.add_value("external_images_count", len(images))  # Int
            # item_loader.add_value("floor_plan_images", floor_plan_images) # Array

            # # Monetary Status
            item_loader.add_value("rent", rent)  # Int
            item_loader.add_value("deposit", deposit)  # Int
            # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
            # item_loader.add_value("utilities", utilities) # Int
            item_loader.add_value("currency", "EUR")  # String

            # item_loader.add_value("water_cost", water_cost) # Int
            # item_loader.add_value("heating_cost", heating_cost) # Int

            item_loader.add_value("energy_label", energy_label)  # String

            # # LandLord Details
            item_loader.add_value("landlord_name", landlord_name)  # String
            item_loader.add_value("landlord_phone", landlord_phone)  # String
            item_loader.add_value("landlord_email", landlord_email)  # String

            self.position += 1
            yield item_loader.load_item()
