# -*- coding: utf-8 -*-
# Author: Ahmed Omran
import scrapy
from ..loaders import ListingLoader
import requests
import re
from ..helper import *


class WellgroundedrealestateSpider(scrapy.Spider):
    name = "sanetraimmobilien"
    start_urls = ['https://sanetra-immobilien.de/properties-search/?status=mieten']
    country = 'germany'  # Fill in the Country's name
    locale = 'de'  # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type = 'testing'
    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url=url, method="GET", callback=self.parse)

    # 2. SCRAPING level 2
    def parse(self, response, **kwargs):
        urls = response.xpath('//div[@class="rh_list_card__wrap"]/figure/a/@href').extract()
        for x in range(len(urls)):
            url = urls[x]
            yield scrapy.Request(url=url, callback=self.populate_item)
        pages=response.xpath('//div[@class="rh_pagination"]/a/@href').extract()[1:]
        for page in pages :
            yield scrapy.Request(url=page, callback=self.parse)


    # 3. SCRAPING level 3
    def populate_item(self, response):
        room_count = None
        bathroom_count = None
        floor = None
        parking = None
        elevator = None
        balcony = None
        washing_machine = None
        dishwasher = None
        utilities = None
        terrace = None
        furnished = None
        property_type = None
        energy_label = None
        deposit = None
        square_meters=None
        heating_cost=None
        external_id = response.xpath('//p[@class="id"]/text()').extract()[0].strip()
        item_loader = ListingLoader(response=response)
        title = "".join(response.xpath('.//h1[@class="rh_page__title"]/text()').extract()).strip()
        try:
            rent = int(response.xpath('//p[@class="price"]/text()').extract()[0].strip().replace("€ Monatlich", "").replace(".", ""))
        except:
            return
        try:
            bathroom_count = int("".join(response.xpath('//div[@class="rh_property__meta prop_bathrooms"]/div/span/text()').extract()))
        except:
            pass
        try:
            square_meters = int(float("".join(response.xpath('//div[@class="rh_property__meta prop_area"]/div/span/text()').extract()).strip().replace("\t\t\t\t\n\t\t\t\t\t\tm²", "")))
        except:
            pass
        try:
            room_count = int("".join(response.xpath('//div[@class="rh_property__meta prop_bedrooms"]/div/span/text()').extract()).replace(".", ""))
            property_type="apartment"
        except:
            return
        extras = response.xpath('//div[@class="rh_content"]/ul/li/text()').extract()
        for j in range(len(extras)):
            if "nebenkosten" in extras[j].lower():
                utilities = int(extras[j].replace("Nebenkosten : ", "").replace(" €", ""))
            if 'kaution' in extras[j].lower():
                deposit = int(extras[j].replace("Kaution : ", "").replace(" €", ""))
            if 'heizkosten' in extras[j].lower():
                heating_cost = int(extras[j].replace("Heizkosten : ", "").replace(" €", ""))
        description = "".join(response.xpath('//div[@class="rh_content"]/p/text()').extract())
        images = response.xpath('//a[@class="swipebox"]/@href').extract()
        landlord_name = "Sanetra Immobilien GmbH"
        landlord_number = "03447 - 89 51 930"
        landlord_email = "info@sanetra-immobilien.de"
        try:
            energy_label = response.xpath('//div[@class="energy-performance"]/ul/li/span/text()').extract()[0]
        except:
            pass

        address = response.xpath('//p[@class="rh_page__property_address"]/text()').extract()[0].strip()
        longitude, latitude = extract_location_from_address(address)
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)
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
        item_loader.add_value("latitude", str(latitude))  # String
        item_loader.add_value("longitude", str(longitude))  # String
        item_loader.add_value("floor", floor)  # String
        item_loader.add_value("property_type", property_type)  # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters)  # Int
        item_loader.add_value("room_count", room_count)  # Int
        item_loader.add_value("bathroom_count", bathroom_count)  # Int

        # item_loader.add_value("available_date", available)  # String => date_format

        # item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        # item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking)  # Boolean
        item_loader.add_value("elevator", elevator)  # Boolean
        item_loader.add_value("balcony", balcony)  # Boolean
        item_loader.add_value("terrace", terrace)  # Boolean
        # item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        # item_loader.add_value("washing_machine", washing_machine)  # Boolean
        # item_loader.add_value("dishwasher", dishwasher)  # Boolean

        # # Images
        item_loader.add_value("images", images)  # Array
        item_loader.add_value("external_images_count", len(images))  # Int
        # item_loader.add_value("floor_plan_images", floor_plan_images)  # Array

        # # Monetary Status
        item_loader.add_value("rent", int(rent))  # Int
        item_loader.add_value("deposit", deposit) # Int
        # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        item_loader.add_value("utilities", utilities)  # Int
        item_loader.add_value("currency", "EUR")  # String
        #
        # item_loader.add_value("water_cost", water_cost) # Int
        item_loader.add_value("heating_cost", heating_cost) # Int

        item_loader.add_value("energy_label", energy_label)  # String

        # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name)  # String
        item_loader.add_value("landlord_phone", landlord_number)  # String
        item_loader.add_value("landlord_email", landlord_email) # String

        self.position += 1
        yield item_loader.load_item()
