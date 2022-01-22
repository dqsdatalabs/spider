# -*- coding: utf-8 -*-
# Author: Ahmed Omran
import scrapy
from ..loaders import ListingLoader
import requests
import re
from ..helper import *


class WellgroundedrealestateSpider(scrapy.Spider):
    name = "sibelanger"
    start_urls = ['https://sibelanger.com/property-list/']
    country = 'canada'  # Fill in the Country's name
    locale = 'fr'  # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type = 'testing'
    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url=url, method="GET", callback=self.parse)

    # 2. SCRAPING level 2
    def parse(self, response, **kwargs):
        urls = response.xpath('//a[@class="qodef-pli-link qodef-block-drag-link"]/@href').extract()
        for x in urls:
            url = x
            yield scrapy.Request(url=url, callback=self.populate_item)


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
        swimming_pool=None
        external_id = None
        item_loader = ListingLoader(response=response)
        title = "".join(response.xpath('.//h2[@class="qodef-title-title"]//text()').extract()).strip()
        room_count = int("".join(response.xpath('//div[@class="qodef-spec"]/div[1]/div[1]/div/span/text()').extract()).strip())
        try:
            rent = response.xpath('//span[@class="qodef-property-price-value"]/text()').extract()[0].replace(".","")
        except:
            return
        if not room_count:
            property_type = "studio"
            room_count=1
        else:
            property_type = "apartment"
        bathroom_count = int(float("".join(response.xpath('//div[@class="qodef-spec"]/div[1]/div[2]/div/span[@class="qodef-spec-item-value qodef-label-items-value"]/text()').extract()).strip()))
        available = "".join(response.xpath('//div[@class="qodef-spec"]/div[1]/div[8]/div/span/text()').extract()).strip()
        description = "".join(response.xpath('//*[@class="qodef-property-description-items qodef-property-items-style clearfix"]/p/span/text()').extract())
        if description=="" :
            description = "".join(response.xpath('//*[@class="qodef-property-description-items qodef-property-items-style clearfix"]/p/text()').extract())
        images = response.xpath('.//a[@class="qodef-property-single-lightbox"]/@href').extract()
        landlord_name = "".join(response.xpath('//div[@class="qodef-contact-name"]/span[2]/a/text()').extract()).strip()
        if landlord_name=="":
            landlord_name="Bélanger Real Estate"
        landlord_number = "".join(response.xpath('//div[@class="qodef-spec"]/div[1]/div[7]/div/span/text()').extract()).strip()
        address = "".join(response.xpath('//span[@class="qodef-full-address qodef-label-items-item"]/span/text()').extract()).strip()
        external_id="".join(response.xpath('//div[@class="qodef-title-id"]/span[2]/text()').extract()).strip()
        longitude, latitude = extract_location_from_address(address)
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)
        details ="".join(response.xpath('//*[@class="qodef-property-description-items qodef-property-items-style clearfix"]/ul/li/text()').extract()) + " " + description
        details2 = "".join(response.xpath('//*[@class="qodef-feature qodef-feature-active"]/span/text()').extract()).strip()
        if "terrasse" in details.lower() :
            terrace=True
        if "buanderie" in details.lower() :
            washing_machine=True
        if "Meublé" in details2 :
            furnished=True
        if "stationnement" in details.lower() :
            parking=True
        if "ascenseur" in details.lower() :
            elevator=True
        if "piscine" in details.lower() :
            swimming_pool=True
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
        # item_loader.add_value("floor", floor)  # String
        item_loader.add_value("property_type", property_type)  # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters)  # Int
        item_loader.add_value("room_count", room_count)  # Int
        item_loader.add_value("bathroom_count", bathroom_count)  # Int
        item_loader.add_value("available_date", available)  # String => date_format
        # item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking)  # Boolean
        item_loader.add_value("elevator", elevator)  # Boolean
        item_loader.add_value("balcony", balcony)  # Boolean
        item_loader.add_value("terrace", terrace)  # Boolean
        item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        item_loader.add_value("washing_machine", washing_machine)  # Boolean
        item_loader.add_value("dishwasher", dishwasher)  # Boolean

        # # Images
        item_loader.add_value("images", images)  # Array
        item_loader.add_value("external_images_count", len(images))  # Int
        # item_loader.add_value("floor_plan_images", floor_plan_images)  # Array

        # # Monetary Status
        item_loader.add_value("rent", int(rent))  # Int
        # item_loader.add_value("deposit", deposit) # Int
        # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        # item_loader.add_value("utilities", utilities)  # Int
        item_loader.add_value("currency", "CAD")  # String
        #
        # item_loader.add_value("water_cost", water_cost) # Int
        # item_loader.add_value("heating_cost", heating_cost) # Int

        # item_loader.add_value("energy_label", energy_label)  # String

        # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name)  # String
        item_loader.add_value("landlord_phone", landlord_number)  # String
        # item_loader.add_value("landlord_email", landlord_email) # String

        self.position += 1
        yield item_loader.load_item()
