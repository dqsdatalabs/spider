# -*- coding: utf-8 -*-
# Author: Ahmed Omran
import scrapy
from scrapy import Request
from ..helper import sq_feet_to_meters ,extract_location_from_address
from ..loaders import ListingLoader


class graydonhallapartments(scrapy.Spider):
    name = "graydonhallapartments"
    allowed_domains = ['graydonhallapartments.com']
    start_urls = ['https://www.graydonhallapartments.com/suites']
    country = 'canada'  # Fill in the Country's name
    locale = 'en'  # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type = 'testing'
    position = 1
    # 1. SCRAPING level 1
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    # 2. SCRAPING level 2
    def parse(self, response, **kwargs):
        urls = response.xpath('//*[@class="_1ozXL"]//div//div//a//@href').getall()
        urlss=dict(zip(urls,urls))
        for x,y in urlss.items():
            url = x
            yield Request(url=url, callback=self.populate_item)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        try:
            rent = response.xpath('//h3[@class="font_3"]/text()').getall()[2].split("-")
            rent = rent[0].replace("$", "").replace(",", "")
        except:
            rent = response.xpath('//h3[@class="font_3"]/text()').getall()[2]
            rent = rent.replace("$", "").replace(",", "")
        description = response.xpath('//*[@class="font_6"]/span/text()').getall()[0]
        title = response.xpath('//h1[@class="font_0"]/span/text()').getall()[0]
        util = response.xpath('//h3[@class="font_3"]/text()').getall()
        room_count = util[0].replace(" Bedroom","")
        square_meters = int(util[1].replace(" SQFT","").replace(",",""))
        images = response.xpath('//*[@class="gallery-item-wrapper visible cube-type-fill"]/div/picture/img/@src').getall()
        floor_plan_images = response.xpath('//*[@class="gallery-item-wrapper visible cube-type-fit"]/div/a/div/picture/img/@src').getall()
        address= "Graydon Hall Drive, North York, Toronto"
        longitude,latitude=extract_location_from_address(address)
        longitude=str(longitude)
        latitude=str(latitude)
        city="Toronto"
        zipcode="M3A3A9"
        item_loader = ListingLoader(response=response)
        # # MetaData
        item_loader.add_value("external_link", response.url)  # String
        item_loader.add_value("external_source", self.external_source)  # String
        # item_loader.add_value("external_id", external_id) # String
        item_loader.add_value("position", self.position)  # Int
        item_loader.add_value("title", title)  # String
        item_loader.add_value("description", description)  # String
        # # Property Details
        item_loader.add_value("city", city) # String
        item_loader.add_value("zipcode", zipcode) # String
        item_loader.add_value("address", address) # String
        item_loader.add_value("latitude", latitude) # String
        item_loader.add_value("longitude", longitude) # String
        # item_loader.add_value("floor", floor) # String
        item_loader.add_value("property_type", 'apartment') # String
        item_loader.add_value("square_meters", square_meters)  # Int
        item_loader.add_value("room_count", room_count)  # Int
        item_loader.add_value("bathroom_count", 1) # Int
        # item_loader.add_value("available_date", available_date) # String => date_format
        item_loader.add_value("pets_allowed", True) # Boolean
        # item_loader.add_value("furnished", furnished) # Boolean
        # item_loader.add_value("parking", parking) # Boolean
        # item_loader.add_value("elevator", elevator) # Boolean
        item_loader.add_value("balcony", True) # Boolean
        # item_loader.add_value("terrace", terrace) # Boolean
        # item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        # item_loader.add_value("washing_machine", washing_machine) # Boolean
        item_loader.add_value("dishwasher", True) # Boolean
        # # Images
        item_loader.add_value("images", images) # Array
        item_loader.add_value("external_images_count", len(images)) # Int
        item_loader.add_value("floor_plan_images", floor_plan_images) # Array
        # # Monetary Status
        item_loader.add_value("rent", rent)  # Int
        # item_loader.add_value("deposit", deposit) # Int
        # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        # item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", "CAD")  # String
        # item_loader.add_value("water_cost", water_cost) # Int
        # item_loader.add_value("heating_cost", heating_cost) # Int
        response.xpath('//*[@class="gallery-item-wrapper visible cube-type-fill"]/div/picture/img/@src').getall()
        # item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", "Graydon Hall Apartment") # String
        item_loader.add_value("landlord_phone", "416-447-2447") # String
        item_loader.add_value("landlord_email", "leasing@ghcapital.ca") # String

        self.position += 1
        yield item_loader.load_item()
