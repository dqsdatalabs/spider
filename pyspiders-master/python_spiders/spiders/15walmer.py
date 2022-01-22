# -*- coding: utf-8 -*-
# Author: Ahmed Omran
import scrapy
from scrapy import Selector

from ..loaders import ListingLoader
import requests
import re
from ..helper import *


class WellgroundedrealestateSpider(scrapy.Spider):
    name = "15walmer"
    start_urls = ['https://www.15walmer.ca/floorplans']
    country = 'canada'  # Fill in the Country's name
    locale = 'en'  # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type = 'testing'
    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url=url, method="GET", callback=self.parse)

    # 2. SCRAPING level 2
    def parse(self, response, **kwargs):
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
        square_meters = None
        available = None
        suites = response.xpath('//div[@class="suite-wrap"]/div[@class="suite-rate cell"]/span/text()').extract()
        pics = response.xpath('.//div[@class="suite-wrap"]//div[@class="suite-photos cell"]')
        floo = response.xpath('//div[@class="suite-wrap"]/div[@class="suite-floorplans cell"]')

        for i in range(len(suites)):

            floor_plan_images = []
            images = []
            item_loader = ListingLoader(response=response)
            rent = response.xpath('//div[@class="suite-wrap"]/div[@class="suite-rate cell"]/span/text()').extract()[i].replace("$","")
            bathroom_count = int(response.xpath('//div[@class="suite-wrap"]/div[@class="suite-bath cell"]/span[@class="value"]/text()').extract()[i])
            temp = response.xpath('//div[@class="suite-wrap"]/div[@class="suite-type cell"]/text()').extract()[i].split("-")
            title = "".join(temp)
            room_count = int("".join(re.findall(r'\b\d+\b', temp[0])))
            external_id = "".join(temp[1])
            square_meters = response.xpath('//div[@class="suite-wrap"]/div[@class="suite-sqft cell"]/span[@class="value"]/text()').extract()[i]
            images = Selector(text=pics[i].extract()).xpath('.//a/@href').extract()
            floor_plan_images = Selector(text=floo[i].extract()).xpath(".//div/a/@href").extract()
            if floor_plan_images == ["/images/icons/icon-pdf.png"]:
                floor_plan_images = Selector(text=floo[i].extract()).xpath(".//div/a/@data-pdf").extract()
            property_type="apartment"

            # address = l2[l2.index('Wohnfläche')-1].replace("\xa0",",")
            # longitude, latitude = extract_location_from_address(address)
            # zipcode, city, addres = extract_location_from_coordinates(longitude, latitude)
            landlord_number = "(416) 247-4400"
            landlord_name = "Sud Group"
            # # MetaData
            item_loader.add_value("external_link", response.url+f"#{i}")  # String
            item_loader.add_value("external_source", self.external_source)  # String
            item_loader.add_value("external_id", external_id)  # String
            item_loader.add_value("title", title)  # String
            # # Property Details
            # item_loader.add_value("city", city)  # String
            # item_loader.add_value("zipcode", zipcode)  # String
            # item_loader.add_value("address", address)  # String
            # item_loader.add_value("latitude", str(latitude))  # String
            # item_loader.add_value("longitude", str(longitude))  # String
            item_loader.add_value("floor", floor)  # String
            item_loader.add_value("property_type",
                                  property_type)  # String => ["apartment", "house", "room", "student_apartment", "studio"]
            item_loader.add_value("square_meters", square_meters)  # Int
            item_loader.add_value("room_count", room_count)  # Int
            item_loader.add_value("bathroom_count", bathroom_count)  # Int

            item_loader.add_value("available_date", available)  # String => date_format

            # # Images
            item_loader.add_value("images", images)  # Array
            item_loader.add_value("external_images_count", len(images))  # Int
            item_loader.add_value("floor_plan_images", floor_plan_images)  # Array

            # # Monetary Status
            item_loader.add_value("rent", int(rent))  # Int
            item_loader.add_value("deposit", deposit)  # Int
            # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
            item_loader.add_value("utilities", utilities)  # Int
            item_loader.add_value("currency", "CAD")  # String
            #
            # item_loader.add_value("water_cost", water_cost) # Int
            # item_loader.add_value("heating_cost", heating_cost) # Int

            item_loader.add_value("energy_label", energy_label)  # String

            # # LandLord Details
            item_loader.add_value("landlord_name", landlord_name)  # String
            item_loader.add_value("landlord_phone", landlord_number)  # String
            # item_loader.add_value("landlord_email", landlord_email) # String
            yield scrapy.Request(url="https://www.15walmer.ca/amenities", meta={"item_loader":item_loader}, callback=self.amen,dont_filter=True)



    def amen(self,response):
        item_loader=response.meta.get("item_loader")
        description = "".join(response.css('div[class="suite-amenities-container"] ::text').getall())
        pets_allowed, furnished, parking, elevator, balcony, terrace, swimming_pool, washing_machine, dishwasher = get_amenities(description, description, item_loader)
        item_loader.add_value("description", description)  # String
        item_loader.add_value("position", self.position)  # Int
        item_loader.add_value("pets_allowed", pets_allowed)  # Boolean
        item_loader.add_value("furnished", furnished)  # Boolean
        item_loader.add_value("parking", parking)  # Boolean
        item_loader.add_value("elevator", elevator)  # Boolean
        item_loader.add_value("balcony", balcony)  # Boolean
        item_loader.add_value("terrace", terrace)  # Boolean
        item_loader.add_value("swimming_pool", swimming_pool)  # Boolean
        item_loader.add_value("washing_machine", washing_machine)  # Boolean
        item_loader.add_value("dishwasher", dishwasher)  # Boolean
        self.position += 1
        yield item_loader.load_item()



