# -*- coding: utf-8 -*-
# Author: Ahmed Omran
import scrapy
from scrapy import Selector

from ..loaders import ListingLoader
import requests
import re
from ..helper import *


class austeville(scrapy.Spider):
    name = "austeville"
    start_urls = ['https://www.austeville.com/residential']
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
        urls = response.xpath('//span[@class="field-content"]/a/@href').extract()
        for x in urls:
            if "https" in x :
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
        available = None
        pets_allowed = None
        square_meters = None
        swimming_pool = None
        external_id = None
        rent = None
        empty="".join(response.xpath('//div[@class="content"]/div/div/p/text()').extract())
        if empty != "" :
            return
        suites=response.xpath('//*[@class="block block-views block-even clearfix"]/div/div/div/div/table/tbody/tr')
        for i,suite in enumerate (suites):
            item_loader = ListingLoader(response=response)
            rent=int("".join(Selector(text=suite.extract()).xpath('//td[@class="views-field views-field-field-monthlyrent active"]/text()').extract()).strip().replace(",","").replace("$",""))
            available="".join(Selector(text=suite.extract()).xpath('//td[@class="views-field views-field-field-move-date"]/span/text()').extract()).strip()
            description = "".join(response.xpath('//div[@class="views-field views-field-field-building-intro"]/div/p/text()').extract())
            floor_plan_images=Selector(text=suite.extract()).xpath('//td[@class="views-field views-field-field-floor-plans colorbox"]/a/@href').extract()
            square_meters="".join(Selector(text=suite.extract()).xpath('//td[@class="views-field views-field-field-notes"]/text()').extract())
            square_meters=int(re.findall(r'\b\d+\b',square_meters)[0])
            property_type="apartment"
            room_ttile="".join(Selector(text=suite.extract()).xpath('//td[@class="views-field views-field-field-unit-types1"]/text()').extract()).strip()
            room_count=int(re.findall(r'\b\d+\b',room_ttile)[0])
            title = "".join(response.xpath('//*[@class="views-field views-field-title"]/h1/text()').extract()).strip() +" "+ room_ttile
            bathroom_count= 1
            images = Selector(text=suite.extract()).xpath('//td[@class="views-field views-field-field-suite-images"]/a/@href').extract()
            landlord_name = "Austeville Properties Ltd."
            landlord_number = "".join(response.xpath('//div[@class="phone"]/text()').extract()).strip()
            landlord_number="".join(re.findall(r'\b\d+\b',landlord_number))
            landlord_email = "starlight@aplbc.com"
            add=response.xpath('//div[@class="address"]/div/span/text()').extract()
            add2=""
            for x in add :
                add2=" ," + add2 +" ,"+ x
            address ="".join(response.xpath('//div[@class="address"]/div/div/text()').extract())  +  add2
            longitude, latitude = extract_location_from_address(address)
            zipcode, city, address = extract_location_from_coordinates(longitude, latitude)
            pets_allowed, furnished, parking, elevator, balcony, terrace, swimming_pool, washing_machine, dishwasher=get_amenities(description,description,item_loader)
            # # MetaData
            item_loader.add_value("external_link", response.url+f"#{i}")  # String
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
            item_loader.add_value("pets_allowed", pets_allowed)  # Boolean
            item_loader.add_value("furnished", furnished) # Boolean
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
            item_loader.add_value("floor_plan_images", floor_plan_images)  # Array
            # # Monetary Status
            item_loader.add_value("rent", rent)  # Int
            # item_loader.add_value("deposit", deposit) # Int
            # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
            # item_loader.add_value("utilities", utilities)  # Int
            item_loader.add_value("currency", "CAD")  # String
            # item_loader.add_value("water_cost", water_cost) # Int
            # item_loader.add_value("heating_cost", heating_cost) # Int
            # item_loader.add_value("energy_label", energy_label)  # String
            # # LandLord Details
            item_loader.add_value("landlord_name", landlord_name)  # String
            item_loader.add_value("landlord_phone", landlord_number)  # String
            item_loader.add_value("landlord_email", landlord_email)  # String
            self.position += 1
            yield item_loader.load_item()
