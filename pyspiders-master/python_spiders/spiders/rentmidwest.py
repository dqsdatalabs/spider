# -*- coding: utf-8 -*-
# Author: Ahmed Omran
import scrapy
from ..loaders import ListingLoader
import requests
import re
from ..helper import *
import math


class rentmidwest(scrapy.Spider):
    name = "rentmidwest"
    allowed_domains = ["rentmidwest.com"]
    start_urls = ['https://rentmidwest.com/property-listings/']
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
        urls = response.xpath('//div[@class="property-link"]//a/@href').extract()

        for x in range(len(urls)):
            url = urls[x]
            # print(url)
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
        swimming_pool = None
        suites = response.xpath('//div[@class="property-type wow fadeInRight"]').extract()
        for i in range(len(suites)):
            item_loader = ListingLoader(response=response)
            title = response.xpath('/html/body/div[1]/div[1]/div[1]/div[2]/h1/text()').extract()[0].strip()
            address=""
            add = response.xpath('/html/body/div[1]/div[1]/div[1]/div[2]/h3/a/span/text()').extract()
            for x in add :
                address=address+x+","
            longitude, latitude = extract_location_from_address(address)
            zipcode, city, adress = extract_location_from_coordinates(longitude, latitude)
            rent = response.xpath('/html/body/div[1]/div[2]/div/div[2]/div[3]/div/div[2]/meta[1]/@content').extract()[i]
            square_meters = response.xpath( '/html/body/div[1]/div[2]/div/div[2]/div[3]/div/div[1]/p[1]/text()').extract()[i].replace(" sq ft","")
            num=response.xpath( '/html/body/div[1]/div[2]/div/div[2]/div[3]/div/div[1]/p[2]/text()').extract()[i].strip()
            if "Studio" in num :
                room_count=1
                property_type="studio"
                bathroom_count=1
            else :
                room_count=math.floor(float(num.split(",")[0].replace(" Bed","")))
                bathroom_count=round(float(num.split(",")[1].replace(" Bath","")))
                property_type = "apartment"
            desc = response.xpath('/html/body/div[1]/div[2]/div/div[2]/div[2]/p/text()').extract()
            description="".join(desc)
            images=[]
            imgs = response.xpath('//*[@id="location-images"]/div/div/div[2]').getall()
            for k in imgs :
                images.append(k[49:].replace('\')"></div>','').replace('ge: url(','').replace("'",''))
            landlord_name = "Midwest property managaement"
            landlord_number = "1.844.200.1597"
            extras = response.xpath('//*[@id="lsection-2"]/li/text()').extract()
            extras2 = response.xpath('//*[@id="lsection-1"]/li/text()').extract()
            for j in extras:
                if "dishwasher" in j.lower():
                    dishwasher = True
                if "balconies" in j.lower():
                    balcony = True
                if "parking" in j.lower():
                    parking = True
                if "elevator" in j.lower():
                    elevator = True
                if "pool" in j.lower():
                    swimming_pool = True
                if "laundry" in j.lower():
                    washing_machine = True
            for k in extras2:
                if "dishwasher" in k.lower():
                    dishwasher = True
                if "balcon" in k.lower():
                    balcony = True
                if "parking" in k.lower():
                    parking = True
                if "elevator" in k.lower():
                    elevator = True
                if "pool" in k.lower():
                    swimming_pool = True
                if "laundry" in k.lower():
                    washing_machine = True
            # # MetaData
            item_loader.add_value("external_link", response.url + f"#{i}")  # String
            item_loader.add_value("external_source", self.external_source)  # String
            # item_loader.add_value("external_id", external_id) # String
            item_loader.add_value("position", self.position)  # Int
            item_loader.add_value("title", title)  # String
            item_loader.add_value("description", description)  # String
            # # Property Details
            item_loader.add_value("city", city)  # String
            item_loader.add_value("zipcode", zipcode)  # String
            item_loader.add_value("address", address)  # String
            item_loader.add_value("latitude", str(latitude))  # String
            item_loader.add_value("longitude", str(longitude))  # String
            # item_loader.add_value("floor", floor) # String
            item_loader.add_value("property_type",
                                  property_type)  # String => ["apartment", "house", "room", "student_apartment", "studio"]
            item_loader.add_value("square_meters", square_meters)  # Int
            item_loader.add_value("room_count", room_count)  # Int
            item_loader.add_value("bathroom_count", bathroom_count)  # Int
            # item_loader.add_value("available_date", available)  # String => date_format

            item_loader.add_value("pets_allowed", True) # Boolean
            # item_loader.add_value("furnished", furnished) # Boolean
            item_loader.add_value("parking", parking)  # Boolean
            item_loader.add_value("elevator", elevator)  # Boolean
            item_loader.add_value("balcony", balcony)  # Boolean
            # item_loader.add_value("terrace", terrace) # Boolean
            item_loader.add_value("swimming_pool", swimming_pool)  # Boolean
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
            # item_loader.add_value("utilities", utilities) # Int
            item_loader.add_value("currency", "CAD")  # String
            #
            # item_loader.add_value("water_cost", water_cost) # Int
            # item_loader.add_value("heating_cost", heating_cost) # Int

            # item_loader.add_value("energy_label", energy_label) # String

            # # LandLord Details
            item_loader.add_value("landlord_name", landlord_name)  # String
            item_loader.add_value("landlord_phone", landlord_number)  # String
            # item_loader.add_value("landlord_email", landlord_email) # String

            self.position += 1
            yield item_loader.load_item()
