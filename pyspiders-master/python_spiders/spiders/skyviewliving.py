# -*- coding: utf-8 -*-
# Author: Ahmed Omran
import scrapy
from scrapy import Selector

from ..loaders import ListingLoader
import requests
import re
from ..helper import *

class skyviewliving(scrapy.Spider):
    name = "skyviewliving"
    start_urls = ['https://skyviewliving.ca/toronto/',
                  'https://skyviewliving.ca/hamilton/'
                  ,"https://skyviewliving.ca/niagara-falls/"
                  ,"https://skyviewliving.ca/burlington/"
                  ,"https://skyviewliving.ca/barrie/",
                  "https://skyviewliving.ca/orangeville/"
                  ,"https://skyviewliving.ca/oshawa/"
                  ,"https://skyviewliving.ca/brockville/"]
    country = 'canada'  # Fill in the Country's name
    locale = 'en'  # Fill in the Country's locale, look up the docs if unsure
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
        urls = response.xpath('//*[@id="locations"]/div/article/div/div/a/@href').extract()
        for x in range(len(urls)):
            url =urls[x]
            yield scrapy.Request(url=url, callback=self.populate_item)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        room_count=None
        parking=None
        elevator=None
        balcony=None
        washing_machine=None
        dishwasher=None
        pets_allowed=None
        terrace=None
        suites = response.xpath('//*[@id="rental-options"]/div/table/tbody/tr')[1:]
        o=0
        for i,suite in enumerate(suites) :
            # it = response.xpath('//*[@id="rental-options"]/div/table/tbody/tr/td/text()').extract()
            available = Selector(text=suite.extract()).xpath('.//td[5]/text()').extract()[0]
            if available == "Waiting List":
                pass
            else:
                item_loader = ListingLoader(response=response)
                title = response.xpath('//*[@id="property-header"]/div/h1/text()').extract()[0]
                address = "".join(response.xpath('//*[@id="property-header"]/div/section/div[1]/div[1]/p[1]/text()').extract()).replace("\n",",")
                longitude, latitude = extract_location_from_address(address)
                zipcode, city, addres = extract_location_from_coordinates(longitude, latitude)
                rent = Selector(text=suite.extract()).xpath('.//td[3]/text()').extract()[0].replace("$","").replace("/mo","").replace(",","")
                room_count1=Selector(text=suite.extract()).xpath('.//td[1]/text()').extract()[0]
                if "One" or "Bachelor" in room_count1:
                    room_count=1
                elif "Two" in room_count1:
                    room_count=2
                elif "Three" in room_count1:
                    room_count=3
                elif "Four" in room_count1:
                    room_count=4
                bathroom_count=int(Selector(text=suite.extract()).xpath('.//td[2]/text()').extract()[0])
                try :
                    floor_plan_images = response.xpath('//*[@id="rental-options"]/div/table/tbody/tr/td[4]/a/@href').extract()
                    if floor_plan_images[i]=='#':
                        floor_plan_images=[]
                except :
                    floor_plan_images =[]
                det = response.xpath('//div[@class="list"]/ul/li/text()').extract()
                for j in det:
                    if "pet" in j.lower():
                        pets_allowed = True
                    if "balcon" in j.lower():
                        balcony = True
                    if "terrace" in j.lower():
                        terrace = True
                description = "".join(response.xpath('//*[@id="property-overview"]/div/article/div[1]/p/text()').extract())
                property_type="apartment" #apartment company
                images = response.xpath('//*[@class="swiper-slide"]/img/@data-src').extract()
                landlord_name = "Skyviewliving"
                landlord_number = response.xpath('//*[@id="property-header"]/div/section/div[1]/div[2]/p[1]/a/text()').extract()[0]
                landlord_email="info@skyviewmgmt.com"
                extras = response.xpath('//div[@class="list"]/div/ul/li/text()').extract()
                for i in extras:
                    if "dishwasher" in i.lower():
                        dishwasher = True
                    if "Laundry" in i.lower():
                        washing_machine = True
                    if "elevator" in i.lower():
                        elevator = True
                park=response.xpath('//div[@class="parking"]/p/text()').extract()
                if park :
                    parking=True
                # # MetaData
                item_loader.add_value("external_link", response.url+f"#{o}")
                o+=1# String
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
                item_loader.add_value("property_type",property_type)  # String => ["apartment", "house", "room", "student_apartment", "studio"]
                # item_loader.add_value("square_meters", square_meters)  # Int
                item_loader.add_value("room_count", room_count)  # Int
                item_loader.add_value("bathroom_count", bathroom_count)  # Int

                item_loader.add_value("available_date", available)  # String => date_format

                item_loader.add_value("pets_allowed", pets_allowed) # Boolean
                # item_loader.add_value("furnished", furnished) # Boolean
                item_loader.add_value("parking", parking)  # Boolean
                item_loader.add_value("elevator", elevator) # Boolean
                item_loader.add_value("balcony", balcony) # Boolean
                item_loader.add_value("terrace", terrace) # Boolean
                # item_loader.add_value("swimming_pool", swimming_pool) # Boolean
                item_loader.add_value("washing_machine", washing_machine)  # Boolean
                item_loader.add_value("dishwasher", dishwasher)  # Boolean

                # # Images
                item_loader.add_value("images", images)  # Array
                item_loader.add_value("external_images_count", len(images))  # Int
                item_loader.add_value("floor_plan_images", floor_plan_images) # Array

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
                item_loader.add_value("landlord_email", landlord_email) # String

                self.position += 1
                yield item_loader.load_item()




