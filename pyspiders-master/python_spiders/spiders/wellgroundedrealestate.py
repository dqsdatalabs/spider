# -*- coding: utf-8 -*-
# Author: Ahmed Omran
import scrapy
from ..loaders import ListingLoader
import requests
import re
from ..helper import *


class WellgroundedrealestateSpider(scrapy.Spider):
    name = "wellgroundedrealestate"
    allowed_domains = ["wellgroundedrealestate.com"]
    start_urls = ['https://www.wellgroundedrealestate.com/apartments-for-rent/cities/toronto',
                  'https://www.wellgroundedrealestate.com/apartments-for-rent/cities/kitchener']
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
        urls = response.xpath('//*[@id="content"]/div/section/div/a/@href').extract()
        for x in range(len(urls)):
            url = "https://www.wellgroundedrealestate.com" + urls[x]
            yield scrapy.Request(url=url, callback=self.populate_item)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        parking = None
        room_count = None
        elevator = None
        balcony = None
        washing_machine = None
        dishwasher = None
        suites = response.xpath('//div[@class="inner-suite"]').extract()
        for i in range(len(suites)):
            available = response.xpath('//div[@class="inner-suite"]//div[@class="suite-availability"]/span/text()').extract()[i].strip()
            if available == "Not Available":
                pass
            else:
                item_loader = ListingLoader(response=response)
                title = response.xpath('//*[@id="content"]/div/div[4]/div/div[1]/h1/text()').extract()
                address = response.xpath('//*[@id="content"]/div/div[4]/div/div[1]/h2/text()').extract()[0].replace(" | ", "")
                longitude, latitude = extract_location_from_address(address)
                zipcode, city, addres = extract_location_from_coordinates(longitude, latitude)
                det = response.xpath('/html/body/section/div[2]/section/div[2]/div/div[2]/ul/li/text()').extract()
                rent = response.xpath('//div[@class="inner-suite"]//div[@class="suite-rate"]/span[@class="rate-amount"]/text()').extract()[i].strip().replace("$", "")
                square_meters = response.xpath('//div[@class="inner-suite"]//div[@class="suite-size"]/text()').extract()
                square_meters = int("".join(square_meters).split()[i])
                available = response.xpath('//div[@class="inner-suite"]//div[@class="suite-availability"]/span/text()').extract()[i].strip()
                try:
                    floor_plan_images =response.xpath('//div[@class="inner-suite"]//div[@class="floorplans-container"]/a/@href').extract()[i].strip()
                except:
                    floor_plan_images = []
                for j in range(len(det)):
                    if "Laundry" or "laundry" in det[j]:
                        washing_machine = True
                    if "parking" in det[j]:
                        parking = True
                    if "Elevator" in det[j]:
                        elevator = True
                description = response.xpath('//div[@class="building-description page-description cms-content"]/p/text()').extract()
                extras = response.xpath('//div[@class="span6 suite-amenities"]/ul/li/text()').extract()
                proptype = response.xpath('//div[@class="inner-suite"]/div/h2/text()').extract()[i]
                if proptype == "Bachelor":
                    property_type = "studio"
                    room_count = 1
                else:
                    property_type = "apartment"
                    room_count = int(re.findall(r'\b\d+\b', proptype)[0])
                images = response.xpath('//*[@class="gallery-image"]/div/img/@src').extract()
                landlord_name = response.xpath('//*[@id="content"]/div/div[4]/div/div[2]/div[1]/div[2]/h2/text()').extract()[0]
                landlord_number = response.xpath('//*[@id="content"]/div/div[4]/div/div[2]/div[1]/div[1]/h2/text()').extract()[0]
                for k in extras:
                    if " dishwasher" in k:
                        dishwasher = True
                    if "Balcon" in k:
                        balcony = True
                    if "laundry" in k:
                        washing_machine = True
                    if "Elevator" in k:
                        elevator = True
                # # MetaData
                item_loader.add_value("external_link", response.url +f"#{i}")  # String
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
                item_loader.add_value("square_meters",square_meters)  # Int
                item_loader.add_value("room_count", room_count)  # Int
                item_loader.add_value("bathroom_count", 1)  # Int

                item_loader.add_value("available_date", available)  # String => date_format

                # item_loader.add_value("pets_allowed", pets_allowed) # Boolean
                # item_loader.add_value("furnished", furnished) # Boolean
                item_loader.add_value("parking", parking)  # Boolean
                item_loader.add_value("elevator", elevator)  # Boolean
                item_loader.add_value("balcony", balcony)  # Boolean
                # item_loader.add_value("terrace", terrace) # Boolean
                # item_loader.add_value("swimming_pool", swimming_pool) # Boolean
                item_loader.add_value("washing_machine", washing_machine)  # Boolean
                item_loader.add_value("dishwasher", dishwasher)  # Boolean

                # # Images
                item_loader.add_value("images", images)  # Array
                item_loader.add_value("external_images_count", len(images))  # Int
                item_loader.add_value("floor_plan_images", floor_plan_images)  # Array

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
