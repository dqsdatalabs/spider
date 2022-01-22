# -*- coding: utf-8 -*-
# Author: Ahmed Omran
import scrapy
from ..loaders import ListingLoader
import requests
import re
from ..helper import *

class MorrisonsellsrealestatePyspiderCanadaEnSpider(scrapy.Spider):
    name = "devcore"
    start_urls = ["https://location.devcore.ca/en/action/for-rent"]
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
        urls = response.xpath('//div[@class="item active"]/a/@href').extract()
        for x in range(len(urls)):
            url = urls[x]
            yield scrapy.Request(url=url, callback=self.populate_item)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        dishwasher=None
        washing_machine=None
        parking=None
        room_count=None
        floor=None
        bathroom_count=None
        item_loader = ListingLoader(response=response)
        # address="".join(response.css("span[class='adres_area'] ::text").getall()).strip()
        ad="".join(response.xpath('//*[@id="accordion_prop_addr"]/div/div/div/div[1]/text()').extract())
        city="".join(response.xpath('//*[@id="accordion_prop_addr"]/div/div/div/div[2]/a/text()').extract())
        address=ad+", "+city
        longitude, latitude = extract_location_from_address(ad)
        zipcode="".join(response.xpath('//*[@id="accordion_prop_addr"]/div/div/div/div[4]/text()').extract())
        title = "".join(response.xpath('//h1[@class="entry-title entry-prop"]/text()').extract())
        property_type="apartment"
        l1 = response.xpath('//*[@id="collapseOne"]/div/div[@class="listing_detail col-md-4"]/strong/text()').extract()
        l2=response.xpath('//*[@id="collapseOne"]/div/div[@class="listing_detail col-md-4"]/text()').extract()
        l2 = [x for x in l2 if x != " "]
        l=dict(zip(l1,l2))
        try:
            rent = int(float(l.get("Price:").replace(",","").replace("$","")))
        except:
            return
        try:
            room_count=int(l.get("Bedrooms:"))
        except:
            room_count=1
        try :
            bathroom_count=int(l.get("Bathrooms:"))
        except:
            pass
        try:
            floor=l.get("Floors No:")
        except:
            pass
        description = "".join(response.css("div[class='wpestate_property_description wrapper_content '] ::text").getall()).strip()
        pets_allowed, furnished, parking, elevator, balcony, terrace, swimming_pool, washing_machine, dishwasher=get_amenities(description,description,item_loader)
        pics = response.xpath('.//div[@class="item"]/@style').extract()
        images=[]
        for x in pics:
            images.append(x.replace("background-image:url(","").replace(")",""))
        landlord_name = "Devcore Group"
        landlord_number = "819-663-7777"
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
        item_loader.add_value("floor", floor) # String
        item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
        # item_loader.add_value("square_meters", square_meters) # Int
        item_loader.add_value("room_count", room_count)  # Int
        item_loader.add_value("bathroom_count", bathroom_count)  # Int

        # item_loader.add_value("available_date", available_date)  # String => date_format

        item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking)  # Boolean
        item_loader.add_value("elevator", elevator) # Boolean
        item_loader.add_value("balcony", balcony) # Boolean
        item_loader.add_value("terrace", terrace) # Boolean
        item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        item_loader.add_value("washing_machine", washing_machine)  # Boolean
        item_loader.add_value("dishwasher", dishwasher)  # Boolean

        # # Images
        item_loader.add_value("images", images)  # Array
        item_loader.add_value("external_images_count", len(images))  # Int
        # item_loader.add_value("floor_plan_images", floor_plan_images) # Array

        # # Monetary Status
        item_loader.add_value("rent", rent)  # Int
        # item_loader.add_value("deposit", deposit) # Int
        # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        # item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", "CAD")  # String

        # item_loader.add_value("water_cost", water_cost) # Int
        # item_loader.add_value("heating_cost", heating_cost) # Int

        # item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name)  # String
        item_loader.add_value("landlord_phone", landlord_number)  # String
        # item_loader.add_value("landlord_email", landlord_email)  # String

        self.position += 1
        yield item_loader.load_item()
