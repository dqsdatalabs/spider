# -*- coding: utf-8 -*-
# Author: Muhammad Ahmad Hesham
import scrapy
from ..loaders import ListingLoader
from ..helper import extract_location_from_coordinates, extract_number_only, format_date, extract_location_from_address
import re
import json


class OakandapplepropertymanagementPyspiderCanadaEnSpider(scrapy.Spider):
    name = "OakAndApplePropertyManagement_PySpider_canada_en"
    start_urls = ['https://oakandapple.managebuilding.com/Resident/public/rentals']
    allowed_domains = ["oakandapple.managebuilding.com"]
    country = 'canada' # Fill in the Country's name
    locale = 'en' # Fill in the Country's locale, look up the docs if unsure
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
        for listing in  response.css("a.featured-listing::attr(href)").getall():
                yield scrapy.Request("https://oakandapple.managebuilding.com/" + listing, callback=self.populate_item)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        external_id = response.url.split('/')[-1]
        title = response.css("h1.title::text").get().strip()
        landlord_name = response.css("h2.footer__title::text").get().strip()
        landlord_number = response.css("div.text--secondary > div > a::text").getall()[0].replace('.', '-')
        landlord_email = response.css("div.text--secondary > div > a::text").getall()[1]
        description = response.css("p.unit-detail__description::text").get().split('\n')
        new_description = []
        for paragraph in description:
            if paragraph.strip() == '':
                continue
            if paragraph.strip() == '_____':
                continue
            if 'agent:' in paragraph.strip().lower():
                landlord_name = paragraph.split(':')[1].strip()
                continue
            if 'phone:' in paragraph.lower() or 'text:' in paragraph.lower():
                landlord_number = paragraph.split(':')[1].strip()
                continue
            if 'email:' in paragraph.strip().lower():
                landlord_email = paragraph.split(':')[1].strip()
                continue
            new_description.append(paragraph)
        description = '\n'.join(new_description)
        counts = response.css("ul.unit-detail__unit-info > li::text").getall()
        for count in counts:
            if 'bed' in count.lower():
                room_count = int(float(extract_number_only(count)))
            if 'bath' in count.lower():
                bathroom_count = int(float(re.findall(r"[-+]?\d*\.\d+|\d+", count)[0]))
        property_type = 'apartment' if room_count > 1 else 'studio'
        available_date = response.css("div.unit-detail__available-date::text").get().replace("Available", "").strip()
        available_date = format_date(available_date, '%m/%d/%Y') if available_date else None
        features = response.css("ul.unit-detail__features-list > li::text").getall()
        pets_allowed = balcony = parking = washing_machine = dishwasher = None
        for feature in features:
            if 'pet' in feature.strip().lower():
                pets_allowed = True
            if 'balcon' in feature.strip().lower():
                balcony = True
            if 'parking' in feature.strip().lower():
                parking = True
            if 'washer' == feature.strip().lower():
                washing_machine = True
            if 'dishwasher' == feature.strip().lower():
                dishwasher = True
        longitude, latitude = extract_location_from_address(response.css("a.icon::attr(href)").get())
        zipcode, city, address = extract_location_from_coordinates(longitude=longitude, latitude=latitude)
        images = response.css("img.unit-detail__gallery-thumbnail::attr(src)").getall()  
        rent = int(float(extract_number_only(response.css("div.unit-detail__price::text").get().strip(), thousand_separator=',', scale_separator='.'))) 
        deposit = int(float(extract_number_only(response.css("div.unit-detail__info > div:nth-last-child(3) > div > p ::text").get().strip(), thousand_separator=',', scale_separator='.')))
        
        item_loader = ListingLoader(response=response)

        # # MetaData
        item_loader.add_value("external_link", response.url) # String
        item_loader.add_value("external_source", self.external_source) # String

        item_loader.add_value("external_id", external_id) # String
        item_loader.add_value("position", self.position) # Int
        item_loader.add_value("title", title) # String
        item_loader.add_value("description", description) # String

        # # Property Details
        item_loader.add_value("city", city) # String
        item_loader.add_value("zipcode", zipcode) # String
        item_loader.add_value("address", address) # String
        item_loader.add_value("latitude", str(latitude)) # String
        item_loader.add_value("longitude", str(longitude)) # String
        #item_loader.add_value("floor", floor) # String
        item_loader.add_value("room_count", room_count) # Int
        item_loader.add_value("bathroom_count", bathroom_count) # Int
        item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
        #item_loader.add_value("square_meters", square_meters) # Int

        item_loader.add_value("available_date", available_date) # String => date_format

        item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        #item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking) # Boolean
        #item_loader.add_value("elevator", elevator) # Boolean
        item_loader.add_value("balcony", balcony) # Boolean
        #item_loader.add_value("terrace", terrace) # Boolean
        #item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        item_loader.add_value("washing_machine", washing_machine) # Boolean
        item_loader.add_value("dishwasher", dishwasher) # Boolean

        # # Images
        item_loader.add_value("images", images) # Array
        item_loader.add_value("external_images_count", len(images)) # Int
        #item_loader.add_value("floor_plan_images", floor_plan_images) # Array

        # # Monetary Status
        item_loader.add_value("rent", rent) # Int
        item_loader.add_value("deposit", deposit) # Int
        #item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        #item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", "CAD") # String

        #item_loader.add_value("water_cost", water_cost) # Int
        #item_loader.add_value("heating_cost", heating_cost) # Int

        #item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name) # String
        item_loader.add_value("landlord_phone", landlord_number) # String
        item_loader.add_value("landlord_email", landlord_email) # String

        self.position += 1
        yield item_loader.load_item()        