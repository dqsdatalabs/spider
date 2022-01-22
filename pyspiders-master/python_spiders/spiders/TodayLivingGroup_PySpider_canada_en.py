# -*- coding: utf-8 -*-
# Author: Muhammad Ahmad Hesham
import scrapy
from ..loaders import ListingLoader
from ..helper import extract_location_from_coordinates, extract_number_only
import re
import json


class TodaylivinggroupPyspiderCanadaEnSpider(scrapy.Spider):
    name = "TodayLivingGroup_PySpider_canada_en"
    start_urls = ['https://todaylivinggroup.com/?search-listings=true']
    allowed_domains = ["todaylivinggroup.com"]
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
        if response.css("li#next-page-link > a::attr(href)").get() is not None:
            for listing in  response.css("h4 > a::attr(href)").getall():
                yield scrapy.Request(listing, callback=self.populate_item)
            yield response.follow(response.css("li#next-page-link > a::attr(href)").get(), callback=self.parse)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        rent = response.css("h4.price > span.listing-price::text").get()
        if rent is None:
            return
        rent = int(float(extract_number_only(rent, thousand_separator=',', scale_separator='.')))
        price_postfix = response.css("h4.price > span.listing-price-postfix::text").get()
        if price_postfix and 'night' in price_postfix:
            rent = rent * 30
        
        
        external_id = response.css("li.propid > span.right::text").get()
        title = response.css("h1#listing-title::text").get()
        description = response.css("div#listing-content > p::text").getall()
        # description = [pragraph for pragraph in description if not any(x in pragraph.lower() for x in ['email', 'facebook', 'website', 'https', 'today living'])]
        location_array = response.css("input[name='daddr']::attr(value)").get().split(',')
        if not location_array or len(location_array) != 2:
            return
            
        latitude = location_array[0]
        longitude = location_array[1]
        zipcode, city, address = extract_location_from_coordinates(longitude=longitude, latitude=latitude)
        property_type = response.css("li.property-type > span.right::text").get()
        if not property_type or not 'condo' in property_type.lower():
            return
        property_type = 'apartment'
        square_meters = int(float(extract_number_only(response.css("li.sqft > span.right::text").get(), thousand_separator=',', scale_separator='.')))
        if square_meters == 0:
            square_meters = None
        room_count = int(float(extract_number_only(response.css("li.beds > span.right::text").get(), thousand_separator=',', scale_separator='.')))
        bathroom_count = int(float(extract_number_only(response.css("li.baths > span.right::text").get(), thousand_separator=',', scale_separator='.')))
        pet_policy = response.css("li.pets > span.right::text").get()
        pets_allowed = None
        if pet_policy:
            pets_allowed = True if not any(x in pet_policy.lower() for x in ['no', 'restriction']) else False
        furnished = response.css("h6.snipe.status > span::text").get()
        if furnished:
            furnished = True if furnished.lower().strip() == 'furnished' else False
        parking = balcony = washing_machine = None
        for feature in response.css("ul.propfeatures > li::text").getall():
            if 'parking' in feature.lower():
                parking = True
            if 'balcon' in feature.lower():
                balcony = True
            if 'washer' in feature.lower():
                washing_machine = True
        images = response.css("img.listings-slider-image::attr(src)").getall()  
        if len(images) == 0:
            images = response.css("img.attachment-listings-slider-image::attr(src)").getall()
        landlord_name = response.css("h4.border-bottom > a::text").get()
        landlord_email = response.css("input#ctyouremail::attr(value)").get()
        landlord_number = response.css("li.company-phone::text").get().strip()   
            
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
        item_loader.add_value("latitude", latitude) # String
        item_loader.add_value("longitude", longitude) # String
        #item_loader.add_value("floor", floor) # String
        item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters) # Int
        item_loader.add_value("room_count", room_count) # Int
        item_loader.add_value("bathroom_count", bathroom_count) # Int

        #item_loader.add_value("available_date", available_date) # String => date_format

        item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking) # Boolean
        #item_loader.add_value("elevator", elevator) # Boolean
        item_loader.add_value("balcony", balcony) # Boolean
        #item_loader.add_value("terrace", terrace) # Boolean
        #item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        item_loader.add_value("washing_machine", washing_machine) # Boolean
        #item_loader.add_value("dishwasher", dishwasher) # Boolean

        # # Images
        item_loader.add_value("images", images) # Array
        item_loader.add_value("external_images_count", len(images)) # Int
        #item_loader.add_value("floor_plan_images", floor_plan_images) # Array

        # # Monetary Status
        item_loader.add_value("rent", rent) # Int
        #item_loader.add_value("deposit", deposit) # Int
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
