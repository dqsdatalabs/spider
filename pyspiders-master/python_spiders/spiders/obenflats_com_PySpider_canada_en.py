# -*- coding: utf-8 -*-
# Author: Ahmed Hegab
import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import json
import requests
from ..helper import extract_location_from_coordinates


class obenflats_com_PySpider_canadaSpider(scrapy.Spider):
    name = "obenflats_com"
    start_urls = ['https://www.obenflats.com/properties#']
    allowed_domains = ["obenflats.com"]
    country = 'Canada' 
    locale = 'en' 
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing' 

    position = 1



    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)



    def parse(self, response, **kwargs):
        urls = response.css("body > section.properties > div > section > div > a::attr(href)").extract()
        availabilities = response.css('body > section.properties > div > section > div > a > div.property-content > span::text').extract()
        for i in range(len(urls)):
            availability = availabilities[i]
            if 'Units Available' in availability:
                urls[i] = "https://www.obenflats.com" + urls[i]
                yield Request(url = urls[i],
                callback=self.populate_item)

            
            
            
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        title = response.css('body > section.content > div > div.main > div > h1::text').get()
        description = response.css("body > section.content > div > div.details > div.description-side > div > p::text").get()
        addresss = response.css('body > section.content > div > div.main > div > div.property-address *::text').extract()
        address = ''
        for i in range(len(addresss)):
            address = address + " " + addresss[i]
        
        latlng = response.css('body > section.content > div > div.details > div.map-side > div.map-container > section > div.hide > div').get()
        latitude = latlng.split('latitude="')[1].split('"')[0]
        longitude = latlng.split('longitude="')[1].split('"')[0]
        
        zipcode , city, address = extract_location_from_coordinates(longitude, latitude)
        

        property_type = response.css("body > section.content > div > div.main > div > span::text").get()
        if 'apartment' in property_type:
            property_type = 'apartment'
        elif 'house' in property_type:
            property_type = 'house'
        else:
            property_type = None
        
        rent = None
        try:
            rent = response.css('.rent::text').get()
        except:
            pass

        if rent is not None:
            square_meters = int(response.css('.h2::text').get())
            rent = int(rent.split('$')[1].split('/')[0])
            floor_plan_images = response.css('.link-floorplan::attr(href)').get()

            content = response.css('.tab-container').extract()
            available_units = content[2]
            room_count = None
            if '1 Bedroom' in available_units:
                room_count = 1
            if room_count is None:
                room_count = 1

            images= response.css('.cover').extract()
            tmp_images = []
            for i in range(len(images)):
                if 'data-src2x="' in images[i]:
                    images[i] = images[i].split('data-src2x="')[1].split('"')[0]
                    tmp_images.append(images[i])
            images = tmp_images
            images = list(dict.fromkeys(images))
            overview = content[0]
            terrace = None
            if 'terrace' in overview:
                terrace = True
            parking = None
            if 'parking' in overview:
                parking = True
            bathroom_count = None
            if 'bathroom' in overview:
                bathroom_count = 1

            item_loader.add_value("external_link", response.url) # String
            item_loader.add_value("external_source", self.external_source) # String

            #item_loader.add_value("external_id", external_id) # String
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

            #item_loader.add_value("pets_allowed", pets_allowed) # Boolean
            #item_loader.add_value("furnished", furnished) # Boolean
            item_loader.add_value("parking", parking) # Boolean
            #item_loader.add_value("elevator", elevator) # Boolean
            #item_loader.add_value("balcony", balcony) # Boolean
            item_loader.add_value("terrace", terrace) # Boolean
            #item_loader.add_value("swimming_pool", swimming_pool) # Boolean
            #item_loader.add_value("washing_machine", washing_machine) # Boolean
            #item_loader.add_value("dishwasher", dishwasher) # Boolean

            # # Images
            item_loader.add_value("images", images) # Array
            item_loader.add_value("external_images_count", len(images)) # Int
            item_loader.add_value("floor_plan_images", floor_plan_images) # Array

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
            item_loader.add_value("landlord_name", "Obenflats") # String
            item_loader.add_value("landlord_phone", "416 633 6236") # String
            item_loader.add_value("landlord_email", "info@obenflats.com") # String

            self.position += 1
            yield item_loader.load_item()
