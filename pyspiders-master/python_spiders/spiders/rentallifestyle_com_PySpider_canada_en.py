# -*- coding: utf-8 -*-
# Author: Ahmed Hegab
import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import json
import requests

class rentallifestyle_com_PySpider_canadaSpider(scrapy.Spider):
    name = "rentallifestyle_com"
    start_urls = ['http://rentallifestyle.com/for-rent-toronto.php']
    allowed_domains = ["rentallifestyle.com"]
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
        urls = response.css("#gallery > li > a.entry-meta::attr(href)").extract()
        for i in range(len(urls)):
            urls[i] = 'http://rentallifestyle.com' + urls[i]
            yield Request(url=urls[i],
            callback=self.populate_item)


    def populate_item(self, response):
        available_dates = response.css('#content > div > section:nth-child(1) > div.one-fourth.column-last > div > p:nth-child(5) > em::text').get()
        if 'Not Available' in available_dates:
            available_dates = 'not available'
        else:
            available_dates = available_dates.split('Available')[1]
        if 'not available' not in available_dates: 
            if 'Please Inquire' in available_dates:
                available_dates = None
            item_loader = ListingLoader(response=response)
            available_date = available_dates
            title = response.css('#page-title > div > h1::text').get()
            external_id = title.split('#')[1]
            description = response.css("#tab2 > p *::text").extract()
            temp = ''
            for i in range(len(description)):
                temp = temp + " " + description[i]
            description = temp
            address = temp.split('Address:')[1]
            latlng = response.css("#tab3 > script").get().split('.LatLng(')[1].split(')')[0]
            latitude = latlng.split(',')[0]
            longitude = latlng.split(',')[1]
            responseGeocode = requests.get(f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
            responseGeocodeData = responseGeocode.json()
            getting_floor = response.css("#content > div > section:nth-child(1) > div.one-fourth.column-last > div > p:nth-child(7) *::text").extract()
            zipcode = getting_floor[-1].split('-')[1].strip()
            city = getting_floor[-1].split('-')[0].strip()
            try:
                zipcode = responseGeocodeData['address']['Postal']
            except:
                pass
            try:
                city = responseGeocodeData['address']['City']
            except:
                pass
            floor = getting_floor[3].strip()
            property_type_rooms_area = response.css("#content > div > section:nth-child(1) > div.one-fourth.column-last > div > p:nth-child(3) *::text").extract()
            prop_area = property_type_rooms_area[0].strip().lower()
            if '~' in prop_area:
                prop_area = prop_area.replace('~','')
            elif '+' in prop_area:
                prop_area = prop_area.replace('+','')
            property_type = None
            if 'condo' in prop_area:
                property_type = 'apartment'
            square_meters = None
            try:
                square_meters = int(prop_area.split('condo')[1].split('sq.')[0])
            except:
                pass
            rooms = property_type_rooms_area[1].strip()
            room_count = rooms.split('—')[1].split('bedrooms')[0]
            bathroom_count = int(rooms.split('/')[1].split('bathrooms')[0])
            if 'Studio' in room_count:
                room_count = 1
            elif '+' in room_count:
                x = int(room_count.split('+')[0])
                y = int(room_count.split('+')[1])
                room_count = x+y
            else:
                room_count = int(room_count)

            rent = response.css("#content > div > section:nth-child(1) > div.one-fourth.column-last > div > p:nth-child(5) > strong::text").get()
            rent = rent.split('$')[1].split('/')[0]
            if ',' in rent:
                rent = int(rent.replace(',',''))
            else:
                rent = int(rent)
            
            
            furnished_unfurnished = response.css("#content > div > section:nth-child(1) > div.one-fourth.column-last > div > p:nth-child(5)::text").get()
            if 'Unfurnished' in furnished_unfurnished:
                furnished = False
            else: 
                furnished = True

            images = response.css("img::attr(src)").extract()

            for i in range(len(images)):
                images[i] = 'http://rentallifestyle.com' + images[i]
            
            counter = 0
            while counter in range(len(images)):
                if 'thumb' in images[counter]:
                    images.pop(counter)
                    continue
                counter = counter + 1 
            
            counter = 0
            while counter in range(len(images)):
                if 'logo' in images[counter]:
                    images.pop(counter)
                    continue
                counter = counter + 1 

            floor_plan_images = []
            for i in range(len(images)):
                if 'floorplan' in images[i]:
                    floor_plan_images.append(images[i])
            if len(floor_plan_images) == 0:
                floor_plan_images = None
            
            parking = None
            swimming_pool = None
            washing_machine = None
            if 'washer' in description:
                washing_machine = True
            if 'PARKING' in description:
                parking = True
            if 'Indoor pool' in description: 
                swimming_pool = True

            if 'For inquiries' in description:
                description = description.split('For inquiries')[0]
            if 'Please call' in description:
                description = description.split('Please call')[0]
                
            
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
            item_loader.add_value("floor", floor) # String
            item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
            item_loader.add_value("square_meters", square_meters) # Int
            item_loader.add_value("room_count", room_count) # Int
            item_loader.add_value("bathroom_count", bathroom_count) # Int

            item_loader.add_value("available_date", available_date) # String => date_format

            #item_loader.add_value("pets_allowed", pets_allowed) # Boolean
            item_loader.add_value("furnished", furnished) # Boolean
            item_loader.add_value("parking", parking) # Boolean
            #item_loader.add_value("elevator", elevator) # Boolean
            #item_loader.add_value("balcony", balcony) # Boolean
            # item_loader.add_value("terrace", terrace) # Boolean
            item_loader.add_value("swimming_pool", swimming_pool) # Boolean
            item_loader.add_value("washing_machine", washing_machine) # Boolean
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
            item_loader.add_value("landlord_name", 'rentallifestyle') # String
            item_loader.add_value("landlord_phone", '(416) 340-9676') # String
            item_loader.add_value("landlord_email", 'info@rentallifestyle.com') # String

            self.position += 1
            yield item_loader.load_item()
