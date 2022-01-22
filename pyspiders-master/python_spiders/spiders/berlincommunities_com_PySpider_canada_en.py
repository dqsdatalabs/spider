# -*- coding: utf-8 -*-
# Author: Ahmed Hegab
import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import json
from ..helper import extract_location_from_address, extract_location_from_coordinates
import math

class berlincommunities_com_PySpider_canadaSpider(scrapy.Spider):
    name = "berlincommunities_com"
    start_urls = ['https://berlincommunities.com/renters.html']
    allowed_domains = ["berlincommunities.com"]
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
        urls = response.css('body > div.page.text-center > main > section > div > div > div > div:nth-child(2) > div > div > div > a::attr(href)').extract()
        for i in range(len(urls)):
            urls[i] = 'https://berlincommunities.com/' + urls[i]
            title = response.css('body > div.page.text-center > main > section:nth-child('+str(i+1)+') > div > div > div > div:nth-child(2) > div > div > h4::text').get()
            yield Request(url = urls[i],
            callback = self.populate_item,
            meta={
                'title':title
            })
    
    def populate_item(self, response):
        anchor = 1
        counter = 2
        suites = response.css('body > div.page.text-center > main > section:nth-child(1) > div').extract()
        for i in range(len(suites)-1):    
            item_loader = ListingLoader(response=response)
            square_meters = None
            try:
                square_meters = response.css('body > div.page.text-center > main > section:nth-child(1) > div:nth-child('+str(counter)+') > div > div:nth-child(1) > div > div > p::text').get()
                if ',' in square_meters:
                    square_meters = square_meters.replace(',','')
                if '-' in square_meters:
                    square_meters = int(square_meters.split('-')[1].split(' ')[0])
                else:
                    square_meters = int(square_meters.split(' ')[0])
            except:
                pass
            room_count = response.css('body > div.page.text-center > main > section:nth-child(1) > div:nth-child('+str(counter)+') > div > div:nth-child(2) > div > div > p::text').get()
            if 'Plus' in room_count:
                room_count = int(room_count.split(' ')[0]) +1
            else:
                room_count = int(room_count)
            bathroom_count = response.css('body > div.page.text-center > main > section:nth-child(1) > div:nth-child('+str(counter)+') > div > div:nth-child(3) > div > div > p::text').get()
            if '.5' in bathroom_count:
                bathroom_count = int(math.ceil(float(bathroom_count)))
            else:
                bathroom_count = int(bathroom_count)
            floor_plan_images = response.css('body > div.page.text-center > main > section:nth-child(1) > div:nth-child('+str(counter)+') > div > div:nth-child(4) > div > div > p > a:nth-child(1)::attr(href)').extract()
            for i in range(len(floor_plan_images)):
                floor_plan_images[i] = 'https://berlincommunities.com/' + floor_plan_images[i]
            rent = None
            try:
                rent = response.css('body > div.page.text-center > main > section:nth-child(1) > div:nth-child('+str(counter)+') > div > div:nth-child(5) > div > div > p:nth-child(2) > span::text').get()
            except:
                pass
            if rent is None:
                rent = response.css('body > div.page.text-center > main > section:nth-child(1) > div:nth-child('+str(counter)+') > div > div:nth-child(5) > div > div > span::text').get()                              
            
            rent = rent.replace('$','')
            if ',' in rent:
                rent = int(rent.replace(',',''))
            else:
                rent = int(rent)
            
            counter +=1

            title = response.meta.get("title")
            address = title
            longitude, latitude = extract_location_from_address(address)
            zipcode, city, address = extract_location_from_coordinates(longitude, latitude)


            description = response.css('body > div.page.text-center > main > section:nth-child(1) > div:nth-child(1) > div > p *::text').extract()
            temp = ''
            for i in range(len(description)):
                temp = temp + ' ' + description[i]
            description = temp
            temp = temp.lower()

            images = response.css('.thumbnail-default::attr(href)').extract()
            for i in range(len(images)):
                images[i] = 'https://berlincommunities.com/' + images[i]
            if len(images)==0:
                images = response.css('.offset-top-20 img::attr(src)').extract()
                for i in range(len(images)):
                    images[i] = 'https://berlincommunities.com/' + images[i]

        
            amenities = response.css('li::text').extract()
            tempp = ''
            for i in range(len(amenities)):
                tempp = tempp + ' ' + amenities[i].strip()
            tempp = tempp.lower()
            balcony = None
            dishwasher = None
            parking = None
            washing_machine = None
            elevator = None
            if 'laundry' in tempp or 'washer' in tempp:
                washing_machine = True
            if 'dishwasher' in tempp:
                dishwasher = True
            if 'elevator' in tempp:
                elevator = True
            if 'parking' in tempp:
                parking = True
            if 'balconies' in tempp:
                balcony = True


            item_loader.add_value("external_link", response.url+f"#{anchor}") # String
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
            item_loader.add_value("property_type", "apartment") # String => ["apartment", "house", "room", "student_apartment", "studio"]
            item_loader.add_value("square_meters", square_meters) # Int
            item_loader.add_value("room_count", room_count) # Int
            item_loader.add_value("bathroom_count", bathroom_count) # Int

            #item_loader.add_value("available_date", available_date) # String => date_format

            #item_loader.add_value("pets_allowed", pets_allowed) # Boolean
            #item_loader.add_value("furnished", furnished) # Boolean
            item_loader.add_value("parking", parking) # Boolean
            item_loader.add_value("elevator", elevator) # Boolean
            item_loader.add_value("balcony", balcony) # Boolean
            #item_loader.add_value("terrace", terrace) # Boolean
            #item_loader.add_value("swimming_pool", swimming_pool) # Boolean
            item_loader.add_value("washing_machine", washing_machine) # Boolean
            item_loader.add_value("dishwasher", dishwasher) # Boolean

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
            item_loader.add_value("landlord_name", "berlincommunities") # String
            item_loader.add_value("landlord_phone", "226-339-8398") # String
            item_loader.add_value("landlord_email", "hello@BerlinCommunities.com") # String

            anchor += 1
            self.position += 1
            yield item_loader.load_item()
