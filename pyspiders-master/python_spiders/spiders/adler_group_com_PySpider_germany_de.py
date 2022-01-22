# -*- coding: utf-8 -*-
# Author: Ahmed Hegab
import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import json
from ..helper import extract_location_from_coordinates
import math


class adler_group_com_PySpider_germanySpider(scrapy.Spider):
    name = "adler_group_com"
    start_urls = ['http://www.$domain/']
    allowed_domains = ["adler-group.com"]
    country = 'Germany'
    locale = 'de' 
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing' 

    position = 1

    
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    
    def start_requests(self):
        yield Request(url='https://www.adler-group.com/index.php?tx_immoscoutgrabber_pi2%5B__referrer%5D%5B%40extension%5D=ImmoscoutGrabber&tx_immoscoutgrabber_pi2%5B__referrer%5D%5B%40controller%5D=ShowObjects&tx_immoscoutgrabber_pi2%5B__referrer%5D%5B%40action%5D=index&tx_immoscoutgrabber_pi2%5B__referrer%5D%5Barguments%5D=YTowOnt9a8c432c5d610f7a127b9ad7b9927210c644a29e4&tx_immoscoutgrabber_pi2%5B__referrer%5D%5B%40request%5D=%7B%22%40extension%22%3A%22ImmoscoutGrabber%22%2C%22%40controller%22%3A%22ShowObjects%22%2C%22%40action%22%3A%22index%22%7Ded900a1427eb15aa3501045b5884d881b3eabe8a&tx_immoscoutgrabber_pi2%5B__trustedProperties%5D=%7B%22action%22%3A1%7Df690f3a101d7ed725cb7d4fc8e03afd8552210dc&tx_immoscoutgrabber_pi2%5Baction%5D=getAllInThisRegion&type=4276906',
                    callback=self.parse,
                    body='',
                    method='GET')
    def parse(self, response):
        parsed_response = json.loads(response.body)
        for item in parsed_response:
            url = item['link']
            external_id = item['isid']
            title = item['title']
            latitude = item['lat']
            longitude = item['lon']
            room_count = None
            try:
                room_count = item['rooms']
            except:
                pass
            square_meters = str(item['livingSpace'])
            rent = item['price']
            yield Request(url=url, callback=self.populate_item,
            meta={'external_id':external_id,
                  'title':title,
                  'latitude':str(latitude),
                  'longitude':str(longitude),
                  'room_count':room_count,
                  'square_meters':square_meters,
                  'rent':rent
                  })


    
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        external_id = str(response.meta.get("external_id"))
        title = response.meta.get("title")
        latitude = response.meta.get("latitude")
        longitude = response.meta.get("longitude")
        room_count = response.meta.get("room_count")
        if room_count is None:
            room_count = 1
        room_count = str(room_count)
        if '.5' in room_count:
            room_count = int(math.ceil(float(room_count)))
        else:
            room_count = int(room_count)
        square_meters = response.meta.get("square_meters")
        rent = response.meta.get("rent")
        if '.' in square_meters:
            square_meters = int(square_meters.split('.')[0])
        else:
            square_meters = int(square_meters)
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)
        description = response.css('#furnishing > div > p:nth-child(2)::text').get()
        property_types = response.css('tr:nth-child(3) .expose-spec-value::text').get()
        energy_label = None
        try:
            energy_label = response.css('#heating_energy > div > p:nth-child(4)::text').get()
            energy_label = energy_label.split(': ')[1]
        except:
            pass
        floor = None
        try:
            floor = response.css('tr:nth-child(4) .expose-spec-value::text').get()
            floor = floor.split('von')[0]
        except:
            pass
        available_date = None
        try:
            available_date = response.css("tr:nth-child(5) .expose-spec-value::text").get()
            available_date = available_date.lower()
            if '.' in available_date:
                pass
            else:
                available_date = None
        except:
            pass
        images = response.css('.image-covered').extract()
        for i in range(len(images)):
            images[i] = images[i].split('src="')[1].split('"')[0]
        deposit = None
        try:
            deposit = response.css('.expose-spec-value-wrapper tr:nth-child(4) td+ td::text').get()
            if ',' in deposit:
                deposit = int(deposit.split(',')[0])
            else:
                deposit = int(deposit)
        except:
            pass
        property_type = 'apartment'
        if 'Etagenwohnung' in property_types:
            property_type = 'apartment'
        elif 'Maisonette' in property_types:
            property_type = 'house'
        else: 
            property_type = 'apartment'


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
        #item_loader.add_value("bathroom_count", bathroom_count) # Int

        item_loader.add_value("available_date", available_date) # String => date_format

        #item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        #item_loader.add_value("furnished", furnished) # Boolean
        #item_loader.add_value("parking", parking) # Boolean
        #item_loader.add_value("elevator", elevator) # Boolean
        #item_loader.add_value("balcony", balcony) # Boolean
        #item_loader.add_value("terrace", terrace) # Boolean
        #item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        #item_loader.add_value("washing_machine", washing_machine) # Boolean
        #item_loader.add_value("dishwasher", dishwasher) # Boolean

        # # Images
        item_loader.add_value("images", images) # Array
        item_loader.add_value("external_images_count", len(images)) # Int
        #item_loader.add_value("floor_plan_images", floor_plan_images) # Array

        # # Monetary Status
        item_loader.add_value("rent", rent) # Int
        item_loader.add_value("deposit", deposit) # Int
        #item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        #item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", "EUR") # String

        #item_loader.add_value("water_cost", water_cost) # Int
        #item_loader.add_value("heating_cost", heating_cost) # Int

        item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", "Adler Group") # String
        item_loader.add_value("landlord_phone", "+49 40 882 155 990") # String
        item_loader.add_value("landlord_email", "kundenservice@adler-group.com") # String

        self.position += 1
        yield item_loader.load_item()
