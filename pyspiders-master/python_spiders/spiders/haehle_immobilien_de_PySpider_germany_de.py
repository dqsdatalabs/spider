# -*- coding: utf-8 -*-
# Author: Ahmed Hegab
import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import json
from ..helper import extract_location_from_coordinates

class haehle_immobilien_de_PySpider_germanySpider(scrapy.Spider):
    name = "haehle_immobilien_de"
    start_urls = ['https://www.haehle-immobilien.de/miete.xhtml?f[1371-9]=miete&p[obj0]=1']
    allowed_domains = ["haehle-immobilien.de"]
    country = 'Germany'
    locale = 'de' 
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing' 

    position = 1
    page_number = 2
    
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    
    def parse(self, response, **kwargs):
        urls = response.css('div > div > div.image > a::attr(href)').extract()
        for i in range(len(urls)):
            urls[i] = 'https://www.haehle-immobilien.de/' + urls[i] 
            external_id = urls[i].split('=')[1]
            rent = response.css('div:nth-child('+str(i+1)+') > div > p > ins.kaltmiete > span > span::text').get()
            rent = int(rent.split(' ')[0])
            room_count = int(response.css('div:nth-child('+str(i+1)+') > div > div.details.area-details > div:nth-child(2) > span > span::text').get())
            square_meters = response.css('div:nth-child('+str(i+1)+') > div > div.details.area-details > div:nth-child(1) > span > span.object-area-value::text').get()
            square_meters = int(square_meters.split(' ')[0])
            reserved = None
            try:
                reserved = response.css('div:nth-child('+str(i+1)+') > div > div.image > a > ul > li > span').get()
            except:
                pass
            apartment_type = response.css('div:nth-child('+str(i+1)+') > div > div.details.area-details > div:nth-child(1) > span > span.object-area-label::text').get()
            
            if reserved is None:
                if 'Wohnfläche' in apartment_type:
                    yield Request(url= urls[i],
                    callback= self.populate_item,
                    meta={
                        'external_id':external_id,
                        'rent':rent,
                        'room_count':room_count,
                        'square_meters':square_meters
                    }
                    )
        next_page = ("https://www.haehle-immobilien.de/miete.xhtml?f[1371-9]=miete&p[obj0]="+ str(haehle_immobilien_de_PySpider_germanySpider.page_number))
        if haehle_immobilien_de_PySpider_germanySpider.page_number <= 5:
            haehle_immobilien_de_PySpider_germanySpider.page_number += 1
            yield response.follow(next_page, callback=self.parse)

    
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        external_id = response.meta.get('external_id')
        rent = response.meta.get('rent')
        room_count = response.meta.get('room_count')
        square_meters = response.meta.get('square_meters')
        images = response.css('body > section.content.grid > div > div > div.column.two-thirds > div > div.gallery > div.fotorama > div').extract()
        for i in range(len(images)):
            images[i] = images[i].split('data-img="')[1].split('"')[0]
        latlng = response.css('body > section.content.grid > div > div > div.column.two-thirds > div > div.information > div > script::text').get()
        latlng = latlng.replace("'","")
        latitude = latlng.split('"lat": ')[1].split(',')[0]
        longitude = latlng.split('"lng": ')[1].split(' ')[0].strip()
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)
        title = response.css('body > section.content.grid > div > div > div.column.two-thirds > div > h2::text').get()
        description = response.css('body > section.content.grid > div > div > div.column.two-thirds > div > div.information > span:nth-child(1) > span *::text').extract()
        temp = ''
        for i in range(len(description)):
            temp = temp + ' ' + description[i]
        description = temp
        

        amenities = response.css('body > section.content.grid > div > div > div.column.two-thirds > div > div.details > div.details-desktop').get()
        
        

        floor = None
        try:
            floor = amenities.split('Etage</strong></td><td><span>')[1].split('</span>')[0]
        except:
            pass
        utilities = None
        try:
            utilities = amenities.split('Betriebskosten</strong></td><td><span>')[1].split('€</span></td>')[0]
            utilities = int(utilities)
        except:
            pass

        deposit = None
        try:
            deposit = amenities.split('Kaution</strong></td><td><span>')[1].split('€</span></td>')[0]
            if '.' in deposit:
                deposit = deposit.replace('.','')
            deposit = int(deposit)
        except:
            pass

        parking = None
        elevator = None
        if 'Tiefgarage' in amenities:
            parking = True
        if 'fahrstuhl' or 'aufzug' in amenities.lower():
            elevator = True

        bathroom_count = None
        if 'Bad' in amenities:
            bathroom_count = 1

        
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
        item_loader.add_value("property_type", "apartment") # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters) # Int
        item_loader.add_value("room_count", room_count) # Int
        item_loader.add_value("bathroom_count", bathroom_count) # Int

        #item_loader.add_value("available_date", available_date) # String => date_format

        #item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        #item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking) # Boolean
        item_loader.add_value("elevator", elevator) # Boolean
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
        item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", "EUR") # String

        #item_loader.add_value("water_cost", water_cost) # Int
        #item_loader.add_value("heating_cost", heating_cost) # Int

        #item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", "K. Hähle Immobilien") # String
        item_loader.add_value("landlord_phone", "+ 49 351 801 18 77") # String
        item_loader.add_value("landlord_email", "info@haehle-immobilien.de") # String

        self.position += 1
        yield item_loader.load_item()
