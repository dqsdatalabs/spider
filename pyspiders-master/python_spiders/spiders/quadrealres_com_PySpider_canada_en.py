# -*- coding: utf-8 -*-
# Author: Ahmed Hegab
import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import json
import requests

class quadrealres_com_PySpider_canadaSpider(scrapy.Spider):
    name = "quadrealres_com"
    start_urls = ['https://www.quadrealres.com/cities.aspx']
    allowed_domains = ["quadrealres.com"]
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
        urls = response.css("#Cities_Outer > div > div > div > div > div > div > a:nth-child(3)::attr(href)").extract()
        for i in range(len(urls)):
            latitude = None
            longitude = None
            try:
                latlng = urls[i].split('(')[1].split(')')[0]
                latitude = latlng.split(',')[0]
                longitude = latlng.split(',')[1]
            except:
                pass
            urls[i] = "https://www.quadrealres.com/" + urls[i]
            yield Request(url=urls[i],
            callback=self.parse_cities,
            meta={
                'latitude':latitude,
                'longitude':longitude
            }
            )
    def parse_cities(self, response, **kwargs):
        urls = response.css(".propertyUrl::attr(href)").extract()
        
        latitude = response.meta.get("latitude")
        longitude = response.meta.get("longitude")
        for i in range(len(urls)):
            yield Request(url = urls[i],
            callback = self.parse_rooms_info,
            meta={
                'latitude':latitude,
                'longitude':longitude
            })
    
    def parse_rooms_info(self, response, **kwargs):
        url = response.url + "floorplans"
        if 'index.aspx' in url:
            url = url.replace('index.aspx','')
        latitude = response.meta.get("latitude")
        longitude = response.meta.get("longitude")
        description = response.css("#InnerContentDiv > p::text").get()
        title = response.css("head > title::text").get()
        body = response.css("body").get()
        images = response.css("#homepage_slider > div > div > img::attr(src)").extract()
        pets_allowed = None
        if 'Pet-Friendly' in body:
            pets_allowed = True

        yield Request(url = url,
        callback = self.populate_item,
        meta={
            'images':images,
            'pets_allowed':pets_allowed,
            'title':title,
            'description':description,
            'latitude':latitude,
            'longitude':longitude
        })


    def populate_item(self, response):
        url = response.url
        url = url.split('floorplans')[0]
        
        room_info = ''
        script = response.css('script:contains("floorplans")::text').extract()
        for i in range(len(script)):
            if 'var pageData' in script[i]:
                room_info = room_info + script[i]
                room_info = room_info.split('floorplans: [')[1]
        info = room_info.split(' ')
        
        area = []
        for i in range(len(info)):
            if 'sqft:' in info[i]:
                area.append(info[i+1])
        for i in range(len(area)):
            area[i] = int(area[i].split(',')[0].replace('"',''))
        beds = []
        for i in range(len(info)):
            if 'beds:' in info[i]:
                beds.append(info[i+1])
        for i in range(len(beds)):
            beds[i] = float(beds[i].split(',')[0])
        ids = []
        for i in range(len(info)):
            if info[i] == 'id:':
                ids.append(info[i+1])
        for i in range(len(ids)):
            ids[i] = ids[i].split(',')[0]
        prices = []
        for i in range(len(info)):
            if info[i] == 'tilePrice:':
                prices.append(info[i+1])
        for i in range(len(prices)):
            prices[i] = float(prices[i].split('$')[1].split('"')[0].replace(',',''))
        baths = []
        for i in range(len(info)):
            if info[i] == 'baths:':
                baths.append(info[i+1])
        for i in range(len(baths)):
            baths[i] = float(baths[i].split(',')[0])
        
        rents = prices
        square_meterss = area
        room_counts = beds
        external_ids = ids
        bathroom_counts = baths

        counter = 1

        for i in range(len(rents)): 
            item_loader = ListingLoader(response=response)
            rent = None
            try:
                rent = int(rents[i])
            except:
                pass
            room_count = room_counts[i]
            if '.5' in str(room_count):
                room_count = int(str(room_count).replace('.5',''))+1
            else:
                room_count = int(room_count)
            if room_count == 0:
                room_count = 1
            bathroom_count = bathroom_counts[i]
            if '.5' in str(bathroom_count):
                bathroom_count = int(str(bathroom_count).replace('.5',''))+1
            else:
                bathroom_count = int(bathroom_count)  
            if bathroom_count == 0:
                bathroom_count = None  
            square_meters = square_meterss[i]
            external_id = external_ids[i]

            pets_allowed = response.meta.get("pets_allowed")
            description = response.meta.get("description")
            title = response.meta.get("title")
            latitude = response.meta.get("latitude")
            longitude = response.meta.get("longitude")
            responseGeocode = requests.get(f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
            responseGeocodeData = responseGeocode.json()
            zipcode = responseGeocodeData['address']['Postal']
            city = responseGeocodeData['address']['City']
            address = responseGeocodeData['address']['Match_addr']
            images = response.meta.get("images")



            if rent is not None and rent != 0:

                item_loader.add_value("external_link", url+f"#{counter}") # String
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
                item_loader.add_value("property_type", "apartment") # String => ["apartment", "house", "room", "student_apartment", "studio"]
                item_loader.add_value("square_meters", square_meters) # Int
                item_loader.add_value("room_count", room_count) # Int
                item_loader.add_value("bathroom_count", bathroom_count) # Int

                #item_loader.add_value("available_date", available_date) # String => date_format

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
                #item_loader.add_value("deposit", deposit) # Int
                #item_loader.add_value("prepaid_rent", prepaid_rent) # Int
                #item_loader.add_value("utilities", utilities) # Int
                item_loader.add_value("currency", "CAD") # String

                #item_loader.add_value("water_cost", water_cost) # Int
                #item_loader.add_value("heating_cost", heating_cost) # Int

                #item_loader.add_value("energy_label", energy_label) # String

                # # LandLord Details
                item_loader.add_value("landlord_name", "QuadReal") # String
                item_loader.add_value("landlord_phone", "(833) 715-2314") # String
                # item_loader.add_value("landlord_email", landlord_email) # String

                counter = counter + 1
                self.position += 1
                yield item_loader.load_item()
