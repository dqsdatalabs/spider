# -*- coding: utf-8 -*-
# Author: Ahmed Hegab
import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import json
import requests


class pures_leben_de_PySpider_germanySpider(scrapy.Spider):
    name = "pures_leben_de"
    start_urls = ['https://www.pures-leben.de/objekte/berlin-bloved/berlin-bloved.html',
    'https://www.pures-leben.de/objekte/duesseldorf-flincarre/duesseldorf-flincarre.html',
    'https://www.pures-leben.de/objekte/duesseldorf-grafenberger-allee/duesseldorf-grafenbergerallee.html',
    'https://www.pures-leben.de/objekte/muenster-von-steuben-strasse/article-68.html',
    'https://www.pures-leben.de/objekte/muenster-universe/article-67.html',
    'https://www.pures-leben.de/objekte/langenfeld-blumenviertel/langenfeld-blumenviertel.html'
    ]
    allowed_domains = ["pures-leben.de"]
    country = 'Germany'
    locale = 'de'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing' 

    position = 1


    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    

    def parse(self, response, **kwargs):
        urls = response.css("#panel > section.no-bottom-margin > div > div > div > div > a::attr(href)").extract()
        for i in range(len(urls)):
            city = response.css("#panel > div.carousel > div.carousel-textlayer > h1::text").get()
            urls[i] = "https://www.pures-leben.de" + urls[i]
            yield Request(url = urls[i],
            callback=self.populate_item,
            meta={
                'city':city
            })
        
        
        

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        city = response.meta.get('city')
        
        title = response.css("#carousel-header > div.carousel-textlayer > h1::text").get()
        extra_info = response.css("#carousel-header > div.carousel-textlayer > div > p::text").get()
        object_type = response.css("#panel > div > div > section.estatedetail--keyfacts > div > div:nth-child(2) > div:nth-child(2) > div.row.estatedetail--keyfacts--facts > div:nth-child(2)::text").get()
        if 'Apartments' in extra_info or 'Apartments' in object_type:
            property_type = 'apartment'
        else:
            property_type = 'house'
        description = response.css("#panel > div > div > section.estatedetail--intro > div > div > div.estatedetail--intro--description::text").get()
        rent = int(response.css("#panel > div > div > section.estatedetail--keyfacts > div > div:nth-child(2) > div:nth-child(2) > div:nth-child(2) > div.col-xs-8.text-right > div > div.estatedetail--keyfacts--rent--number::text").get())
        room_count = response.css("#panel > div > div > section.estatedetail--keyfacts > div > div:nth-child(2) > div:nth-child(2) > div.row.estatedetail--keyfacts--facts > div:nth-child(5)::text").get()
        bathroom_count = None
        try:
            bathroom_counts = response.css("#panel > div > div > section.estatedetail--keyfacts > div > div:nth-child(2) > div:nth-child(2) > div.row.estatedetail--keyfacts--facts > div:nth-child(29)::text").get()
            if 'vorhanden' in bathroom_counts:
                bathroom_count = 1        
        except:
            pass
        if '-' in room_count:
            room_count = int(room_count.split('-')[0])
            bathroom_count = 1
        else:
            room_count = int(room_count)


        square_meters = response.css('#panel > div > div > section.estatedetail--keyfacts > div > div:nth-child(2) > div:nth-child(2) > div.row.estatedetail--keyfacts--facts > div:nth-child(8)::text').get()
        if '-' in square_meters:
            square_meters = int(square_meters.split('- ')[1].split(' ')[0])
        elif '~' in square_meters:
            square_meters = int(square_meters.split('~ ')[1].split(' ')[0])
        else:
            square_meters = int(square_meters.split(' ')[0])
        
        balcony = None
        balconies = response.css("#panel > div > div > section.estatedetail--highlights > div > div:nth-child(3)::text").get()
        if 'Balkon' in balconies:
            balcony = True

        elevators = response.css("#panel > div > div > section.estatedetail--keyfacts > div > div:nth-child(2) > div:nth-child(2) > div.row.estatedetail--keyfacts--facts > div:nth-child(11)::text").get()
        if 'vorhanden' in elevators:
            elevator = True
        else:
            elevator = None
        furnisheds = response.css("#panel > div > div > section.estatedetail--keyfacts > div > div:nth-child(2) > div:nth-child(2) > div.row.estatedetail--keyfacts--facts > div:nth-child(14)::text").get()
        if 'voll möbliert' in furnisheds:
            furnished = True
        elif 'nicht möbliert':
            furnished = False
        else:
            furnished = None
        parking = None
        try:
            parking = response.css("#panel > div > div > section.estatedetail--keyfacts > div > div:nth-child(2) > div:nth-child(2) > div.row.estatedetail--keyfacts--facts > div:nth-child(17)::text").get()
        except:
            pass
        if parking is not None:
            parking = True
        
        floor_plan_imagess = response.css("#panel > div > div > section.estatedetail--fittings > div.row.vcenter > div:nth-child(1) > img::attr(src)").get()
        floor_plan_images = "https://www.pures-leben.de" + floor_plan_imagess
        images = response.css("#carousel-header > div.carousel-inner > div > img::attr(src)").extract()
        for i in range(len(images)):
            images[i] = "https://www.pures-leben.de" + images[i]
        
        address = response.css("#panel > div > div > section.estatedetail--highlights > div > div:nth-child(5)").get()

        responseGeocode = requests.get(f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/find?f=json&text={address}&maxLocations=1")
        responseGeocodeData = responseGeocode.json()

        longitude = responseGeocodeData['locations'][0]['feature']['geometry']['x']
        latitude = responseGeocodeData['locations'][0]['feature']['geometry']['y']

        responseGeocode = requests.get(f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
        responseGeocodeData = responseGeocode.json()
        zipcode = responseGeocodeData['address']['Postal']
        longitude  = str(longitude)
        latitude  = str(latitude)



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
        item_loader.add_value("latitude", latitude) 
        item_loader.add_value("longitude", longitude) # String
        #item_loader.add_value("floor", floor) # String
        item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters) # Int
        item_loader.add_value("room_count", room_count) # Int
        item_loader.add_value("bathroom_count", bathroom_count) # Int

        #item_loader.add_value("available_date", available_date) # String => date_format

        #item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking) # Boolean
        item_loader.add_value("elevator", elevator) # Boolean
        item_loader.add_value("balcony", balcony) # Boolean
        #item_loader.add_value("terrace", terrace) # Boolean
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
        item_loader.add_value("currency", "EUR") # String

        #item_loader.add_value("water_cost", water_cost) # Int
        #item_loader.add_value("heating_cost", heating_cost) # Int

        #item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", 'pures-leben') # String
        item_loader.add_value("landlord_phone", '05931 - 98 99 - 250') # String
        item_loader.add_value("landlord_email", 'info@pro-immoservice.de') # String

        self.position += 1
        yield item_loader.load_item()
