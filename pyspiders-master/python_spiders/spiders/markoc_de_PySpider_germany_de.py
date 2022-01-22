# -*- coding: utf-8 -*-
# Author: Ahmed Hegab
import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import json



class markoc_de_PySpider_germanySpider(scrapy.Spider):
    name = "markoc_de"
    start_urls = ['https://www.markoc.de/vermietung/']
    allowed_domains = ["markoc.de"]
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
        urls = response.css("#sell > div > div > div > div.fusion-text.fusion-text-1 > div > div > div > div > div.imageframe-align-center > span > a::attr(href)").extract()
        titles = response.css("#sell > div > div > div > div.fusion-text.fusion-text-1 > div > div > div > div > div.fusion-text.text > h2 > a::text").extract()
        rents = response.css("#sell > div > div > div > div.fusion-text.fusion-text-1 > div > div > div > div > div.fusion-text.text > p:nth-child(4) > strong::text").extract()
        for i in range(len(urls)):
            title = titles[i]
            rent = rents[i]
            if 'Gewerberäume' in title or 'Büro' in title or 'Laden' in title or 'Gewerbeetage' in title:
                pass
            else:
                yield Request(url = urls[i],
                callback=self.populate_item,
                meta={
                    'title':title,
                    'rent':rent
                })



    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        title = response.meta.get("title") 
        rent = response.meta.get("rent")
        if '.' in rent:
            rent = rent.replace('.','')
        rent = int(rent.split(',')[0])

        descriptions = response.css("tr:nth-child(8) p *::text").extract()
        description = ''
        for i in range(len(descriptions)):
            description = description + " " + descriptions[i]

        square_meters = response.css("tr:nth-child(2) td+ td::text").get()
        square_meters = int(square_meters.split('ca.')[1].split('m²')[0])
        
        if 'Zi-Whg' in title:
            property_type = 'apartment'
        else:
            property_type = 'house'

        utilities = response.css("tr:nth-child(6) td+ td::text").get()
        utilities = int(utilities.split(",")[0])       
        
        energy_labels = response.css("tr:nth-child(12) td+ td::text").get()
        energy_label = None
        if ',' in energy_labels:
            energy_label = energy_labels[-1]

        available_dates = response.css('tr:nth-child(4) td+ td::text').get()
        available_date = None
        if 'ab sofort bis' in available_dates:
            pass
        else:
            available_date = available_dates
        city = response.css(".immo-subline p::text").get()
        city = city.split('-')[0]
        
        furnished = None
        if 'möblierte' in description:
            furnished = True
        elevator = None
        if 'Aufzug' in description:
            elevator = True
        parking = None
        if 'Tiefgaragenstellplatz' in description:
            parking = True
        terrace = None
        if 'Terrasse' in description:
            terrace = True
        washing_machine = None
        if 'Waschmaschine' in description:
            washing_machine = True
        room_count = None
        if 'Schlafraum' in description:
            room_count = 1
        if room_count is None:
            room_count = 1
        bathroom_count = None
        if 'Badezimmer' in description:
            bathroom_count = 1

        images = response.css('.img-responsive::attr(src)').extract()
        floor_plan_images = []
        for i in range(len(images)):
            if 'Grundriss' in images[i]:
                floor_plan_images.append(images[i])
        
        item_loader.add_value("external_link", response.url) # String
        item_loader.add_value("external_source", self.external_source) # String

        #item_loader.add_value("external_id", external_id) # String
        item_loader.add_value("position", self.position) # Int
        item_loader.add_value("title", title) # String
        item_loader.add_value("description", description) # String

        # # Property Details
        item_loader.add_value("city", city) # String
        #item_loader.add_value("zipcode", zipcode) # String
        #item_loader.add_value("address", address) # String
        #item_loader.add_value("latitude", latitude) # String
        #item_loader.add_value("longitude", longitude) # String
        #item_loader.add_value("floor", floor) # String
        item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters) # Int
        item_loader.add_value("room_count", room_count) # Int
        item_loader.add_value("bathroom_count", bathroom_count) # Int

        item_loader.add_value("available_date", available_date) # String => date_format

        #item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking) # Boolean
        item_loader.add_value("elevator", elevator) # Boolean
        #item_loader.add_value("balcony", balcony) # Boolean
        item_loader.add_value("terrace", terrace) # Boolean
        #item_loader.add_value("swimming_pool", swimming_pool) # Boolean
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
        item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", "EUR") # String

        #item_loader.add_value("water_cost", water_cost) # Int
        #item_loader.add_value("heating_cost", heating_cost) # Int

        item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", "Karel Markoc") # String
        item_loader.add_value("landlord_phone", "07153 8377-0") # String
        item_loader.add_value("landlord_email", "immobilien@markoc.de") # String

        self.position += 1
        yield item_loader.load_item()
