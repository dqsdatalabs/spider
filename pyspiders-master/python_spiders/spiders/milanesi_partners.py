# -*- coding: utf-8 -*-
# Author: Ahmed Shahien
import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import requests
from ..helper import *


class MilanesiPartnersSpider(scrapy.Spider):
    name = "milanesi_partners"
    allowed_domains = ["milanesipartners.com"]
    start_urls = ['http://www.milanesipartners.com/r/annunci/affitto-.html?Codice=&Tipologia%5B%5D=0&Motivazione%5B%5D=2&Comune=0&Prezzo_da=&Prezzo_a=&cf=yes']
    country = 'Italy'
    locale = 'it'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'
    position = 1
    # 1. SCRAPING level 1
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse,headers={'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:66.0) Gecko/20100101 Firefox/66.0'})

    # 2. SCRAPING level 2
    def parse(self, response, **kwargs):
        area_urls = response.css('section a::attr(href)').extract()
        for area_url in area_urls:
            yield Request(url=area_url,
                          callback=self.populate_item,headers={'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:66.0) Gecko/20100101 Firefox/66.0'})
        try:
            next_page = response.css('.active+ .num::attr(href)')[0].get()
            yield Request(url=next_page, callback=self.parse,headers={'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:66.0) Gecko/20100101 Firefox/66.0'})
        except:
            pass

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        title = response.css(".titoloscheda::text")[0].extract()
        if "Appartamento" in title:
            property_type = 'apartment'
        elif 'Studio' in title:
            property_type = 'studio'
        elif 'Villa' in title:
            property_type = 'house'
        else:
            return
        external_id = response.css(".codice span::text")[0].extract()
        description = response.css(".realestate-scheda .testo p::text").extract()
        description = ' '.join(description)
        if 'PER INFO' in description:
            description = description.split('PER INFO')[0]
        if 'Per info e/o visite' in description:
            description = description.split('Per info e/o visite')[0]
        if 'Per info' in description:
            description = description.split('Per info')[0]
        if 'Per Info' in description:
            description = description.split('Per Info')[0]
        if 'Per Info e visite' in description:
            description = description.split('Per Info e visite')[0]
        if 'per info e visite' in description.lower():
            info = description.split('visite ')[1].strip()
            landlord_name = info[:-11].strip()
            landlord_phone = info[-11:].strip()
        else:
            landlord_name = 'Milanesi Partners'
            landlord_phone = '0354284641'
        parking = None
        try:
            if 'parcheggio' in description:
                parking = True
        except:
            pass


        latlng = response.css('script:contains("lat")::text').get()
        latitude = latlng.split('lat = "')[1].split('";')[0]
        longitude = latlng.split('lgt = "')[1].split('";')[0]
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)
        room_count = 1
        try:
            room_count = response.css(".realestate-scheda .ico-24-camere span::text")[0].extract()
            room_count = int(room_count[0])
        except:
            pass
        square_meters = None
        try:
            square_meters = response.css(".realestate-scheda .ico-24-mq span::text")[0].extract()
            square_meters = int(square_meters[:-2])
        except:
            pass

        bathroom_count = None
        try:
            bathroom_count = response.css(".realestate-scheda .ico-24-bagni span::text")[0].extract()
            bathroom_count = int(bathroom_count[0])
        except:
            pass

        rent = response.css(".details .prezzo::text")[0].extract()
        if any(char.isdigit() for char in rent):
            rent = int(''.join(x for x in rent if x.isdigit()))
        else:
            return

        images = response.css(".swiper-slide a::attr(href)").extract()

        list = response.css("#sezInformazioni .box strong::text").extract()
        elevator = None
        furnished = None
        utilities = None
        terrace = None
        try:
            elevator_index = [i for i, x in enumerate(list) if "Ascensore" in x][0]
            elevator = response.css("#sezInformazioni .box::text")[elevator_index].extract()
            if "Si" in elevator:
                elevator = True
        except:
            pass
        try:
            utilities_index = [i for i, x in enumerate(list) if "Spese condominio" in x][0]
            utilities = response.css("#sezInformazioni .box::text")[utilities_index].extract()
            if any(char.isdigit() for char in utilities):
                utilities = int(''.join(x for x in utilities if x.isdigit()))
        except:
            pass
        try:
            furnished_index = [i for i, x in enumerate(list) if "Arredato" in x][0]
            furnished = response.css("#sezInformazioni .box::text")[furnished_index].extract()
            if "Sì" in furnished:
                furnished = True
            else:
                furnished = None
        except:
            pass
        try:
            terrace_index = [i for i, x in enumerate(list) if "Terrazzo" in x][0]
            terrace = response.css("#sezInformazioni .box::text")[terrace_index].extract()
            if "Presente" in terrace:
                terrace = True
        except:
            pass


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
        item_loader.add_value("property_type", property_type) # String
        item_loader.add_value("square_meters", square_meters) # Int
        item_loader.add_value("room_count", room_count) # Int
        item_loader.add_value("bathroom_count", bathroom_count) # Int

        #item_loader.add_value("available_date", available_date) # String => date_format also "Available", "Available Now" ARE allowed

        #item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking) # Boolean
        item_loader.add_value("elevator", elevator) # Boolean
        #item_loader.add_value("balcony", balcony) # Boolean
        item_loader.add_value("terrace", terrace) # Boolean
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
        item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", "EUR") # String

        #item_loader.add_value("water_cost", water_cost) # Int
        #item_loader.add_value("heating_cost", heating_cost) # Int

        #item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name) # String
        item_loader.add_value("landlord_phone", landlord_phone) # String
        item_loader.add_value("landlord_email", "milanesi.partners@gmail.com") # String

        self.position += 1
        yield item_loader.load_item()