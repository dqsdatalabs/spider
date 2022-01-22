# -- coding: utf-8 --
# Author: Ahmed Shahien
import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import requests
from ..helper import *
from bs4 import BeautifulSoup


class LivingHalleSpider(scrapy.Spider):
    name = "living_halle"
    start_urls = ['https://www.living-halle.de/wohnungen-miete/']
    allowed_domains = ["living-halle.de"]
    country = 'Germany'  # Fill in the Country's name
    locale = 'de'  # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}PySpider{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'

    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.pages)

    # 2. SCRAPING level 2
    def pages(self, response, **kwargs):
        next_page = response.css('.pagination a::attr(href)').extract()
        for url in next_page:
            yield Request(url=url, callback=self.parse)
        for url in self.start_urls:
            yield Request(url=url, callback=self.parse)


    def parse(self, response, **kwargs):
        property_urls = response.css('.oo-detailslink a::attr(href)').extract()
        for index, property_url in enumerate(property_urls):
            list = response.css(f'.oo-listobject:nth-child({index+1}) .oo-listinfotable ::text').extract()
            title = response.css(f'.oo-listobject:nth-child({index+1}) .oo-listtitle ::text')[0].extract()
            yield Request(url=property_url, callback=self.populate_item, meta={'list': list, 'title': title})

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        title = response.meta['title']
        list = response.meta['list']
        property_type = list[2]
        if 'Wohnung' in property_type:
            property_type = 'apartment'
        zipcode = list[8]
        longitude = list[-2]
        latitude = list[-4]
        zipcode, city, address = extract_location_from_coordinates(longitude,latitude)

        list = ' '.join(list)
        if 'Kaltmiete' in list:
            rent = list.split('Kaltmiete ')[1].split(',')[0]
            rent = int(''.join(x for x in rent if x.isdigit()))

        room_count = None
        if 'Zimmer' in list:
            room_count = int(list.split('Zimmer ')[1].split(' ')[0])
        bathroom_count = None
        if 'Badezimmer' in list:
            bathroom_count = int(list.split('Badezimmer ')[1].split(' ')[0])

        external_id = None
        if 'ImmoNr' in list:
            external_id = list.split('ImmoNr ')[1].split(' ')[0]

        square_meters = None
        if 'Wohnfläche' in list:
            square_meters = list.split('Wohnfläche ca. ')[1].split(' m²')[0]
            square_meters = int(''.join(x for x in square_meters if x.isdigit()))

        inner_list = response.css(".oo-detailslisttd ::text").extract()
        inner_list = ' '.join(inner_list)

        utilities = None
        if ' € Nebenkosten' in inner_list:
            utilities = inner_list.split(' € Nebenkosten ')[1].split(',')[0]
            utilities = int(''.join(x for x in utilities if x.isdigit()))
            heating_cost = None
            if 'Heizkosten in Nebenkosten enthalten ' in inner_list:
                heat = inner_list.split('Heizkosten in Nebenkosten enthalten ')[1].split(' ')[0]
                if 'ja' in heat.lower():
                    warm_rent = inner_list.split('Warmmiete ')[1].split(',')[0]
                    warm_rent = int(''.join(x for x in warm_rent if x.isdigit()))
                    heating_cost = warm_rent - rent - utilities
                    if heating_cost == 0:
                        heating_cost = None

        balcony = None
        if 'Balkon' in inner_list:
            balcony = inner_list.split('Balkon ')[1].split(' ')[0]
            if 'ja' in balcony.lower():
                balcony = True
            else:
                balcony = False
        terrace = None
        if 'Terrasse' in inner_list:
            terrace = inner_list.split('Terrasse ')[1].split(' ')[0]
            if 'ja' in terrace.lower():
                terrace = True
            else:
                terrace = False
        elevator = None
        if 'Fahrstuhl' in inner_list:
            elevator = inner_list.split('Fahrstuhl ')[1].split(' ')[0]
            if 'Kein' in elevator:
                elevator = False
            else:
                elevator = True

        description = response.css(".oo-detailsfreetext ::text").extract()
        description = ' '.join(description)
        description = description_cleaner(description)
        landlord_name = response.css("strong ::text")[0].extract()
        if 'firma' in landlord_name.lower():
            landlord_name = 'Living Halle'
        landlord_phone = '0345 977 30 895'
        try:
            landlord_phone = response.css(".oo-aspcontact span ::text")[0].extract()
        except:
            pass

        images = response.css('.oo-detailspicture').extract()
        images = [image.split("url(\'")[1].split("\');")[0] for image in images]
        images = [image.replace("amp;", "") for image in images]

        # # MetaData
        item_loader.add_value("external_link", response.url)  # String
        item_loader.add_value("external_source", self.external_source)  # String

        item_loader.add_value("external_id", external_id) # String
        item_loader.add_value("position", self.position)  # Int
        item_loader.add_value("title", title) # String
        item_loader.add_value("description", description) # String

        # # Property Details
        item_loader.add_value("city", city) # String
        item_loader.add_value("zipcode", zipcode) # String
        item_loader.add_value("address", address) # String
        item_loader.add_value("latitude", latitude) # String
        item_loader.add_value("longitude", longitude) # String
        # item_loader.add_value("floor", floor) # String
        item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters) # Int
        item_loader.add_value("room_count", room_count) # Int
        item_loader.add_value("bathroom_count", bathroom_count) # Int

        # item_loader.add_value("available_date", available_date) # String => date_format

        # item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        # item_loader.add_value("furnished", furnished) # Boolean
        # item_loader.add_value("parking", parking) # Boolean
        item_loader.add_value("elevator", elevator) # Boolean
        item_loader.add_value("balcony", balcony) # Boolean
        item_loader.add_value("terrace", terrace) # Boolean
        # item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        # item_loader.add_value("washing_machine", washing_machine) # Boolean
        # item_loader.add_value("dishwasher", dishwasher) # Boolean

        # # Images
        item_loader.add_value("images", images) # Array
        item_loader.add_value("external_images_count", len(images)) # Int
        # item_loader.add_value("floor_plan_images", floor_plan_images) # Array

        # # Monetary Status
        item_loader.add_value("rent", rent) # Int
        # item_loader.add_value("deposit", deposit) # Int
        # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", "EUR") # String

        # item_loader.add_value("water_cost", water_cost) # Int
        item_loader.add_value("heating_cost", heating_cost) # Int

        # item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name) # String
        item_loader.add_value("landlord_phone", landlord_phone) # String
        item_loader.add_value("landlord_email", 'info@living-halle.de') # String

        self.position += 1
        yield item_loader.load_item()