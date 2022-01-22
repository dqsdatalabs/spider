# -*- coding: utf-8 -*-
# Author: Muhammad Alaa
import scrapy
from ..loaders import ListingLoader
from python_spiders.helper import extract_location_from_address, extract_location_from_coordinates, extract_number_only, get_amenities

class LoewImmobilienSpider(scrapy.Spider):
    name = "Loew_Immobilien"
    start_urls = ['https://www.loew-immobilien.de/immobilien-objektsuche.html']
    allowed_domains = ['loew-immobilien.de']
    country = 'germany'
    locale = 'de'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'

    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url=url, callback=self.parse)

    # 2. SCRAPING level 2
    def parse(self, response, **kwargs):
        urls = response.css(
            'div.pagination-container a.number::attr(href)').getall()
        urls = urls[:int(len(urls)/2)]
        for url in urls:
            yield scrapy.Request(url, callback=self.populate_items)

    # 3. SCRAPING level 3
    def populate_items(self, response):
        units = response.css('div.estate-item.clearfix')
        for unit in units:
            yield scrapy.Request(unit.css('div.estate-image a::attr(href)').get(), callback=self.populate_item)
    # 3. SCRAPING level 4

    def populate_item(self, response):
        title = response.css('div.estate-detail span::attr(content)').get()
        external_id = response.css('strong.estate-id::text').get()
        descriptions = response.css('div.estate-detail p::text').getall()[3:]
        description = ''
        for des in descriptions:
            description += des.strip()
        description = description.replace('(*) Pflichtfelder', '')
        full_address = response.css(
            'div[itemprop="address"] p span::text').getall()
        address = ''
        for a in full_address:
            address += a + ' '
        longitude, latitude = extract_location_from_address(address)
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)
        price = response.css('table')[0].css('td::text').getall()
        rent = extract_number_only(price[1]).split('.')[0]
        deposit = extract_number_only(price[7]).split('.')[0]
        utilities = extract_number_only(price[3]).split('.')[0]
        heating_cost = int(extract_number_only(price[5]).split('.')[0]) - int(rent)

        areas = response.css('table')[1].css('td::text').getall()
        square_meters = areas[1].split(',')[0]
        room_count = 1
        bathroom_count = None
        for inx, item in enumerate(areas):
            if 'Zimmer' in item:
                room_count = extract_number_only(areas[inx + 1])
                
            if 'Anzahl Schlafzimmer' in item:
                bathroom_count = extract_number_only(areas[inx + 1])
                
        if type(room_count) is str:
            room_count = room_count[0]
        images = response.css('figure img::attr(src)').getall()
        landlord_info = response.css('div.spalte.last::text').getall()
        floor_plan_images = None
        if response.css('div.estate-detail figure')[-1].css('::attr(title)').get() == 'Grundriss':
            floor_plan_images = [images[-1]]
            images = images[:len(images) - 1]
        landlord_phone = None
        for item in landlord_info:
            if 'Tel' in item:
                landlord_phone = item.split(':')[1].strip()
        

        item_loader = ListingLoader(response=response)
        # # MetaData
        item_loader.add_value("external_link", response.url)  # String
        item_loader.add_value(
            "external_source", self.external_source)  # String
        item_loader.add_value("position", self.position)  # Int

        item_loader.add_value("external_id", external_id)  # String
        item_loader.add_value("title", title)  # String
        item_loader.add_value("description", description)  # String

        # Property Details
        item_loader.add_value("city", city)  # String
        item_loader.add_value("zipcode", zipcode)  # String
        item_loader.add_value("address", address)  # String
        item_loader.add_value("latitude", str(latitude))  # String
        item_loader.add_value("longitude", str(longitude))  # String
        # item_loader.add_value("floor", floor)  # String
        # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("property_type", 'apartment')
        item_loader.add_value("square_meters", square_meters)  # Int
        item_loader.add_value("room_count", room_count)  # Int
        item_loader.add_value("bathroom_count", bathroom_count)  # Int

        # String => date_format
        # item_loader.add_value("available_date", available_date)

        # item_loader.add_value("pets_allowed", pets_allowed)  # Boolean
        # item_loader.add_value("furnished", furnished) # Boolean
        # item_loader.add_value("parking", parking)  # Boolean
        # item_loader.add_value("elevator", elevator)  # Boolean
        # item_loader.add_value("balcony", balcony)  # Boolean
        # item_loader.add_value("terrace", terrace)  # Boolean
        # item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        # item_loader.add_value("washing_machine", washing_machine) # Boolean
        # item_loader.add_value("dishwasher", dishwasher) # Boolean
        get_amenities(description, description, item_loader)

        # Images
        item_loader.add_value("images", images)  # Array
        item_loader.add_value("external_images_count", len(images))  # Int
        item_loader.add_value("floor_plan_images", floor_plan_images)  # Array

        # Monetary Status
        item_loader.add_value("rent", rent)  # Int
        item_loader.add_value("deposit", deposit)  # Int
        # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        item_loader.add_value("utilities", utilities)  # Int
        item_loader.add_value("currency", 'EUR')  # String

        # item_loader.add_value("water_cost", water_cost) # Int
        item_loader.add_value("heating_cost", heating_cost) # Int

        # item_loader.add_value("energy_label", energy_label) # String

        # LandLord Details
        item_loader.add_value("landlord_name", 'LÖW IMMOBILIEN')  # String
        item_loader.add_value("landlord_phone", landlord_phone)  # String
        # item_loader.add_value("landlord_email", landlord_email)  # String
        self.position += 1
        yield item_loader.load_item()