# -*- coding: utf-8 -*-
# Author: Muhammad Alaa
import scrapy
from ..loaders import ListingLoader
from python_spiders.helper import sq_feet_to_meters, extract_location_from_address, extract_location_from_coordinates
import re



class MenkeholdingslimitedPyspiderCanadaEnSpider(scrapy.Spider):
    name = "MenkeHoldingsLimited_Pyspider_canada_en"
    start_urls = ['https://menkesuites.ca/']
    allowed_domains = ["menkesuites.ca"]
    country = 'canada'
    locale = 'en'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'

    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    # 2. SCRAPING level 2
    def parse(self, response, **kwargs):
        properties = response.css("div.et_pb_portfolio_grid div")
        for property in properties:
            url = property.css("a::attr(href)").get()
            yield scrapy.Request(url, callback=self.populate_item, meta={'title': url[url[:len(url) - 1].rfind('/')+1:len(url) - 1]})

    # 3. SCRAPING level 3
    def populate_item(self, response):
        property_type = "apartment" 
        description = response.css(
            "div.et_pb_text.et_pb_module.et_pb_bg_layout_light.et_pb_text_align_left.et_pb_text_4 p em::text").get()
        if description == None:
            description = response.css(
                "div.et_pb_text.et_pb_module.et_pb_bg_layout_light.et_pb_text_align_left.et_pb_text_4 em::text").get()
        address = response.css(
            "div.et_pb_text.et_pb_module.et_pb_bg_layout_light.et_pb_text_align_left.et_pb_text_2 p a::text").get()  
        title = response.meta["title"] 
        longitude, latitude = extract_location_from_address(address)
        zipcode, city, address = extract_location_from_coordinates(
            longitude, latitude)
        phones = response.css(
            "div.et_pb_text.et_pb_module.et_pb_bg_layout_light.et_pb_text_align_left.et_pb_text_2 p::text").getall()
        if phones[1].strip() == '':
            landlord_phone = phones[2].strip()
        else:
            landlord_phone = phones[1].strip()
        amenities = response.css("div.et_pb_all_tabs ul li::text").getall()
       
        parking = False
        washing_machine = False
        elevator = False
        balcony = False
        water_cost = None
        heater_cost = None
        pets_allowed = False
        dishwasher = False
        for amenity in amenities:
            if "Balcony" in amenity or "balcony" in amenity:
                balcony = True
            if "elevator" in amenity or "Elevator" in amenity:
                elevator = True
            if "Park " in amenity or "park" in amenity:
                parking = True
            if "laundry" in amenity or "Laundry" in amenity or "Washer" in amenity:
                washing_machine = True
            if "water" in amenity or "Water" in amenity:
                water_cost = 0
            if "heat" in amenity or "Heat" in amenity:
                heater_cost = 0
            if "Dishwasher" in amenity:
                dishwasher = True

        images = [response.css("div.et_pb_module.et-waypoint img::attr(src)").get()]
        images += response.css(
            "div.et_pb_text.et_pb_module.et_pb_bg_layout_light.et_pb_text_align_left.et_pb_text_5 a::attr(href)").getall()
        for inx, appartment in enumerate(response.css("table tr")[1:]):
            item_loader = ListingLoader(response=response)
            row = appartment.css("td::text").getall()
            if len(row) == 2:
                row.insert(1, None)
            rooms = row[0]
            if rooms.find("Bachelor") != -1:
                room_count = 1
            else:
                room_count = rooms[0]
            if row[2].find("–") != -1:
                row[2] = row[2][:row[2].find("–")-1]
            square_meters = re.sub("[^0-9]", "", row[2])
            landlord_name = "Menke Holdings Limited"
            if square_meters:
                square_meters = sq_feet_to_meters(square_meters)
                item_loader.add_value("square_meters", square_meters)
            if row[1] != None:
                if row[1].find("–") != -1:
                    row[1] = row[1][:row[1].find("–")-1]
                rent = re.sub("[^0-9]", "", row[1])
                item_loader.add_value("rent", rent)
                deposit = rent
                item_loader.add_value("deposit", deposit)
            else:
                continue
            item_loader.add_value("pets_allowed", pets_allowed)

            item_loader.add_value(
                "external_link", response.url+"#" + str(inx + 1))
            item_loader.add_value("external_source", self.external_source)
            item_loader.add_value("position", self.position)
            item_loader.add_value("title", title)
            item_loader.add_value("description", description)
            item_loader.add_value("city", city)
            item_loader.add_value("address", address)
            item_loader.add_value("property_type", property_type)
            item_loader.add_value("room_count", room_count)
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))
            item_loader.add_value('zipcode', zipcode)
            item_loader.add_value('latitude', str(latitude))
            item_loader.add_value('longitude', str(longitude))
            item_loader.add_value("landlord_name", landlord_name)
            item_loader.add_value('landlord_phone', landlord_phone)
            item_loader.add_value("currency", "CAD")
            item_loader.add_value("parking", parking)
            item_loader.add_value("washing_machine", washing_machine)
            item_loader.add_value("balcony", balcony)
            item_loader.add_value("dishwasher", dishwasher)
            if water_cost:
                item_loader.add_value("water_cost", water_cost)
            if heater_cost:
                item_loader.add_value("heating_cost", heater_cost)
            item_loader.add_value("elevator", elevator)
            self.position += 1
            yield item_loader.load_item()
