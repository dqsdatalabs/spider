# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader
from python_spiders.helper import extract_location_from_address, extract_location_from_coordinates, remove_white_spaces

class Livefurnished_caSpider(Spider):
    name = 'livefurnished_ca'
    country='canada'
    locale='en' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.livefurnished.ca"]
    start_urls = ["https://livefurnished.ca/find-a-home/"]
    position = 1

    def parse(self, response):
        for url in response.css("a:contains('View Furnished Suites')::attr(href)").getall():
            yield Request(response.urljoin(url), callback = self.get_pages, dont_filter = True)

    def get_pages(self, response):
        for url in response.css("div.link-container a.propLink::attr(href)").getall():
            yield Request(response.urljoin(url), callback=self.populate_item, dont_filter = True)
        
        next_page_script = response.css("script:contains('data-page')::text").get()
        if( next_page_script):
            next_page = re.findall(r'next\\\" data-page=\\\"([0-9]+)\\\">', next_page_script)
            if (len(next_page) > 0):
                next_page = next_page[0]
                url = response.url.split("?")[0]
                rest_url = f"{url}?_paged={next_page}"
                yield response.follow(rest_url, callback=self.get_pages, dont_filter = True)

    def populate_item(self, response):
        
        description = remove_white_spaces(" ".join(response.css(".all-the-steps *::text").getall()))

        property_type = "apartment"

        title = response.css("h1.property-name::text").get()
        rent = response.css("span.rentAmt::text").get()
        rent = re.findall(r"([0-9]+)", rent)
        if(len(rent) > 0):
            rent = rent[0]
        else:
            return
        
        if(not re.search(r"([0-9]{2,})", rent)):
            return
        
        currency = "CAD"
        
        heading = response.css("span.home-heading::text").get()
        room_count = re.findall(r"([1-9]) Bed", heading)
        if( len(room_count) > 0):
            room_count = room_count[0]
        else:
            room_count = "1"

        bathroom_count = re.findall(r"([1-9]) Bath", heading)
        if( len(bathroom_count) > 0):
            bathroom_count = bathroom_count[0]
        else:
            bathroom_count = None

        images = response.css("div.carousel-image::attr(data-bg)").getall()

        latitude = response.css("section.location-of-property div.marker::attr(data-lat)").get()
        longitude = response.css("section.location-of-property div.marker::attr(data-lng)").get()
        city,zipcode,address='','',''
        try:
            location_data = extract_location_from_coordinates(longitude, latitude)
            address = location_data[2]
            city = location_data[1]
            zipcode = location_data[0]
        except:
            address = response.css("p:contains('located at:')::text").get()
            address = address.split(":")[1]
            
            try:
                location_data = extract_location_from_address(address)
                latitude = str(location_data[1])
                longitude = str(location_data[0])
                location_data = extract_location_from_coordinates(longitude, latitude)
                zipcode = location_data[0]
                city = location_data[1]
            except:
                city = title.split(" ")[0]
                pass
            

        available_date = response.css("p.availability-of-unit::text").get()

        amenities = response.css("section.amenities").css("p.amenity::text").getall()

        amenities = " ".join(amenities)
        dishwasher = "Dishwasher" in amenities
        pets_allowed = "Pet Friendly" in amenities
        balcony = "balcony" in amenities
        parking = "Parking" in amenities
        washing_machine = "laundry" in amenities
        furnished_data = response.css("div.listing-short-deets div p::text").getall()
        furnished_data = " ".join(furnished_data)
        if("Furnished" in furnished_data):
            furnished = True
        if("Unfurnished" in furnished_data):
            furnished = False

        landlord_name = "livefurnished_ca"
        landlord_phone = "604-690-6942"
        landlord_email = "hello@livefurnished.ca"

        item_loader = ListingLoader(response=response)
        # # # MetaData
        item_loader.add_value("external_link", response.url) # String
        item_loader.add_value("external_source", self.external_source) # String

        # item_loader.add_value("external_id", external_id) # String
        item_loader.add_value("position", self.position) # Int
        self.position += 1
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
        # item_loader.add_value("square_meters", square_meters) # Int
        item_loader.add_value("room_count", room_count) # Int
        item_loader.add_value("bathroom_count", bathroom_count) # Int

        item_loader.add_value("available_date", available_date) # String => date_format

        item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking) # Boolean
        # # item_loader.add_value("elevator", elevator) # Boolean
        item_loader.add_value("balcony", balcony) # Boolean
        # # #item_loader.add_value("terrace", terrace) # Boolean
        # # #item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        item_loader.add_value("washing_machine", washing_machine) # Boolean
        item_loader.add_value("dishwasher", dishwasher) # Boolean

        # # # Images
        item_loader.add_value("images", images) # Array
        item_loader.add_value("external_images_count", len(images)) # Int
        # # # item_loader.add_value("floor_plan_images", floor_plan_images) # Array

        # # # Monetary Status
        item_loader.add_value("rent_string", rent) # Int
        # item_loader.add_value("deposit", deposit) # Int
        # # #item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        # item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", currency) # String

        # # #item_loader.add_value("water_cost", water_cost) # Int
        # item_loader.add_value("heating_cost", heating_cost) # Int

        # # #item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name) # String
        item_loader.add_value("landlord_phone", landlord_phone) # String
        item_loader.add_value("landlord_email", landlord_email) # String

        yield item_loader.load_item()
