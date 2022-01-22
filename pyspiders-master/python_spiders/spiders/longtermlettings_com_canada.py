# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re
import requests

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader
from python_spiders.helper import extract_location_from_address, extract_location_from_coordinates, remove_white_spaces

class Longtermlettings_com_canadaSpider(Spider):
    name = 'longtermlettings_com_canada'
    name2 = 'longtermlettings_com'
    country='canada'
    locale='en' 
    external_source = "{}_PySpider_{}_{}".format(name2.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.longtermlettings.com"]
    start_urls = ["https://www.longtermlettings.com/find/rentals/canada/"]
    position = 1

    def parse(self, response):
        for url in response.css("div.searchrestable div.prop_title").css("a::attr(href)").getall():
            url = str(url)
            if( not re.search(r'^https?:\/\/.*[\r\n]*', url)):
                continue
            yield Request(response.urljoin(url), callback=self.populate_item)
        
        next_page = response.css("div.search_page").css("a:contains('Next Page')::attr(href)").get()
        if (next_page):
            yield response.follow(response.urljoin(next_page), callback=self.parse, dont_filter = True)

    def populate_item(self, response):
        
        property_type = "apartment"

        title = response.css("h2.largetitle::text").get()
        rent = response.css("span.advert-price b::text").get()
        if( not rent ):
            return
        rent = re.findall("([0-9]+)", rent)
        rent = "".join(rent)
        if(not re.search("([0-9]+)", rent)):
            return
        currency = "CAD"

        external_id = response.css("span.calendars::text").getall()
        external_id = " ".join(external_id)
        external_id = re.findall(r"\(ID: (.+)\)", external_id)[0]
        images_url = f"https://www.longtermlettings.com/r/img_view/img/?id={external_id}&type=R"

        images = requests.get(images_url)
        images = images.text
        images = re.findall(r'<img \s+ src=\"(.+)\"', images)

        property_data = response.css("div:contains('Bathrooms')::text").getall()
        property_data = " ".join(property_data)
        room_count = re.findall("([1-9]) room", property_data)
        if(len(room_count)>0):
            room_count = room_count[0]
        else:
            room_count = "1"

        bathroom_count = re.findall("Bathrooms: ([1-9])", property_data)
        if(len(bathroom_count) > 0):
            bathroom_count = bathroom_count[0]
        else:
            bathroom_count = None        
        
        furnished = re.findall("Furnishings: (.+)", property_data)
        if(len(furnished) > 0):
            furnished = furnished[0]
            if(furnished == "Furnished"):
                furnished = True
            else:
                furnished = False
        else:
            furnished = None
        
        description = response.css("div.descriptiontext::text").getall()
        description = " ".join(description)
        description = re.sub(r'[A-Za-z0-9]*@[A-Za-z]*\.?[A-Za-z0-9]*', "", description)
        description = re.sub(r'^https?:\/\/.*[\r\n]*', '', description, flags=re.MULTILINE)
        description = re.sub(r'[0-9]+\-[0-9]+\-[0-9]+', '', description, flags=re.MULTILINE)
        description = re.sub(r'[0-9]+\.[0-9]+\.[0-9]+', '', description, flags=re.MULTILINE)
        description = re.sub(r'\([0-9]\)+ [0-9]+\-[0-9]+', '', description, flags=re.MULTILINE)
        description = re.sub(r"\s+", " ", description)

        swimming_pool = "Swimming" in description

        address = response.css("span[itemprop='addressRegion']::text").get()
        zipcode = response.css("span[itemprop='postalCode']::text").get()   
        city = response.css("span[itemprop='addressLocality']::text").get()

        address = f"{address}, {city}, {zipcode}"

        location_data = extract_location_from_address(address)
        latitude = str(location_data[1])
        longitude = str(location_data[0])

        landlord_name = response.css("div.texbiggreen").css("b::text").get()
        if( not landlord_name):
            landlord_name = "longtermlettings"
        landlord_phone = "1-403-775-7358"
        
        item_loader = ListingLoader(response=response)
        # # # MetaData
        item_loader.add_value("external_link", response.url) # String
        item_loader.add_value("external_source", self.external_source) # String

        item_loader.add_value("external_id", external_id) # String
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

        # item_loader.add_value("available_date", available_date) # String => date_format

        # # item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        item_loader.add_value("furnished", furnished) # Boolean
        # # item_loader.add_value("parking", parking) # Boolean
        # # item_loader.add_value("elevator", elevator) # Boolean
        # item_loader.add_value("balcony", balcony) # Boolean
        # # #item_loader.add_value("terrace", terrace) # Boolean
        # # #item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        # # item_loader.add_value("washing_machine", washing_machine) # Boolean
        # # item_loader.add_value("dishwasher", dishwasher) # Boolean

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
        # item_loader.add_value("landlord_email", landlord_email) # String

        yield item_loader.load_item()
