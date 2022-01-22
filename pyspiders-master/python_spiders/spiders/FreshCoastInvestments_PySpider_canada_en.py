# -*- coding: utf-8 -*-
# Author: Muhammad Alaa
import scrapy
from ..loaders import ListingLoader
from python_spiders.helper import extract_location_from_address, extract_location_from_coordinates, extract_number_only
import datetime

class FreshcoastinvestmentsPyspiderCanadaEnSpider(scrapy.Spider):
    name = "FreshCoastInvestments_PySpider_canada_en"
    start_urls = [
        'https://freshcoast.managebuilding.com/Resident/public/rentals']
    allowed_domains = ["freshcoast.managebuilding.com"]
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
        urls = response.css("a.featured-listing::attr(href)").getall()
        title = response.css("h3.featured-listing__title::text").get()
        for url in urls:
            yield scrapy.Request('https://freshcoast.managebuilding.com' + url, callback=self.populate_item, meta={"title": title})


    # 3. SCRAPING level 3
    def populate_item(self, response):
        address = response.css("span.text--muted::text").get()
        if address == None:
            address = response.css(
                "h1.title.title--medium::text").get().strip()
        info = address.split(" ")
        zipcode = info[0]
        city = info[1]
        longitude, latitude = extract_location_from_address(zipcode)
    
        images = response.css(
            "img.unit-detail__gallery-thumbnail::attr(src)").getall()
        property_type = "house"

        house_details = response.css(
            "li.unit-detail__unit-info-item::text").getall()
        square_meters = None
        room_count = 1
        bathroom_count = None
        for detail in house_details:
            if detail.lower().find("bed") != -1:
                room_count = detail[0]

            if detail.lower().find("bath") != -1:
                bathroom_count = detail[0]
                
            if detail.lower().find("sqft") != -1:
                square_meters = detail.split(" ")[0]
            available_date = response.css(
                "div.unit-detail__available-date.text--muted::text").get().strip()[10:]
            datetime.datetime.strptime(
                available_date, '%m/%d/%Y').strftime('%m/%d/%y')

        texts = response.css("p::text").getall()
        for txt in texts:
            if txt.lower().find("deposit") != -1:
                deposit = txt
                deposit = deposit[1:].replace(",","")
                deposit = deposit[:deposit.find(".")]
        rent = response.css(
            "div.unit-detail__price::text").get().strip()

        amenities = response.css("li.column.column--sm5::text").getall()
        pets_allowed = None
        parking = None
        elevator = None
        balcony = None
        washing_machine = None
        dishwasher = None

        for amenity in amenities:
            if amenity.lower().find("dishwasher") != -1:
                dishwasher = True
            if amenity.lower().find("pet friendly") != -1:
                pets_allowed = True
            if amenity.lower().find("laundry") != -1:
                washing_machine = True
            if amenity.lower().find("parking") != -1:
                parking = True
            if amenity.lower().find("balcony") != -1:
                balcony = True
            if amenity.lower().find("elevator") != -1:
                elevator = True
        furnished = None
        details = response.css("div.unit-detail__info.column p.unit-detail__description::text").getall()
        description = ''
        for txt in details:
            txt = txt.lower()
            txt = txt.replace("text us at 587-315-0273", "")
            txt = txt.replace('contact us today at 587-315-0273 or rentals@freshcoastinvestments.ca', "")
            txt = txt.replace('contact kate today at 587-315-0273 or rentals@freshcoastinvestments.ca', "")
            txt = txt.replace('contact 587-315-0273 or rentals@freshcoastinvestments.ca', "")
            if txt.find("furnished"):
                furnished = True
            if txt.find("laundry"):
                washing_machine = True
            if txt.find("parking"):
                parking = True
            description += txt
            
        title = response.meta["title"]

        landlord_number = '587-315-0273'
        landlord_email = 'rentals@freshcoastinvestments.ca'

        landlord_name = 'Fresh Coast Investments'
        if rent == "" or rent == None:
            return
        rent = rent[1:].replace(",","")
        rent = rent[:rent.find(".")]
        
        item_loader = ListingLoader(response=response)

        # # MetaData
        item_loader.add_value("external_link", response.url) # String
        item_loader.add_value("external_source", self.external_source) # String

        item_loader.add_value("position", self.position) # Int
        item_loader.add_value("title", title) # String
        item_loader.add_value("description", description) # String

        # # Property Details
        item_loader.add_value("city", city) # String
        item_loader.add_value("zipcode", zipcode) # String
        item_loader.add_value("address", address) # String
        item_loader.add_value("latitude", str(latitude)) # String
        item_loader.add_value("longitude", str(longitude)) # String
        item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters) # Int
        item_loader.add_value("room_count", room_count) # Int
        item_loader.add_value("bathroom_count", bathroom_count) # Int

        item_loader.add_value("available_date", available_date) # String => date_format

        item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking) # Boolean
        item_loader.add_value("elevator", elevator) # Boolean
        item_loader.add_value("balcony", balcony) # Boolean
        item_loader.add_value("washing_machine", washing_machine) # Boolean
        item_loader.add_value("dishwasher", dishwasher) # Boolean

        # # Images
        item_loader.add_value("images", images) # Array
        item_loader.add_value("external_images_count", len(images)) # Int

        # # Monetary Status
        item_loader.add_value("rent", rent) # Int
        item_loader.add_value("deposit", deposit) # Int
        item_loader.add_value("currency", "CAD") # String

        # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name) # String
        item_loader.add_value("landlord_phone", landlord_number) # String
        item_loader.add_value("landlord_email", landlord_email) # String

        self.position += 1
        yield item_loader.load_item()
