# -*- coding: utf-8 -*-
# Author: Fill with the developer's Name
import scrapy
from ..loaders import ListingLoader
from ..helper import extract_location_from_coordinates
import json


class LandlordpropertyrentalmanagementPyspiderCanadaSpider(scrapy.Spider):
    name = "LandlordPropertyRentalManagement"
    start_urls = ['https://diary.landlord.net/public/getListings']
    allowed_domains = ["landlord.net"]
    country = 'canada' # Fill in the Country's name
    locale = 'en' # Fill in the Country's locale, look up the docs if unsure
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
        # Your code goes here
        apartments_json = json.loads(response.text)
        for apartment in apartments_json:
            if apartment["category"] == "For Rent":
                external_id = str(apartment["rentalUnitId"])

                landlord_info = apartment["listingAgent"]
                landlord_name = landlord_info["name"]
                landlord_email = landlord_info["email"]
                landlord_number = landlord_info["phone"]

                title = apartment["street"]

                city = apartment["city"]
                zipcode = apartment["postalCode"]

                room_count = apartment["bedrooms"]
                room_count = int(room_count)
                bathroom_count = apartment["bathrooms"]
                bathroom_count = int(bathroom_count)

                property_type = apartment["houseType"]
                if "condo apt" in property_type.lower():
                    property_type = "apartment"
                elif "townhouse" in property_type.lower():
                    property_type = "house"

                parking = apartment["parking"]
                if parking:
                    if parking == 0:
                        parking = None
                    else:
                        parking = True
                else:
                    parking = "not yet"

                rent = apartment["price"]
                rent = round(rent)
                rent = int(rent)

                available_date = apartment["availabilityDate"]

                description = apartment["description"]
                description = " ".join(description.split())
                description = description.title()

                description_data = description.split()
                balcony = None
                dishwasher = None
                washing_machine = None
                swimming_pool = None
                for description_item in description_data:
                    if "balcony" in description_item.lower():
                        balcony = True
                    if "dishwasher" in description_item.lower():
                        dishwasher = True
                    if "washer" in description_item.lower():
                        washing_machine = True
                    if parking == "not yet":
                        if "parking" in description_item.lower():
                            parking = True
                        else:
                            parking = None
                    if "pool" in description_item.lower():
                        swimming_pool = True

                if parking == "not yet":
                    parking = None

                latitude = str(apartment["latitude"])
                longitude = str(apartment["longitude"])
                no_zipcode, no_city, address = extract_location_from_coordinates(longitude, latitude)

                images_list = apartment["images"]
                images = []
                for image in images_list:
                    images.append("https://diary.landlord.net/documents/listingImages" + image["path"])

                external_link = "https://landlord.net/listings/" + "#" + (title.strip()).replace(" ", "-")

                item_loader = ListingLoader(response=response)

                # # MetaData
                item_loader.add_value("external_link", external_link)  # String
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
                # item_loader.add_value("square_meters", square_meters) # Int
                item_loader.add_value("room_count", room_count) # Int
                item_loader.add_value("bathroom_count", bathroom_count) # Int

                item_loader.add_value("available_date", available_date) # String => date_format

                # item_loader.add_value("pets_allowed", pets_allowed) # Boolean
                # item_loader.add_value("furnished", furnished) # Boolean
                item_loader.add_value("parking", parking) # Boolean
                # item_loader.add_value("elevator", elevator) # Boolean
                item_loader.add_value("balcony", balcony) # Boolean
                # item_loader.add_value("terrace", terrace) # Boolean
                item_loader.add_value("swimming_pool", swimming_pool) # Boolean
                item_loader.add_value("washing_machine", washing_machine) # Boolean
                item_loader.add_value("dishwasher", dishwasher) # Boolean

                # # Images
                item_loader.add_value("images", images) # Array
                item_loader.add_value("external_images_count", len(images)) # Int
                # item_loader.add_value("floor_plan_images", floor_plan_images) # Array

                # # Monetary Status
                item_loader.add_value("rent", rent) # Int
                # item_loader.add_value("deposit", deposit) # Int
                # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
                # item_loader.add_value("utilities", utilities) # Int
                item_loader.add_value("currency", "CAD") # String

                # item_loader.add_value("water_cost", water_cost) # Int
                # item_loader.add_value("heating_cost", heating_cost) # Int

                # item_loader.add_value("energy_label", energy_label) # String

                # # LandLord Details
                item_loader.add_value("landlord_name", landlord_name) # String
                item_loader.add_value("landlord_phone", landlord_number) # String
                item_loader.add_value("landlord_email", landlord_email) # String

                self.position += 1
                yield item_loader.load_item()
