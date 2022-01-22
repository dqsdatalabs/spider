# -*- coding: utf-8 -*-
# Author: Asmaa Elshahat
import scrapy
from ..loaders import ListingLoader
import json


class PromptonrentalsPyspiderCanadaSpider(scrapy.Spider):
    name = "PromptonRentals"
    start_urls = ['https://api.theliftsystem.com/v2/search?locale=en&client_id=760&auth_token=sswpREkUtyeYjeoahA2i&city_id=3044&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=0&max_rate=5600&min_sqft=0&max_sqft=10000&show_custom_fields=true&show_promotions=true&region=&keyword=false&property_types=apartments%2C+houses&ownership_types=&exclude_ownership_types=&custom_field_key=&custom_field_values=&order=min_rate+ASC&limit=66&neighbourhood=&amenities=&promotions=&city_ids=3201%2C3044&pet_friendly=&offset=0&count=false']
    allowed_domains = ["prompton.com"]
    country = 'canada' # Fill in the Country's name
    locale = 'en' # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'

    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        for url in self.start_urls:
            headers = {
                'Set-Cookie': '_LIFTAPI_session=BAh7CEkiD3Nlc3Npb25faWQGOgZFVEkiJTFmOTA2N2U3ODU5YWYzM2I5NTlhMmZjNmI1YTJjZDE3BjsAVEkiGXdhcmRlbi51c2VyLnVzZXIua2V5BjsAVFsHWwZpJkkiIiQyYSQxMCRTQ1BMUS90UU9xM3d4bE5JTHFSZzguBjsAVEkiHXdhcmRlbi51c2VyLnVzZXIuc2Vzc2lvbgY7AFR7BkkiFGxhc3RfcmVxdWVzdF9hdAY7AFRsKweza81h--0ef7201c2de27acd07a0d30186459315c982aef6; path=/; HttpOnly',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36'
                       }
            yield scrapy.Request(url, callback=self.parse, headers=headers)

    # 2. SCRAPING level 2
    def parse(self, response, **kwargs):
        apartments_json = json.loads(response.text)
        for apartment_json in apartments_json:
            external_id = apartment_json["id"]
            url = apartment_json["permalink"]
            url = url.split("/")
            url = url[-1]
            url = "https://www.promptonrentals.ca/rentals/" + url
            title = apartment_json["name"]
            landlord_name = apartment_json["client"]["name"]
            landlord_email = apartment_json["client"]["email"]
            landlord_number = apartment_json["client"]["phone"]
            address = apartment_json["address"]["address"]
            city = apartment_json["address"]["city"] + ", " + apartment_json["address"]["province"]
            zipcode = apartment_json["address"]["postal_code"]
            pets_allowed = apartment_json["pet_friendly"]
            parking = apartment_json["parking"]
            description = apartment_json["details"]["overview"]
            latitude = apartment_json["geocode"]["latitude"]
            longitude = apartment_json["geocode"]["longitude"]
            suites = apartment_json["matched_suite_names"]
            available_date = apartment_json["availability_status_label"]
            apartment_info = {
                "external_id": external_id,
                "title": title,
                "landlord_name": landlord_name,
                "landlord_email": landlord_email,
                "landlord_number": landlord_number,
                "address": address,
                "city": city,
                "zipcode": zipcode,
                "pets_allowed": pets_allowed,
                "parking": parking,
                "description": description,
                "latitude": latitude,
                "longitude": longitude,
                "suites": suites,
                "available_date": available_date
            }
            yield scrapy.Request(url, callback=self.populate_item, meta=apartment_info, dont_filter=True)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        suites = response.meta.get("suites")
        for suite in suites:
            suite_type = suite

            room_count = suite_type.split()
            room_count = room_count[-2]
            if "-" in room_count:
                room_count = room_count.split("-")
                room_count = room_count[-1]
                room_count = room_count.replace(",", ".")
                room_count = float(room_count)
                room_count = round(room_count)
                room_count = int(room_count)

            external_id = response.meta.get("external_id")
            external_id = str(external_id)

            title = response.meta.get("title")
            title = title + " " + suite

            landlord_name = response.meta.get("landlord_name")
            landlord_email = response.meta.get("landlord_email")
            landlord_number = response.meta.get("landlord_number")

            address = response.meta.get("address")
            city = response.meta.get("city")
            address = address + ", " + city
            zipcode = response.meta.get("zipcode")

            pets_allowed = response.meta.get("pets_allowed")
            if pets_allowed == "False":
                pets_allowed = None
            elif pets_allowed == "True":
                pets_allowed = True

            parking = response.meta.get("parking")
            parking = parking.values()
            parking = list(parking)
            if None not in parking:
                parking = True
            else:
                parking = None

            description = response.meta.get("description")
            description = description.replace("\n", "")

            latitude = response.meta.get("latitude")
            longitude = response.meta.get("longitude")

            available_date = response.meta.get("available_date")
            available_date = available_date.replace("Available", "")
            available_date = available_date.strip()
            if "Now" in available_date:
                available_date = None
            else:
                available_date = available_date.split()
                year = available_date[-1]
                day = "01"
                month = self.month_str_to_num(available_date[-2])
                available_date = year + "-" + month + "-" + day

            suite_link = suite_type.replace(" ", "-")
            suite_link = suite_link.replace("--", "-")
            suite_link = suite_link.replace("--", "-")
            external_link = response.url + "#" + suite_link
            external_link = external_link.replace("##", "#")

            suites_div = response.css('div.suite-wrap')
            bathroom_count = None
            rent = None
            square_meters = None
            floor_plan_images = None
            images = None
            for suite_div in suites_div:
                suite_title = suite_div.css('div.suite-type div.title::text')[0].extract()
                suite_title = suite_title.strip()
                if suite_title == suite:
                    bathroom_count = suite_div.css('div.suite-bath span.value::text')[0].extract()
                    bathroom_count = int(bathroom_count)

                    rent = suite_div.css('div.suite-rate span.value::text')[0].extract()
                    rent = rent.replace("$", "")
                    rent = rent.replace(",", "")
                    rent = float(rent)
                    rent = round(rent)
                    rent = int(rent)

                    square_meters = suite_div.css('div.suite-sqft span.value::text').extract()
                    if len(square_meters) >= 1:
                        square_meters = square_meters[0]
                        square_meters = float(square_meters)
                        square_meters = round(square_meters)
                        square_meters = int(square_meters)

                    images = suite_div.css('div.suite-photos a::attr(href)').extract()

                    if suite_div.css('div.suite-floorplans'):
                        floor_plan_images = suite_div.css('div.suite-floorplans div a::attr(href)').extract()

            amenities = response.css('div.amenities div.amenity-group div::text').extract()
            balcony = None
            dishwasher = None
            washing_machine = None
            elevator = None
            swimming_pool = None
            terrace = None
            for amenity in amenities:
                if "balcon" in amenity.lower():
                    balcony = True
                if "dishwasher" in amenity.lower():
                    dishwasher = True
                if "Washer" in amenity:
                    washing_machine = True
                if "elevator" in amenity.lower():
                    elevator = True
                if "pool" in amenity.lower():
                    swimming_pool = True
                if "parking" in amenity.lower():
                    parking = True
                if "garage" in amenity.lower():
                    parking = True
                if "terrace" in amenity.lower():
                    terrace = True

            item_loader = ListingLoader(response=response)

            # # MetaData
            item_loader.add_value("external_link", external_link) # String
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
            item_loader.add_value("property_type", "apartment") # String => ["apartment", "house", "room", "student_apartment", "studio"]
            item_loader.add_value("square_meters", square_meters) # Int
            item_loader.add_value("room_count", room_count) # Int
            item_loader.add_value("bathroom_count", bathroom_count) # Int

            item_loader.add_value("available_date", available_date) # String => date_format

            item_loader.add_value("pets_allowed", pets_allowed) # Boolean
            #item_loader.add_value("furnished", furnished) # Boolean
            item_loader.add_value("parking", parking) # Boolean
            item_loader.add_value("elevator", elevator) # Boolean
            item_loader.add_value("balcony", balcony) # Boolean
            item_loader.add_value("terrace", terrace) # Boolean
            item_loader.add_value("swimming_pool", swimming_pool) # Boolean
            item_loader.add_value("washing_machine", washing_machine) # Boolean
            item_loader.add_value("dishwasher", dishwasher) # Boolean

            # # Images
            item_loader.add_value("images", images) # Array
            item_loader.add_value("external_images_count", len(images)) # Int
            item_loader.add_value("floor_plan_images", floor_plan_images) # Array

            # # Monetary Status
            item_loader.add_value("rent", rent) # Int
            #item_loader.add_value("deposit", deposit) # Int
            #item_loader.add_value("prepaid_rent", prepaid_rent) # Int
            #item_loader.add_value("utilities", utilities) # Int
            item_loader.add_value("currency", "CAD") # String

            #item_loader.add_value("water_cost", water_cost) # Int
            #item_loader.add_value("heating_cost", heating_cost) # Int

            #item_loader.add_value("energy_label", energy_label) # String

            # # LandLord Details
            item_loader.add_value("landlord_name", landlord_name) # String
            item_loader.add_value("landlord_phone", landlord_number) # String
            item_loader.add_value("landlord_email", landlord_email) # String

            self.position += 1
            yield item_loader.load_item()

    def month_str_to_num(self, month):
        months = {
            "January": "01",
            "February": "02",
            "March": "03",
            "April": "04",
            "May": "05",
            "June": "06",
            "July": "07",
            "August": "08",
            "September": "09",
            "October": "10",
            "November": "11",
            "December": "12"
        }
        return months[month.strip()]
