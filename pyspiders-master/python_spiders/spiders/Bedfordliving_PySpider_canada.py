# -*- coding: utf-8 -*-
# Author: Fill with the developer's Name
import scrapy
from ..loaders import ListingLoader
from urllib.request import urlopen
import json
from ..helper import extract_location_from_coordinates


class BedfordlivingPyspiderCanadaSpider(scrapy.Spider):
    name = "Bedfordliving"
    start_urls = ['https://www.bedfordliving.ca/residential?']
    allowed_domains = ["bedfordliving.ca"]
    country = 'canada' # Fill in the Country's name
    locale = 'en' # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'

    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url="https://api.theliftsystem.com/v2/search?show_child_properties=true&client_id=59&auth_token=sswpREkUtyeYjeoahA2i&city_id=970&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=0&max_rate=99999&min_sqft=0&max_sqft=10000&only_available_suites=true&show_custom_fields=true&show_promotions=true&local_url_only=true&region=&keyword=false&property_types=low-rise-apartment%2Cmid-rise-apartment%2Chigh-rise-apartment%2Cluxury-apartment&ownership_types=&exclude_ownership_types=&custom_field_key=&custom_field_values=&order=&limit=50&neighbourhood=&amenities=&promotions=&city_ids=3133%2C1154%2C2870%2C2081%2C229%2C415%2C332%2C532%2C970&pet_friendly=&offset=0&count=false", callback=self.parse)

    # 2. SCRAPING level 2
    def parse(self, response, **kwargs):
        # Your code goes here
        response = urlopen(response.url)
        apartments_json = json.loads(response.read())
        for apartment_json in apartments_json:
            title = apartment_json["name"]

            apartment_url = apartment_json["permalink"]
            apartment_url = apartment_url.split("/")
            apartment_url = apartment_url[4]
            apartment_url = "https://www.bedfordliving.ca/apartments/" + apartment_url

            address = apartment_json["address"]["address"]
            city = apartment_json["address"]["city"]
            zip_code = apartment_json["address"]["postal_code"]

            external_id = apartment_json["id"]

            property_type = apartment_json["property_type"]
            landlord_name = apartment_json["contact"]["name"]
            landlord_email = apartment_json["contact"]["email"]
            landlord_phone = apartment_json["contact"]["phone"]

            parking = [apartment_json["parking"]["indoor"], apartment_json["parking"]["outdoor"], apartment_json["parking"]["additional"]]
            if len(parking) > 0:
                parking = True
            else:
                parking = None

            longitude = apartment_json["geocode"]["longitude"]
            latitude = apartment_json["geocode"]["latitude"]

            apartment_info = {
                "title": title,
                "address": address,
                "city": city,
                "zip_code": zip_code,
                "external_id": external_id,
                "property_type": property_type,
                "landlord_name": landlord_name,
                "landlord_email": landlord_email,
                "landlord_phone": landlord_phone,
                "parking": parking,
                "longitude": longitude,
                "latitude": latitude
            }

            yield scrapy.Request(url=apartment_url, callback=self.populate_item, meta=apartment_info)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        units_divs = response.xpath('.// *[ @ id = "content"] / section[3] / div / section[1] / div[2] / div')
        for unit_div in units_divs:
            unit_availability = unit_div.xpath('.//div[contains(@class, "suite-inquiry")]//a//span/text()').extract()
            unit_availability = unit_availability[0].strip()
            if unit_availability == "Inquire Now":
                item_loader = ListingLoader(response=response)

                title = response.meta.get("title")


                external_id = str(response.meta.get("external_id"))

                property_type = response.meta.get("property_type")
                if "apartment" in property_type:
                    property_type = "apartment"

                landlord_name = "Bedford Living"
                landlord_email = response.meta.get("landlord_email")
                landlord_number = response.meta.get("landlord_phone")
                landlord_number = landlord_number.replace("(", "")
                landlord_number = landlord_number.replace(") ", "-")

                parking = response.meta.get("parking")

                longitude = response.meta.get("longitude")
                latitude = response.meta.get("latitude")
                zip, city, address = extract_location_from_coordinates(longitude, latitude)
                zipcode = response.meta.get("zip_code")

                # description = response.xpath('normalize-space(.//meta[contains(@name, "description")]/@content)').extract()
                description = response.css('meta[name="description"]').xpath('normalize-space(@content)').extract()

                amenities_existance = response.css('div.amenities-container')
                balcony = None
                dishwasher = None
                pets_allowed = None
                elevator = None

                if amenities_existance:
                    suite_amenities = amenities_existance.css('div.container div.suite-amenities ul li span.amenity::text').extract()
                    for amenity in suite_amenities:
                        if "Balconies" in amenity:
                            balcony = True
                        if "dishwasher" in amenity:
                            dishwasher = True

                    building_amenities = amenities_existance.css('div.container div.building-amenities ul li span.amenity::text').extract()
                    for amenity in building_amenities:
                        if "Elevator" in amenity:
                            elevator = True
                        if "Pets Allowed" in amenity:
                            pets_allowed = True

                building_images = response.xpath('.//div[contains(@class, "gallery-image")]//a/@href').extract()
                unit_images = unit_div.xpath('.//div[contains(@class, "suite-photos")]//a/@href').extract()
                images = building_images + unit_images
                output_image = []
                for single_image in images:
                    import requests
                    re = requests.get(single_image)
                    try:
                        check_error = re.text
                        if "Access Denied" not in check_error:
                            output_image.append(single_image)
                    except:
                        continue
                images = output_image

                unit_title = unit_div.xpath('normalize-space(string(.//div[contains(@class, "type-name")]))')[0].extract()
                title = title + " - " + unit_title

                room_count = unit_title[0]
                if unit_title == "bachelor":
                    room_count = 1

                unit_rent = unit_div.xpath('normalize-space(.//div[contains(@class, "rate-value")]/text())')[0].extract()
                unit_rent = unit_rent.replace("$", "")
                rent = int(unit_rent)

                unit_sqm = unit_div.xpath('normalize-space(.//div[contains(@class, "suite-sqft")]//p/text())')[0].extract()
                unit_sqm = unit_sqm.replace("sq.ft", "")
                square_meters = unit_sqm
                if unit_sqm == " N/A":
                    square_meters = None

                unit_availabe_data = unit_div.xpath('.//div[contains(@class, "suite-availability")]//p/text()')[0].extract()
                unit_availabe_data = unit_availabe_data.replace("Available", "")
                unit_availabe_data = unit_availabe_data.split()
                if "Now" in unit_availabe_data:
                    available_date = None
                if len(unit_availabe_data) == 3:
                    month = unit_availabe_data[0]
                    month = self.convertMonthToNum(month)
                    day = unit_availabe_data[1]
                    year = unit_availabe_data[2]
                    available_date = year + "-" + month + "-" + day

                unit_url = unit_div.xpath('.//div[contains(@class, "suite-inquiry")]//a//@href').extract()
                external_link = response.url + unit_url[0]

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
                item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
                item_loader.add_value("square_meters", square_meters) # Int
                item_loader.add_value("room_count", room_count) # Int
                #item_loader.add_value("bathroom_count", bathroom_count) # Int

                item_loader.add_value("available_date", available_date) # String => date_format

                item_loader.add_value("pets_allowed", pets_allowed) # Boolean
                #item_loader.add_value("furnished", furnished) # Boolean
                item_loader.add_value("parking", parking) # Boolean
                item_loader.add_value("elevator", elevator) # Boolean
                item_loader.add_value("balcony", balcony) # Boolean
                #item_loader.add_value("terrace", terrace) # Boolean
                #item_loader.add_value("swimming_pool", swimming_pool) # Boolean
                #item_loader.add_value("washing_machine", washing_machine) # Boolean
                item_loader.add_value("dishwasher", dishwasher) # Boolean

                # # Images
                item_loader.add_value("images", images) # Array
                item_loader.add_value("external_images_count", len(images)) # Int
                #item_loader.add_value("floor_plan_images", floor_plan_images) # Array

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

    def convertMonthToNum(self, monthName):
        months = {
            "jan": "01",
            "feb": "02",
            "mar": "03",
            "apr": "04",
            "may": "05",
            "jun": "06",
            "jul": "07",
            "aug": "08",
            "sep": "09",
            "oct": "10",
            "nov": "11",
            "dec": "12",
        }
        return months.get(str(monthName).lower())
