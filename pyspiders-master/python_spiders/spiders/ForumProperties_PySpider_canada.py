# -*- coding: utf-8 -*-
# Author: Asmaa Elshahat
import scrapy
from ..loaders import ListingLoader
import json


class ForumpropertiesPyspiderCanadaSpider(scrapy.Spider):
    name = "ForumProperties"
    start_urls = ['https://api.theliftsystem.com/v2/search?client_id=490&auth_token=sswpREkUtyeYjeoahA2i&city_id=2171&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=700&max_rate=3300&min_sqft=0&max_sqft=50000&show_all_properties=true&show_custom_fields=true&show_promotions=true&local_url_only=true&translate_property_types=true&region=&keyword=false&property_types=apartments%2C+houses&ownership_types=&exclude_ownership_types=&custom_field_key=&custom_field_values=&order=min_rate+ASC%2Cmax_rate+ASC%2Cmin_bed+ASC%2Cmax_bed+ASC%2Cid+DESC%2Cfeatured+DESC&limit=12&neighbourhood=&amenities=&promotions=&city_ids=1863%2C32950%2C2695%2C3218%2C1778%2C1598%2C845%2C427%2C2619%2C898%2C2860%2C1879%2C273%2C796%2C2910%2C1347%2C1483%2C3085%2C2793%2C2291%2C2437%2C2782%2C1857%2C2493%2C1831%2C1607%2C32951%2C3111%2C437%2C1485%2C644%2C1039%2C2791%2C1615%2C1573%2C2171&pet_friendly=&offset=0&count=false']
    allowed_domains = ["forumproperties.com"]
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
        apartments_json = json.loads(response.text)
        for apartment_json in apartments_json:
            external_id = apartment_json["id"]
            url = apartment_json["permalink"]
            url = url.split("/")
            url = url[-1]
            url = "https://www.forumproperties.com/residential/" + url
            title = apartment_json["name"]
            landlord_name = apartment_json["client"]["name"]
            landlord_email = apartment_json["client"]["email"]
            landlord_number = apartment_json["client"]["phone"]
            address = apartment_json["address"]["address"]
            city = apartment_json["address"]["city"] + ", " + apartment_json["address"]["province"]
            zipcode = apartment_json["address"]["postal_code"]
            pets_allowed = apartment_json["pet_friendly"]
            parking = apartment_json["parking"]
            description = apartment_json["website"]["description"]
            latitude = apartment_json["geocode"]["latitude"]
            longitude = apartment_json["geocode"]["longitude"]
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
                "available_date": available_date
            }
            yield scrapy.Request(url, callback=self.populate_item, meta=apartment_info, dont_filter=True)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        suites = response.css('div.suites-table')
        suites_exits = suites.css('p::text').extract()
        building_images = response.css('section.gallery section section div div.gallery-image div::attr(style)').extract()
        building_images_all = []
        for item in building_images:
            item = item.split("url(")[1]
            item = item.split(")")[0]
            building_images_all.append(item)
        if "No vacancy" not in suites_exits:
            suites_rows = suites.css('div.suites-details div.suite-row')
            for suite in suites_rows:
                row_details = suite.css('div.suite-row-details')
                suite_type = row_details.css('div.column-unit span.value::text').extract()

                rent = row_details.css('div.column-price span.value::text')[0].extract()
                rent = rent.replace("$", "")
                rent = rent.replace(",", "")
                rent = float(rent)
                rent = round(rent)
                rent = int(rent)

                square_meters = row_details.css('div.column-size span.value::text').extract()
                if len(square_meters) >= 1:
                    square_meters = square_meters[0]
                    square_meters = square_meters.split()[0]
                    square_meters = float(square_meters)
                    square_meters = round(square_meters)
                    square_meters = int(square_meters)
                else:
                    square_meters = None

                balcony_exist = row_details.css('div.column-balcony span.value::text').extract()
                balcony = None
                if len(balcony_exist) >= 1:
                    balcony = True

                room_count = row_details.css('div.column-bed span.value::text')[0].extract()
                room_count = int(room_count)
                bathroom_count = row_details.css('div.column-bath span.value::text')[0].extract()
                bathroom_count = int(bathroom_count)

                available_date = row_details.css('div.column-availability span.value::text')[0].extract()
                if "Available" in available_date:
                    available_date = None
                else:
                    available_date = available_date.split()
                    if len(available_date) == 3:
                        year = available_date[2]
                        month = available_date[0]
                        month = self.month_str_to_num(month.strip())
                        day = available_date[1]
                        if len(day) == 1:
                            day = "0" + day
                        available_date = year + "-" + month + "-" + day
                    else:
                        month = available_date[0]
                        month = month.lower()
                        month = month.title()
                        month = self.month_str_to_num(month)
                        day = available_date[1]
                        if len(day) == 1:
                            day = "0" + day
                        available_date = "2022" + "-" + month + "-" + day

                images = suite.css('div.photos-slider div.gallery-image a::attr(href)').extract()
                images = building_images_all + images
                floor_image = suite.css('div.floorplans-slider div.gallery-image a::attr(href)').extract()
                floor_plan_images = []
                for item in floor_image:
                    floor_test = item.split(".")
                    if "pdf" not in floor_test:
                        floor_plan_images.append(item)

                suite_amenities = suite.css('div.suite-data div.suite-amenities ul li::text').extract()
                dishwasher = None
                washing_machine = None
                elevator = None
                swimming_pool = None
                terrace = None
                parking = None
                for amenity in suite_amenities:
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

                floor = suite.css('div.suite-data::attr(data-floor)').extract()
                suite_url = suite.css('div.suite-data div.suite-buttons p a::attr(href)').extract()
                if len(suite_url) == 2:
                    suite_url = suite_url[1]
                else:
                    suite_url = suite_url[0]
                external_link = response.url + "#" + suite_url
                external_link = external_link.replace("##", "#")

                external_id = response.meta.get("external_id")
                external_id = str(external_id)

                title = response.meta.get("title")
                if len(suite_type) >= 1:
                    title = title + " | suite " + suite_type[0]
                    suite_type_url = suite_type[0]
                    suite_type_url = suite_type_url.replace(" ", "-")
                    external_link = external_link + "#suite" + suite_type_url

                landlord_name = response.meta.get("landlord_name")
                landlord_email = response.meta.get("landlord_email")
                landlord_number = response.meta.get("landlord_number")

                address = response.meta.get("address")
                city = response.meta.get("city")
                address = address + ", " + city
                zipcode = response.meta.get("zipcode")
                latitude = response.meta.get("latitude")
                longitude = response.meta.get("longitude")

                pets_allowed = response.meta.get("pets_allowed")
                if pets_allowed == "False":
                    pets_allowed = None
                elif pets_allowed == "True":
                    pets_allowed = True
                else:
                    pets_allowed = None

                description_one = response.meta.get("description")
                description_one = description_one.replace("\n", "")
                description_two = response.css('div.suite-description div div ul li::text').extract()
                description_two = " ".join(description_two)
                if not description_two:
                    description_two = response.css('div.suite-description div div::text').extract()
                    description_two = " ".join(description_two)
                    description_two = description_two.replace("\n", "")
                    description_two = description_two.replace("\r", "")
                description_three = response.css('div.suite-description div div p::text').extract()
                description_three = " ".join(description_three)
                description = description_one + description_two + description_three

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
                item_loader.add_value("floor", floor) # String
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
            "Jan": "01",
            "Feb": "02",
            "Mar": "03",
            "Apr": "04",
            "May": "05",
            "Jun": "06",
            "Jul": "07",
            "Aug": "08",
            "Sep": "09",
            "Oct": "10",
            "Nov": "11",
            "Dec": "12"
        }
        return months[month.strip()]
