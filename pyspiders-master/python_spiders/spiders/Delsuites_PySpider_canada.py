# -*- coding: utf-8 -*-
# Author: Fill with the developer's Name
import scrapy
from ..loaders import ListingLoader
import json
from ..helper import extract_location_from_address, extract_location_from_coordinates


class DelsuitesPyspiderCanadaSpider(scrapy.Spider):
    name = "Delsuites"
    start_urls = ['https://www.delsuites.com/locations/short-term-rental-locations.php']
    allowed_domains = ["delsuites.com", "beds24.com"]
    country = 'canada'  # Fill in the Country's name
    locale = 'en'  # Fill in the Country's locale, look up the docs if unsure
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
        apartments_urls = response.xpath('.//ul[contains(@class, "dropdown-menu")]//li//a/@href').extract()
        csrf_token_value = response.xpath('.//input[contains(@id, "csrf-token")]/@value')[0].extract()
        url = "https://www.delsuites.com/include/check-csrf-token.php?srclocation=ALL&arrdate=13-Dec-2021&type=rates"
        yield scrapy.Request(url=url, callback=self.apartment_json_parse, headers={'x-csrf-token': csrf_token_value},
                             meta={"apartments_urls": apartments_urls})

    def apartment_json_parse(self, response):
        apartments_json = json.loads(response.text)
        apartments_json_list = []
        for apartment_json in apartments_json:
            address = apartment_json["parentbuildingname"] + ", " + apartment_json["location"] + ", Toronto, Canada"
            rent = apartment_json["rateyearly"]
            title = apartment_json["parentbuildingname"] + " - " + apartment_json["suitetype"]
            room_count = apartment_json["suitetype"]
            building_id = apartment_json["parentbuildingunkid"]
            apartment_json_need = {
                "address": address,
                "rent": rent,
                "title": title,
                "room_count": room_count,
                "building_id": building_id,
            }
            apartments_json_list.append(apartment_json_need)

        apartments_urls = response.meta.get("apartments_urls")
        for apartment_url in apartments_urls:
            if len(apartment_url) > 0:
                if apartment_url.startswith("https://www.delsuites.com/locations/"):
                    if apartment_url.count("/") > 4:
                        yield scrapy.Request(url=apartment_url, callback=self.populate_item,
                                             meta={"apartments_json_list": apartments_json_list})

    def populate_item(self, response):
        apartment_id = "".join(response.xpath('.//script').extract())
        apartment_id = apartment_id.split("propid=")
        apartment_id = (apartment_id[1])[0:5]

        building_id = response.xpath('//h1[contains(@itemprop, "name")]/@id').extract()
        building_id = (building_id[0].split("_"))[1]

        apartments_json_list = response.meta.get("apartments_json_list")
        apartment_json_object = list(filter(lambda x: x.get("building_id") == str(building_id), apartments_json_list))

        building_overview = response.xpath('.//div[contains(@class, "tabovrvfix")]//ul//li/text()').extract()
        swimming_pool = None
        parking = None
        terrace = None
        balcony = None
        washing_machine = None
        pets_allowed = None
        dishwasher = None
        for amenity in building_overview:
            if "Pool" in amenity:
                swimming_pool = True
            if "parking" or "Parking" in amenity:
                parking = True
            if "Terrace" or "Terraces" in amenity:
                terrace = True

        suite_overview = response.xpath('.//div[contains(@class, "grbg")]//ul//li/text()').extract()
        for amenity in suite_overview:
            if "Balcony" in amenity:
                balcony = True
            if "Washer" in amenity:
                washing_machine = True
            if "Pet Friendly" or "Pets are Permitted" in amenity:
                pets_allowed = True
            if "Dishwasher" or "dishwasher" in amenity:
                dishwasher = True

        floor_plan_images_existance = response.xpath('.//div[contains(@id, "Floorplans")]')
        floor_plan_images_dict = {}
        if floor_plan_images_existance:
            floor_plan_images = floor_plan_images_existance.xpath('.//div//div//img/@src').extract()
            floor_plan_images_type = floor_plan_images_existance.xpath('.//div//div//h3/text()').extract()
            if floor_plan_images_type and floor_plan_images:
                floor_plan_images_dict = dict(zip(floor_plan_images_type, floor_plan_images))

        amenities = {
            "swimming_pool": swimming_pool,
            "parking": parking,
            "terrace": terrace,
            "balcony": balcony,
            "washing_machine": washing_machine,
            "pets_allowed": pets_allowed,
            "dishwasher": dishwasher,
        }

        apartment_link = "https://www.beds24.com/booking2.php?propid=" + apartment_id + "&checkin=2021-12-11&checkout=2023-01-02&hideheader=no&hidefooter=yes&cssfile=https%3A%2F%2Fwww.delsuites.com%2Fcss%2Fbeds24.style.css"
        yield scrapy.Request(
            url=apartment_link, callback=self.apartment_details_parse,
            meta={"suites": apartment_json_object, "amenities": amenities, "floor_plan_images": floor_plan_images_dict,
                  "external_link": response.url}
        )

    def apartment_details_parse(self, response):
        amenities = response.meta.get("amenities")
        parking = amenities["parking"]
        terrace = amenities["terrace"]
        balcony = amenities["balcony"]
        washing_machine = amenities["washing_machine"]
        pets_allowed = amenities["pets_allowed"]
        dishwasher = amenities["dishwasher"]
        swimming_pool = amenities["swimming_pool"]

        floor_plan_images_dict = response.meta.get("floor_plan_images")

        suites = response.meta.get("suites")

        suites_divs = response.xpath('.//div[contains(@class, "b24room")]')
        i = 1
        for suite_div in suites_divs:

            building_images_1 = response.xpath(
                './/div[contains(@class, "b24fullcontainer-proprow1")]//div//img[contains(@class, "bootstrap-carousel-img")]/@src').extract()
            building_images_2 = response.xpath(
                './/div[contains(@class, "b24fullcontainer-proprow1")]//div//img[contains(@class, "bootstrap-carousel-img")]/@data-lazy-load-src').extract()
            building_images = building_images_1 + building_images_2

            available_suite = suite_div.xpath('.//a[contains(@class, "myButton")]/text()').extract()
            if available_suite:
                item_loader = ListingLoader(response=response)
                suite_title = suite_div.xpath(
                    'normalize-space(.//div[contains(@class, "atcolor")]//div[contains(@class, "b24-roompanel-heading")]//div[contains(@class, "b24inline-block")]/text())').extract()
                if suite_title[0]:
                    suite_details = suite_title[0].split("/")
                    room_count_str = suite_details[0]
                    if room_count_str == "Studio":
                        room_count = 1
                        property_type = "studio"
                    else:
                        room_count = [int(word) for word in room_count_str.split() if word.isdigit()]
                        property_type = "apartment"

                    bathroom_type = suite_details[1].split()
                    bathroom_count = bathroom_type[0:2]
                    bathroom_count_str = " ".join(bathroom_count)
                    bathroom_count = bathroom_count_str.replace(" Bathroom", "")
                    bathroom_count = float(bathroom_count)
                    bathroom_count = round(bathroom_count)
                    bathroom_count = int(bathroom_count)

                    suite_type = bathroom_type[2:]
                    suite_type = " ".join(suite_type)
                    compare_json_dlx = ""
                    if suite_type == "Townhouse":
                        property_type = "house"
                    if suite_type == "Deluxe Suite":
                        compare_json_dlx = " Dlx"
                    if compare_json_dlx:
                        compare_json = room_count_str + compare_json_dlx
                    else:
                        compare_json = room_count_str
                    suite_dict = list(filter(lambda x: x["room_count"] == compare_json, suites))[0]

                    address = suite_dict['address']
                    long, lat = extract_location_from_address(address)
                    longitude = str(long)
                    latitude = str(lat)
                    zipcode, city, no_address = extract_location_from_coordinates(longitude, latitude)

                    rent = suite_dict["rent"]
                    rent = float(rent)
                    rent = round(rent)
                    rent = int(rent) * 30

                    title = suite_dict['title']
                    suite_desc = (title.split("-"))[1]
                    if "studio" in suite_desc:
                        external_link = response.meta.get("external_link") + "#" + (suite_desc.strip()).replace(" ", "-")
                    else:
                        external_link = response.meta.get("external_link") + "#" + (suite_desc.strip()).replace(" ", "-") + "-suite"

                    external_id = suite_div.xpath('./@id').extract()
                    external_id = external_id[0].replace("roomid", "")

                    suite_images_1 = suite_div.xpath(
                        './/img[contains(@class, "bootstrap-carousel-img")]/@src').extract()
                    suite_images_2 = suite_div.xpath(
                        './/img[contains(@class, "bootstrap-carousel-img")]/@data-lazy-load-src').extract()
                    suite_images = suite_images_1 + suite_images_2
                    images = building_images + suite_images

                    suite_description = suite_div.xpath('.//ul//li/text()').extract()
                    description = " ".join(suite_description)
                    furnished = None
                    square_meters = None
                    elevator = None
                    for each_description in suite_description:
                        if "Fully furnished" in each_description:
                            furnished = True
                        if "square footage" in each_description.lower():
                            if ":" in each_description:
                                square_meters_string = (each_description.split(":"))[1]
                                square_meters_string = square_meters_string.strip()
                                if "-" in square_meters_string:
                                    square_meters_string = square_meters_string.split("-")
                                    square_meters = (int(square_meters_string[0]) + int(square_meters_string[1])) / 2
                                    square_meters = round(square_meters)
                                    square_meters = int(square_meters)
                                else:
                                    square_meters = int(square_meters_string)
                                square_meters = int(square_meters)
                        if "sq ft" in each_description:
                            square_meters_string = each_description.split("(")
                            square_meters_string = square_meters_string[1]
                            square_meters_string = square_meters_string.split(")")
                            square_meters_string = square_meters_string[0]
                            square_meters_string = square_meters_string.replace("sq ft", "")
                            if "-" in square_meters_string:
                                square_meters_string = square_meters_string.split("-")
                                square_meters = (int(square_meters_string[0]) + int(square_meters_string[1])) / 2
                                square_meters = round(square_meters)
                                square_meters = int(square_meters)
                            else:
                                square_meters = int(square_meters_string)
                            square_meters = int(square_meters)

                        if "terrace" in each_description.lower():
                            terrace = True
                        if "balcony" in each_description.lower():
                            balcony = True

                    landlord_name = "Del Suites"
                    landlord_number = "647-370-3504"
                    landlord_email = "info@delsuites.com"

                    floor_plan_images = []
                    for key in floor_plan_images_dict.keys():
                        if room_count_str in key:
                            if floor_plan_images_dict[key].startswith("https"):
                                floor_plan_images.append(floor_plan_images_dict[key])
                            else:
                                floor_plan_images.append("https://www.delsuites.com" + floor_plan_images_dict[key])
                    if not floor_plan_images:
                        floor_plan_images = None

                    # # MetaData
                    item_loader.add_value("external_link", external_link)  # String
                    item_loader.add_value("external_source", self.external_source)  # String

                    item_loader.add_value("external_id", external_id)  # String
                    item_loader.add_value("position", self.position)  # Int
                    item_loader.add_value("title", title)  # String
                    item_loader.add_value("description", description)  # String

                    # # Property Details
                    item_loader.add_value("city", city)  # String
                    item_loader.add_value("zipcode", zipcode)  # String
                    item_loader.add_value("address", address)  # String
                    item_loader.add_value("latitude", latitude)  # String
                    item_loader.add_value("longitude", longitude)  # String
                    # item_loader.add_value("floor", floor) # String
                    item_loader.add_value("property_type", property_type)
                    # String => ["apartment", "house", "room", "student_apartment", "studio"]
                    item_loader.add_value("square_meters", square_meters)  # Int
                    item_loader.add_value("room_count", room_count)  # Int
                    item_loader.add_value("bathroom_count", bathroom_count)  # Int

                    # item_loader.add_value("available_date", available_date) # String => date_format

                    item_loader.add_value("pets_allowed", pets_allowed)  # Boolean
                    item_loader.add_value("furnished", furnished)  # Boolean
                    item_loader.add_value("parking", parking)  # Boolean
                    item_loader.add_value("elevator", elevator)  # Boolean
                    item_loader.add_value("balcony", balcony)  # Boolean
                    item_loader.add_value("terrace", terrace)  # Boolean
                    item_loader.add_value("swimming_pool", swimming_pool)  # Boolean
                    item_loader.add_value("washing_machine", washing_machine)  # Boolean
                    item_loader.add_value("dishwasher", dishwasher)  # Boolean

                    # # Images
                    item_loader.add_value("images", images)  # Array
                    item_loader.add_value("external_images_count", len(images))  # Int
                    item_loader.add_value("floor_plan_images", floor_plan_images)  # Array

                    # # Monetary Status
                    item_loader.add_value("rent", rent)  # Int
                    # item_loader.add_value("deposit", deposit) # Int
                    # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
                    # item_loader.add_value("utilities", utilities) # Int
                    item_loader.add_value("currency", "CAD")  # String

                    # item_loader.add_value("water_cost", water_cost) # Int
                    # item_loader.add_value("heating_cost", heating_cost) # Int

                    # item_loader.add_value("energy_label", energy_label) # String

                    # # LandLord Details
                    item_loader.add_value("landlord_name", landlord_name)  # String
                    item_loader.add_value("landlord_phone", landlord_number)  # String
                    item_loader.add_value("landlord_email", landlord_email)  # String

                    i += 1

                    self.position += 1
                    yield item_loader.load_item()
