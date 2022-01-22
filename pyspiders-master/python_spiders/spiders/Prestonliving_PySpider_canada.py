# -*- coding: utf-8 -*-
# Author: Fill with the developer's Name
import scrapy
from ..loaders import ListingLoader
from ..helper import extract_location_from_address, sq_feet_to_meters


class PrestonlivingPyspiderCanadaSpider(scrapy.Spider):
    name = "Prestonliving"
    start_urls = ['https://prestonliving.ca/']
    allowed_domains = ["prestonliving.ca"]
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
        apartments_div = response.xpath('.//ul[contains(@class, "properties-features")]/li')[:-2]
        for apartment_div in apartments_div:
            apartment_url = apartment_div.xpath('.//a/@href')[0].extract()
            apartment_url = "https://prestonliving.ca" + apartment_url
            address = apartment_div.xpath('normalize-space(.//div[contains(@class, "building-location")]/text())').extract()
            title = apartment_div.xpath('.//div[contains(@class, "caption")]//h3/text()').extract()
            yield scrapy.Request(url=apartment_url, callback=self.populate_item, meta={"address": address, "title": title})

    # 3. SCRAPING level 3
    def populate_item(self, response):
        apartments_available = response.xpath('.//div[contains(@class, "residential")]//ul[contains(@class, "wpb-tabs-menu")]//li')
        for apartment_available in apartments_available:
            item_loader = ListingLoader(response=response)

            title = response.meta.get("title")

            property_type = "apartment"

            address = response.meta.get("address")
            address_info = address[0].split(", ")
            city = address_info[1] + ", ON"
            zipcode = (address_info[2])[-7:]
            lon, lat = extract_location_from_address(address[0])
            latitude = str(lat)
            longitude = str(lon)

            facilities = response.xpath(
                './/div[contains(@class, "last-content")]//div[contains(@class, "facilities")]//label/text()').extract()
            elevator = None
            if "Elevator" in facilities:
                elevator = True
            parking = None
            if "Indoor Parking" or "Outdoor Parking" in facilities:
                parking = True
            swimming_pool = None
            if "Indoor Swimming Pool" or "Outdoor Swimming Pool" in facilities:
                swimming_pool = True
            pets_allowed = None
            if "Small pets allowed" or "Large dogs allowed" in facilities:
                pets_allowed = True

            apartment_url = apartment_available.xpath('.//a/@href')[0].extract()

            external_id = apartment_available.xpath('.//a//span[1]/text()').extract()
            title = title[0] + " - Unit " + external_id[0]

            rent = apartment_available.xpath('.//a//span[2]/text()').extract()
            rent = rent[0].replace("$", "")
            rent = rent.replace(",", "")
            rent = int(rent)

            square_meters = apartment_available.xpath('.//a//span[3]/text()').extract()
            square_meters = int(square_meters[0])

            room_count = apartment_available.xpath('.//a//span[4]/text()').extract()

            bathroom_count = apartment_available.xpath('.//a//span[5]/text()').extract()

            external_link = response.url + apartment_url

            landlord_number = response.xpath('.//a[contains(@class, "contact")]/text()')[0].extract()
            landlord_number = landlord_number.replace("(", "")
            landlord_number = landlord_number.replace(") ", "-")
            landlord_name = "Preston Living"

            apartment_id = apartment_available.xpath('.//a/@data-unitid').extract()

            # # MetaData
            item_loader.add_value("external_link", external_link)  # String
            item_loader.add_value("external_source", self.external_source)  # String

            item_loader.add_value("external_id", external_id)  # String
            item_loader.add_value("title", title)  # String
            # item_loader.add_value("description", description) # String

            # # Property Details
            item_loader.add_value("city", city) # String
            item_loader.add_value("zipcode", zipcode) # String
            item_loader.add_value("address", address) # String
            item_loader.add_value("latitude", latitude) # String
            item_loader.add_value("longitude", longitude) # String
            # item_loader.add_value("floor", floor) # String
            item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
            item_loader.add_value("square_meters", square_meters) # Int
            item_loader.add_value("room_count", room_count) # Int
            item_loader.add_value("bathroom_count", bathroom_count) # Int

            # item_loader.add_value("available_date", available_date) # String => date_format

            item_loader.add_value("pets_allowed", pets_allowed) # Boolean
            # item_loader.add_value("furnished", furnished) # Boolean
            item_loader.add_value("parking", parking)  # Boolean
            item_loader.add_value("elevator", elevator) # Boolean
            # item_loader.add_value("balcony", balcony) # Boolean
            # item_loader.add_value("terrace", terrace) # Boolean
            item_loader.add_value("swimming_pool", swimming_pool) # Boolean
            # item_loader.add_value("washing_machine", washing_machine) # Boolean
            # item_loader.add_value("dishwasher", dishwasher) # Boolean

            # # Monetary Status
            item_loader.add_value("rent", rent) # Int
            # item_loader.add_value("deposit", deposit) # Int
            # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
            # item_loader.add_value("utilities", utilities) # Int
            item_loader.add_value("currency", "CAD")  # String

            # item_loader.add_value("water_cost", water_cost) # Int
            # item_loader.add_value("heating_cost", heating_cost) # Int

            # item_loader.add_value("energy_label", energy_label) # String

            # # LandLord Details
            item_loader.add_value("landlord_name", landlord_name) # String
            item_loader.add_value("landlord_phone", landlord_number) # String
            # item_loader.add_value("landlord_email", landlord_email) # String

            url = "https://prestonliving.ca/Listing/ApartmentView?id=" + apartment_id[0] + "&lang=en-CA"

            yield scrapy.Request(url=url,  callback=self.apartment_view, meta={"item_loader": item_loader})

    # 4. SCRAPING level 4
    def apartment_view(self, response):
        item_loader = response.meta.get("item_loader")
        images_all = response.xpath('.//div[contains(@class, "tab-slider")]//a/@href').extract()
        images = []
        for image in images_all:
            if not image.startswith("http://www.youtube.com"):
                images.append(image)

        description_1 = response.xpath('.//div[contains(@class, "unit-notes")]//p//strong/text()').extract()
        description_2 = response.xpath('.//div[contains(@class, "unit-notes")]//ul//li/text()').extract()
        description = description_1 + description_2
        description = " ".join(description)

        amentities = response.xpath('.//ul[contains(@class, "amentity-links")]//li//span//label/text()').extract()
        balcony = None
        if "Balcony" in amentities:
            balcony = True
        dishwasher = None
        if "Dishwasher" in amentities:
            dishwasher = True

        # Images
        item_loader.add_value("images", images)  # Array
        item_loader.add_value("external_images_count", len(images))  # Int
        # item_loader.add_value("floor_plan_images", floor_plan_images)  # Array

        item_loader.add_value("description", description)

        item_loader.add_value("balcony", balcony)  # Boolean
        item_loader.add_value("dishwasher", dishwasher)  # Boolean

        item_loader.add_value("position", self.position)  # Int

        self.position += 1
        yield item_loader.load_item()

