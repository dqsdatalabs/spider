# -*- coding: utf-8 -*-
# Author: Asmaa Elshahat
import scrapy
from ..loaders import ListingLoader
from ..helper import extract_location_from_address


class ImmobilienborsenordwestPyspiderGermanySpider(scrapy.Spider):
    name = "ImmobilienborseNordwest"
    start_urls = [
        'https://www.immoboerse-nordwest.de/mietobjekte-wohnen/?itemPerPage=50'
    ]
    allowed_domains = ['immoboerse-nordwest.de']
    country = 'germany'  # Fill in the Country's name
    locale = 'de'  # Fill in the Country's locale, look up the docs if unsure
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
        apartments_divs = response.xpath('.//div[contains(@class, "list-item")]')
        for apartment_div in apartments_divs:
            apartment_info = apartment_div.xpath('.//div[contains(@class, "list-info")]')
            rent_sqm_values = apartment_info.xpath(
                './/div[contains(@class, "list-info_middle")]//div//span[contains(@class, "value")]/text()').extract()
            rent_sqm_keys = apartment_info.xpath(
                './/div[contains(@class, "list-info_middle")]//div//span[contains(@class, "label")]/text()').extract()
            rent_sqm = dict(zip(rent_sqm_keys, rent_sqm_values))
            square_meters = rent_sqm["Wohnfläche"]
            square_meters = square_meters.split()[0]
            if square_meters != "m":
                rent = rent_sqm["Kaltmiete"]
                title = apartment_info.xpath('.//div[contains(@class, "list-info_top")]//h3//a/text()')[0].extract()
                apartment_url = apartment_info.xpath('.//div[contains(@class, "list-info_top")]//h3//a/@href')[
                    0].extract()
                url = "https://www.immoboerse-nordwest.de" + apartment_url + "/"
                address = apartment_info.xpath('.//div[contains(@class, "list-info_top")]//p/text()')[0].extract()
                landlord_name = apartment_info.xpath('.//div[contains(@class, "list-info_bottom")]//span/text()')[
                    0].extract()
                yield scrapy.Request(url, callback=self.populate_item, dont_filter=True, meta={
                    "square_meters": square_meters,
                    "rent": rent,
                    "title": title,
                    "address": address,
                    "landlord_name": landlord_name,
                })

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        square_meters = response.meta.get("square_meters")
        square_meters = int(square_meters)

        rent = response.meta.get("rent")
        rent = (rent.split())[0]
        rent = rent.replace(".", "")
        rent = rent.replace(",", ".")
        rent = float(rent)
        rent = round(rent)
        rent = int(rent)

        title = response.meta.get("title")

        address = response.meta.get("address")
        zipcode = address.split()[0]
        city = address.split()[1]
        longitude, latitude = extract_location_from_address(address)
        longitude = str(longitude)
        latitude = str(latitude)

        images = response.xpath(
            './/div[contains(@class, "object-gallery")]//div[contains(@class, "inner")]//a/@href').extract()

        external_id = response.xpath('.//p[contains(@class, "object-id")]/text()')[0].extract()
        external_id = external_id.split(":")[1]
        external_id = external_id.strip()

        description = response.xpath(
            './/div[contains(@class, "object-desc")]//div[contains(@id, "kurzbeschreibung")]//section//p/text()').extract()

        amenities = response.xpath(
            './/div[contains(@class, "object-desc")]//div[contains(@id, "ausstattung")]//section//p/text()').extract()
        terrace = None
        dishwasher = None
        elevator = None
        parking = None
        balcony = None
        washing_machine = None
        furnished = None
        for amenity in amenities:
            if "terrasse" in amenity.lower():
                terrace = True
            if "spülmaschine" in amenity.lower():
                dishwasher = True
            if "fahrstuhl" in amenity.lower():
                elevator = True
            if "garage" in amenity.lower():
                parking = True
            if "balkon" in amenity.lower():
                balcony = True
            if "waschmaschine" in amenity.lower():
                washing_machine = True
            if "waschmaschinenanschluss" in amenity.lower():
                washing_machine = None
            if "komplett möbliert" in amenity.lower():
                furnished = True
            if "stellplätze" in amenity.lower():
                parking = True
            if "pkw-" in amenity.lower():
                parking = True

        property_type = "apartment"
        if "Haus" in title:
            property_type = "house"

        landlord_name = response.xpath(
            './/div[contains(@class, "contact-person")]//div[contains(@class, "contact-person_middle")]//p[contains(@class, "contact_name")]//strong/text()').extract()
        landlord_number = response.xpath(
            './/div[contains(@class, "contact-person")]//div[contains(@class, "contact-person_middle")]//p[contains(@class, "contact_phone")]/text()')[
            0].extract()
        landlord_email = 'info@immoboerse-nordwest.de'

        room_count = 1

        # # MetaData
        item_loader.add_value("external_link", response.url)  # String
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
        item_loader.add_value("property_type",
                              property_type)  # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters)  # Int
        item_loader.add_value("room_count", room_count)  # Int
        # item_loader.add_value("bathroom_count", bathroom_count) # Int

        # item_loader.add_value("available_date", available_date) # String => date_format

        # item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        item_loader.add_value("furnished", furnished)  # Boolean
        item_loader.add_value("parking", parking)  # Boolean
        item_loader.add_value("elevator", elevator)  # Boolean
        item_loader.add_value("balcony", balcony)  # Boolean
        item_loader.add_value("terrace", terrace)  # Boolean
        # item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        item_loader.add_value("washing_machine", washing_machine)  # Boolean
        item_loader.add_value("dishwasher", dishwasher)  # Boolean

        # # Images
        item_loader.add_value("images", images)  # Array
        item_loader.add_value("external_images_count", len(images))  # Int
        # item_loader.add_value("floor_plan_images", floor_plan_images) # Array

        # # Monetary Status
        item_loader.add_value("rent", rent)  # Int
        # item_loader.add_value("deposit", deposit) # Int
        # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        # item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", "EUR")  # String

        # item_loader.add_value("water_cost", water_cost) # Int
        # item_loader.add_value("heating_cost", heating_cost) # Int

        # item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name)  # String
        item_loader.add_value("landlord_phone", landlord_number)  # String
        item_loader.add_value("landlord_email", landlord_email)  # String

        self.position += 1
        yield item_loader.load_item()
