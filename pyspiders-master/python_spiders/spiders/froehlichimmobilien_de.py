# -*- coding: utf-8 -*-
# Author: Abdulrahman Abbas
import scrapy
import w3lib.html

from ..helper import *
from ..loaders import ListingLoader


class FroehlichimmobilienDeSpider(scrapy.Spider):
    name = "froehlichimmobilien_de"
    start_urls = [
        'https://www.froehlichimmobilien.de/immobilienangebote/?mt=rent&category=14&radius=15',
    ]
    country = 'germany'  # Fill in the Country's name
    locale = 'de'  # Fill in the Country's locale, look up the docs if unsure
    external_source = "froehlichimmobilien_de_pyspider_germany_de"
    execution_type = 'testing'

    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    # 2. SCRAPING level 2
    def parse(self, response, **kwargs):
        apartment_page_links = response.xpath('//a[@class="immo-listing__image"]')
        yield from response.follow_all(apartment_page_links, self.populate_item)

    # 3. SCRAPING level 3
    def populate_item(self, response):

        title = response.xpath('//div[@class="container pt-5"]//div[@class="col-24 text-center pb-4"]//p//text()').get()
        info = remove_white_spaces(response.xpath('//div[@class="pb-4"]//text()').getall()[1])
        description = w3lib.html.remove_tags(info).split("Ausstattung")[0]

        property_details = response.xpath('//div[@class="col-24 col-lg-15"]//ul//text()').getall()
        details = response.xpath('//div[@class="pb-4"]//text()').getall()
        amenities = " ".join(property_details) + " ".join(details)

        terrace = False
        if string_found(['Terrassenwohnung', 'Terrasse', 'Terrasse (ca.)', 'Dachterrasse', 'Südterrasse'], amenities):
            terrace = True

        bathroom_count = None
        if string_found(['WC', 'Dusche', "Badewanne", 'Bad', 'Wohlfühlbad', 'Vollbad', 'Badezimmer', 'Gäste-WC'],
                        amenities):
            if "Gäste-WC" in amenities:
                bathroom_count = 2
            else:
                bathroom_count = 1

        rent = None
        if "Kaltmiete" in property_details:
            position = property_details.index("Kaltmiete")
            rent = int(convert_to_numeric(extract_number_only(property_details[position + 2])))

        utilities = None
        if "Nebenkosten" in property_details:
            position = property_details.index("Nebenkosten")
            utilities = int(convert_to_numeric(extract_number_only(property_details[position + 2])))

        external_id = None
        if "Objekt-Nr" in property_details:
            position = property_details.index("Objekt-Nr")
            external_id = property_details[position + 2]

        floor = None
        if "Lage im Objekt (Etage)" in property_details:
            position = property_details.index("Lage im Objekt (Etage)")
            floor = (property_details[position + 2])

        available_date = None
        if "verfügbar ab:" in property_details:
            position = property_details.index("verfügbar ab:")
            available_date = extract_date(property_details[position + 1])

        square_meters = int(convert_to_numeric(extract_number_only(
            response.xpath('//div[@class="container pt-5"]//ul/li[1]//span[@class="value"]//text()').getall())))
        room_count = response.xpath('//div[@class="container pt-5"]//ul/li[2]//span[@class="value"]//text()').get()
        if "," in room_count:
            room_count = int(convert_to_numeric(extract_number_only(room_count)))
            room_count += 1

        location = title.split("in ")[-1]
        longitude, latitude = extract_location_from_address(location)
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)

        energy_label = response.xpath('//ul[@class="epass__info-list"]//li[5]//span//text()').get()

        images = response.xpath('//div[@id="exGallery"]//a[not(@title="Logo")]//img//@src').getall()
        # # MetaData
        item_loader = ListingLoader(response=response)

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
        item_loader.add_value("latitude", str(latitude))  # String
        item_loader.add_value("longitude", str(longitude))  # String
        item_loader.add_value("floor", floor)  # String
        item_loader.add_value("property_type", "apartment")  # String
        item_loader.add_value("square_meters", square_meters)  # Int
        item_loader.add_value("room_count", room_count)  # Int
        item_loader.add_value("bathroom_count", bathroom_count)  # Int

        item_loader.add_value("available_date", available_date)  # String => date_format

        # item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        # item_loader.add_value("furnished", furnished) # Boolean
        # item_loader.add_value("parking", parking) # Boolean
        # item_loader.add_value("elevator", elevator) # Boolean
        # item_loader.add_value("balcony", balcony) # Boolean
        item_loader.add_value("terrace", terrace)  # Boolean
        # item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        # item_loader.add_value("washing_machine", washing_machine) # Boolean
        # item_loader.add_value("dishwasher", dishwasher) # Boolean

        # # Images
        item_loader.add_value("images", images)  # Array
        item_loader.add_value("external_images_count", len(images))  # Int
        # item_loader.add_value("floor_plan_images", floor_plan_images) # Array

        # # Monetary Status
        item_loader.add_value("rent", rent)  # Int
        # item_loader.add_value("deposit", deposit) # Int
        # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        item_loader.add_value("utilities", utilities)  # Int
        item_loader.add_value("currency", "EUR")  # String

        # item_loader.add_value("water_cost", water_cost) # Int
        # item_loader.add_value("heating_cost", heating_cost) # Int

        item_loader.add_value("energy_label", energy_label)  # String

        # # LandLord Details
        item_loader.add_value("landlord_name", "Froehlich Immobilien")  # String
        item_loader.add_value("landlord_phone", ' 0621/4962006')  # String
        # item_loader.add_value("landlord_email", landlord_email) # String

        self.position += 1
        yield item_loader.load_item()
