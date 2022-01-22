# -*- coding: utf-8 -*-
# Author: Abdulrahman Abbas

import scrapy

from ..helper import string_found, extract_number_only, convert_to_numeric, \
    extract_date, extract_location_from_coordinates
from ..loaders import ListingLoader


class MittelweserImmobilienDeSpider(scrapy.Spider):
    name = "mittelweser_immobilien_de"
    start_urls = [
        'https://www.mittelweser-immobilien.de/miete/wohnung/',
        'https://www.mittelweser-immobilien.de/immobilien/n/1/o/3/r/3/v/2/',

    ]
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
        apartment_page_links = response.xpath('//a[@itemprop="url"]')
        yield from response.follow_all(apartment_page_links, self.populate_item)

    # 3. SCRAPING level 3
    def populate_item(self, response):

        title = response.xpath('//h4[@class="headline"]//text()').get()
        description = "".join(response.xpath('//div[@class="boxce"]//p[1]//text()').getall()).replace(
            "vermietung@ruebenack.de", "")
        property_details = response.xpath('//table[@class="content"]//text()').getall()
        additional_info = response.xpath('//div[@class="boxce"]//text()').getall()
        amenities = " ".join(property_details) + " ".join(additional_info)

        external_id = None
        if "Portal-Nummer:" in property_details:
            position = property_details.index("Portal-Nummer:")
            external_id = property_details[position + 1]

        rent = None
        if "Kaltmiete:" in property_details:
            position = property_details.index("Kaltmiete:")
            rent = int(convert_to_numeric(extract_number_only(property_details[position + 1])))

        utilities = None
        if "Nebenkosten:" in property_details:
            position = property_details.index("Nebenkosten:")
            utilities = property_details[position + 1]

        heating_cost = None
        if "Warmmiete:" in property_details:
            position = property_details.index("Warmmiete:")
            all_cost = convert_to_numeric(extract_number_only(property_details[position + 1]))
            heating_cost = int(all_cost - rent)

        deposit = None
        if "Kaution:" in property_details:
            position = property_details.index("Kaution:")
            deposit_value = property_details[position + 1]
            if "drei" in deposit_value:
                deposit_value = 3
                deposit = deposit_value * rent
            else:
                deposit_value = convert_to_numeric(extract_number_only(property_details[position + 1]))
                if deposit_value > 10:
                    deposit = deposit_value
                else:
                    deposit = deposit_value * rent

        property_type = 'apartment'
        if "Immobilienart" in property_details:
            position = property_details.index("Immobilienart")
            type = property_details[position + 1]
            if type == "Haus":
                property_type = "house"

        square_meters = None
        if "Wohnfläche:" in property_details:
            position = property_details.index("Wohnfläche:")
            square_meters = int(convert_to_numeric(extract_number_only(property_details[position + 1])))
        elif "Gesamt­fläche:" in property_details:
            position = property_details.index("Gesamt­fläche:")
            square_meters = int(convert_to_numeric(extract_number_only(property_details[position + 1])))

        address = None
        if "Ort:" in property_details:
            position = property_details.index("Ort:")
            address = property_details[position + 1]

        room_count = 1
        if "Zimmer:" in property_details:
            position = property_details.index("Zimmer:")
            room_count = convert_to_numeric(extract_number_only(property_details[position + 1]))
            x = isinstance(room_count, float)
            if x:
                room_count += 0.5

        floor = None
        if "Etage:" in property_details:
            position = property_details.index("Etage:")
            floor = property_details[position + 1]

        available_date = None
        if "Übernahme:" in property_details:
            position = property_details.index("Übernahme:")
            try:
                available_date = extract_date(property_details[position + 1])
            except:
                available_date = None
        energy_label = None
        if "Energieeffizienzklasse:" in additional_info:
            position = additional_info.index("Energieeffizienzklasse:")
            energy_label = additional_info[position + 1]

        elevator = False
        if string_found(['Aufzug'], amenities):
            elevator = True

        parking = False
        if string_found(['Stellplatz', 'Garage'], amenities):
            parking = True

        balcony = False
        if string_found(['Balkone', ], amenities):
            balcony = True

        terrace = False
        if string_found(['Terrasse'], amenities):
            terrace = True

        pets_allowed = False
        if string_found(['Haustierhaltung ist gestattet'], amenities):
            pets_allowed = True

        bathroom_count = None
        if "Badezimmer:" in property_details and "Gäste-WC:" in property_details:
            position = property_details.index("Gäste-WC:")
            guest_toilet = property_details[position + 1]
            if "✔" in guest_toilet:
                bathroom_count = 2
            else:
                nu_of_bathroom = int(convert_to_numeric(extract_number_only(guest_toilet))) + 1
                bathroom_count = nu_of_bathroom

        elif string_found(['Gäste WC', 'Gäste-WC'], amenities) and string_found(["Badezimmer"], amenities):
            bathroom_count = 2

        elif string_found(["Badezimmer"], amenities) or "Badezimmer:" in property_details:
            bathroom_count = 1

        longitude = response.xpath('//div[@id="map-content"]//@data-lon').get()
        latitude = response.xpath('//div[@id="map-content"]//@data-lat').get()
        try:
            zipcode, city, location = extract_location_from_coordinates(longitude, latitude)
        except:
            zipcode = None
            city = None
            location = None

        # MetaData
        if rent != 0 and latitude is not None:
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
            item_loader.add_value("address", (address))  # String
            item_loader.add_value("latitude", str(latitude))  # String
            item_loader.add_value("longitude", str(longitude))  # String
            item_loader.add_value("floor", floor)  # String
            item_loader.add_value("property_type", property_type)  # String
            item_loader.add_value("square_meters", square_meters)  # Int
            item_loader.add_value("room_count", room_count)  # Int
            item_loader.add_value("bathroom_count", bathroom_count)  # Int

            item_loader.add_value("available_date", available_date)  # String => date_format

            item_loader.add_value("pets_allowed", pets_allowed)  # Boolean
            # item_loader.add_value("furnished", furnished) # Boolean
            item_loader.add_value("parking", parking)  # Boolean
            item_loader.add_value("elevator", elevator)  # Boolean
            item_loader.add_value("balcony", balcony)  # Boolean
            item_loader.add_value("terrace", terrace)  # Boolean
            # item_loader.add_value("swimming_pool", swimming_pool)  # Boolean
            # item_loader.add_value("washing_machine", washing_machine) # Boolean
            # item_loader.add_value("dishwasher", dishwasher) # Boolean

            # # Images
            images = response.xpath('//div[@id="immobilie-images"]//a//@href').getall()
            floor_plan_images = response.xpath(
                '//div[@id="immobilie-images-images"]/a[@title="Obergeschoss"]//@href').get()
            item_loader.add_value("images", images)  # Array
            item_loader.add_value("external_images_count", len(images))  # Int
            item_loader.add_value("floor_plan_images", floor_plan_images)  # Array

            # # Monetary Status
            item_loader.add_value("rent", rent)  # Int
            item_loader.add_value("deposit", deposit)  # Int
            # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
            item_loader.add_value("utilities", utilities)  # Int
            item_loader.add_value("currency", "EUR")  # String

            # item_loader.add_value("water_cost", water_cost) # Int
            item_loader.add_value("heating_cost", heating_cost)  # Int

            item_loader.add_value("energy_label", energy_label)  # String

            # # LandLord Details
            landlord_name = response.xpath('//div[@class="result premium"]//p/text()').get()
            if "geschützter Identität" in landlord_name:
                landlord_name = "MITTELWESER | IMMOBILIEN"

            landlord_email = response.xpath('//div[@class="result premium"]//a[@class="anfrage3"]/text()').get()

            landlord_number = None
            number = response.xpath('//div[@class="result premium"]//p[@class="address"]/text()').get()
            if number:
                landlord_number = number.split(":")[-1]

            item_loader.add_value("landlord_name", landlord_name)  # String
            item_loader.add_value("landlord_phone", landlord_number)  # String
            item_loader.add_value("landlord_email", landlord_email)  # String

            self.position += 1
            yield item_loader.load_item()
