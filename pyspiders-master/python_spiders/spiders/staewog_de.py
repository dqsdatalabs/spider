# -*- coding: utf-8 -*-
# Author: Abdulrahman Abbas
import scrapy

from ..helper import extract_number_only, convert_to_numeric, extract_date, string_found, \
    extract_location_from_coordinates, extract_location_from_address, remove_white_spaces
from ..loaders import ListingLoader


class StaewogDeSpider(scrapy.Spider):
    name = "staewog_de"
    start_urls = [
        "https://staewog.de/freiraum/leben-und-wohnen?section=wohnungen&sort=&flaeche=&kaltmiete=&freiraumsuche=Show+apartments"]
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
        for item in response.xpath('//section[@id="suchergebnis"]//article[@class="row no-gutters mb-5 resultat"]'):
            urls = item.xpath('.//a[@class="underline"]//@href').get()
            yield scrapy.Request("https://staewog.de{}".format(urls), callback=self.populate_item)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        property_details = response.xpath('//table[@class="mietobjekt-attribute"]//text()').getall()
        external_id = remove_white_spaces(
            response.xpath('//div[@class="col-12 col-md-6 mb-5"]//p//text()').get().split(":")[-1]).replace("null", "")
        title1 = response.xpath('//div[@class="h1 --wider"]//text()').get()
        title2 = response.xpath('//div[@class="h4 --wider"]//text()').get()
        title = title1 + title2
        details1 = response.xpath('//div[@class="tag-container"]//span//text()').getall()
        amenities = " ".join(details1)

        rent = None
        if "Nettokaltmiete:" in property_details:
            position = property_details.index("Nettokaltmiete:")
            rent = int(convert_to_numeric(extract_number_only(property_details[position + 2])))

        heating_cost = None
        if "Heizkostenvorauszahlung:" in property_details:
            position = property_details.index("Heizkostenvorauszahlung:")
            heating_cost = convert_to_numeric(extract_number_only(property_details[position + 2]))

        deposit = None
        if "Kaution:" in property_details:
            position = property_details.index("Kaution:")
            deposit = property_details[position + 2]

        utilities = None
        if "Betriebskostenvorauszahlung:" in property_details:
            position = property_details.index("Betriebskostenvorauszahlung:")
            utilities = property_details[position + 2]

        property_type = 'apartment'

        square_meters = None
        if "Wohnfläche:" in property_details:
            position = property_details.index("Wohnfläche:")
            square_meters = int(convert_to_numeric(extract_number_only(property_details[position + 2])))

        street = None
        if "Straße:" in property_details:
            position = property_details.index("Straße:")
            street = property_details[position + 2].lower().replace("str.", "straße")

        district = None
        if "Ortsteil:" in property_details:
            position = property_details.index("Ortsteil:")
            district = property_details[position + 2]

        location = remove_white_spaces(district + " " + street).replace("bgm.", "Bürgermeister")

        longitude, latitude = extract_location_from_address(location)
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)

        room_count = None
        if "Zimmer:" in property_details:
            position = property_details.index("Zimmer:")
            room_count = convert_to_numeric(extract_number_only(property_details[position + 2]))
            x = isinstance(room_count, float)
            if x:
                room_count += 0.5

        floor = None
        if "Etage:" in property_details:
            position = property_details.index("Etage:")
            floor = property_details[position + 2]

        available_date = None
        if "Frei ab:" in property_details:
            position = property_details.index("Frei ab:")
            available_date = extract_date(property_details[position + 2])

        energy_label = None
        if "Energieeffizienzklasse:" in property_details:
            position = property_details.index("Energieeffizienzklasse:")
            if "Keine Angabe" not in property_details[position + 2]:
                energy_label = property_details[position + 2]

        elevator = False
        if string_found(['Aufzug', 'Fahrstuhl'], amenities):
            elevator = True

        balcony = False
        if string_found(['Balkone', "Balkon"], amenities):
            balcony = True

        bathroom_count = None
        if string_found(['WC', 'Dusche', "Badewanne"], amenities):
            bathroom_count = 1

        # # MetaData
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)  # String
        item_loader.add_value("external_source", self.external_source)  # String

        item_loader.add_value("external_id", external_id)  # String
        item_loader.add_value("position", self.position)  # Int
        item_loader.add_value("title", title)  # String
        # item_loader.add_value("description", description) # String

        # Property Details
        item_loader.add_value("city", city)  # String
        item_loader.add_value("zipcode", zipcode)  # String
        item_loader.add_value("address", location)  # String
        item_loader.add_value("latitude", latitude)  # String
        item_loader.add_value("longitude", longitude)  # String
        item_loader.add_value("floor", floor)  # String
        item_loader.add_value("property_type", property_type)  # String
        item_loader.add_value("square_meters", square_meters)  # Int
        item_loader.add_value("room_count", room_count)  # Int
        item_loader.add_value("bathroom_count", bathroom_count)  # Int

        item_loader.add_value("available_date", available_date)  # String => date_format

        # item_loader.add_value("pets_allowed", pets_allowed)  # Boolean
        # item_loader.add_value("furnished", furnished) # Boolean
        # item_loader.add_value("parking", parking)  # Boolean
        item_loader.add_value("elevator", elevator)  # Boolean
        item_loader.add_value("balcony", balcony)  # Boolean
        # item_loader.add_value("terrace", terrace)  # Boolean
        # item_loader.add_value("swimming_pool", swimming_pool)  # Boolean
        # item_loader.add_value("washing_machine", washing_machine) # Boolean
        # item_loader.add_value("dishwasher", dishwasher) # Boolean

        # # Images
        images = response.xpath('//div[@class="carousel-item  active "]//img//@src').getall()

        floor_plan_images = response.xpath('//div[@class="col-12 text-left"]//p[2]//@onclick').get()
        if floor_plan_images:
            floor_plan_images = \
                response.xpath('//div[@class="col-12 text-left"]//p[2]//@onclick').get().split("src:")[-1].split("}")[
                    0].replace("'", '').replace(" ", "")
        item_loader.add_value("images", images)  # Array
        item_loader.add_value("external_images_count", len(images))  # Int
        item_loader.add_value("floor_plan_images", f"https://staewog.de{floor_plan_images}")  # Array

        # # Monetary Status
        item_loader.add_value("rent", rent)  # Int
        item_loader.add_value("deposit", deposit)  # Int
        # item_loader.add_value("prepaid_rent", prepaid_rent)  # Int
        item_loader.add_value("utilities", utilities)  # Int
        item_loader.add_value("currency", "EUR")  # String

        # item_loader.add_value("water_cost", water_cost) # Int
        item_loader.add_value("heating_cost", heating_cost)  # Int
        item_loader.add_value("energy_label", energy_label)  # String

        # # LandLord Details
        item_loader.add_value("landlord_name", 'staewog')  # String
        item_loader.add_value("landlord_phone", ' 0471 9451-0')  # String
        item_loader.add_value("landlord_email", 'info@staewog.de')  # String

        self.position += 1
        yield item_loader.load_item()
