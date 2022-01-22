# -*- coding: utf-8 -*-
# Author: Asmaa Elshahat
import scrapy
from ..loaders import ListingLoader
from ..helper import extract_location_from_address, extract_location_from_coordinates


class HbhbimmobilienPyspiderGermanySpider(scrapy.Spider):
    name = "HBHBImmobilien"
    start_urls = ['https://hb-hb-immobilien.de/wohnungen-zur-miete/']
    allowed_domains = ["hb-hb-immobilien.de"]
    country = 'germany' # Fill in the Country's name
    locale = 'de' # Fill in the Country's locale, look up the docs if unsure
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
        apartments_divs = response.css('div.property')
        for apartment_div in apartments_divs:
            apartment_all = apartment_div.css('div.property-container div.property-details')
            title = apartment_all.css('h3.property-title a::text')[0].extract()
            url = apartment_all.css('h3.property-title a::attr(href)')[0].extract()
            address_type = apartment_all.css('div.property-subtitle::text')[0].extract()
            property_data = apartment_all.css('div.property-data')
            external_id = property_data.css('div.data-objektnr_extern div.dd::text')[0].extract()
            room_count = property_data.css('div.data-anzahl_zimmer div.dd::text')[0].extract()
            square_meters = property_data.css('div.data-wohnflaeche div.dd::text')[0].extract()
            available_date = property_data.css('div.data-verfuegbar_ab div.dd::text')[0].extract()
            rent = property_data.css('div.data-kaltmiete div.dd::text')[0].extract()
            warm_rent = property_data.css('div.data-warmmiete div.dd::text')[0].extract()
            apartment_info = {
                "title": title,
                "address_type": address_type,
                "external_id": external_id,
                "room_count": room_count,
                "square_meters": square_meters,
                "available_date": available_date,
                "rent": rent,
                "warm_rent": warm_rent,
            }
            yield scrapy.Request(url, callback=self.populate_item, meta=apartment_info)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        title = response.meta.get('title')
        title = title.strip()

        address_type = response.meta.get('address_type')
        address = address_type.split(",")[0]
        property_type = "apartment"

        external_id = response.meta.get('external_id')

        room_count = response.meta.get('room_count')
        room_count = room_count.replace(",", ".")
        room_count = float(room_count)
        room_count = round(room_count)
        room_count = int(room_count)

        square_meters = response.meta.get('square_meters')
        square_meters = square_meters.split()[0]
        square_meters = square_meters.replace(",", ".")
        square_meters = float(square_meters)
        square_meters = round(square_meters)
        square_meters = int(square_meters)

        available_date = response.meta.get('available_date')
        if "sofort" in available_date:
            available_date = None
        else:
            available_date = available_date.split(".")
            day = available_date[0]
            month = available_date[1]
            year = available_date[2]
            available_date = year + "-" + month + "-" + day

        rent = response.meta.get('rent')
        rent = rent.split()[0]
        rent = rent.replace(".", "")
        rent = rent.replace(",", ".")
        rent = round(float(rent))
        rent = int(rent)

        warm_rent = response.meta.get('warm_rent')
        warm_rent = warm_rent.split()[0]
        warm_rent = warm_rent.replace(".", "")
        warm_rent = warm_rent.replace(",", ".")
        warm_rent = round(float(warm_rent))
        warm_rent = int(warm_rent)

        heating_cost = warm_rent - rent
        if heating_cost == 0:
            heating_cost = None

        property_details = response.css('div.property-details ul')
        floor = property_details.css('li.data-etage div.row div.dd::text').extract()

        bathroom_count = property_details.css('li.data-anzahl_badezimmer div.row div.dd::text').extract()
        if len(bathroom_count) >= 1:
            bathroom_count = bathroom_count[0]
            bathroom_count = bathroom_count.replace(",", ".")
            bathroom_count = float(bathroom_count)
            bathroom_count = round(bathroom_count)
            bathroom_count = int(bathroom_count)
        else:
            bathroom_count = None

        utilities = property_details.css('li.data-nebenkosten div.row div.dd::text').extract()
        if len(utilities) >= 1:
            utilities = utilities[0]
            utilities = utilities.split()[0]
            utilities = utilities.replace(".", "")
            utilities = utilities.replace(",", ".")
            utilities = round(float(utilities))
            utilities = int(utilities)
        else:
            utilities = None

        deposit = property_details.css('li.data-kaution div.row div.dd::text').extract()
        if len(deposit) >= 1:
            deposit = deposit[0]
            deposit = deposit.split()[0]
            deposit = deposit.replace(".", "")
            deposit = deposit.replace(",", ".")
            deposit = round(float(deposit))
            deposit = int(deposit)
        else:
            deposit = None

        images_raw = response.css('div#immomakler-galleria a::attr(href)').extract()
        images = []
        for image in images_raw:
            image = image.replace(" ", "%20")
            images.append(image)

        property_features = response.css('div.property-features ul li::text').extract()
        balcony = None
        terrace = None
        swimming_pool = None
        parking = None
        pets_allowed = None
        furnished = None
        elevator = None
        for item in property_features:
            if "Balkon" in item:
                balcony = True
            if "Terrasse" in item:
                terrace = True
            if "pool" in item:
                swimming_pool = True
            if "garage" in item:
                parking = True
            if "Haustiere erlaubt" in item:
                pets_allowed = True
            if "Vollmöbliert" in item:
                furnished = True
            if "aufzug" in item:
                elevator = True

        property_energy = response.css('div.property-epass ul li')
        property_energy_keys = property_energy.css('div.row div.dt::text').extract()
        property_energy_values = property_energy.css('div.row div.dd::text').extract()
        property_epass = dict(zip(property_energy_keys, property_energy_values))
        energy_label = None
        if "Energie\xadeffizienz\xadklasse" in property_epass.keys():
            energy_label = property_epass["Energie\xadeffizienz\xadklasse"]

        description = response.css('div.property-description div.panel-body')
        description_one = description.xpath('.//p[1]/text()').extract()
        description_one = " ".join(description_one)
        if len(description_one) < 150:
            description_two = description.xpath('.//p[2]/text()').extract()
            description_two = " ".join(description_two)
            description = description_one + description_two
        else:
            description = description_one
        description = description.replace("\n", "")

        landlord_data = response.css('div.property-contact div.panel-body ul li')
        landlord_name = landlord_data.css('div.row div.dd span.p-name::text').extract()
        landlord_email = landlord_data.css('div.row div.dd.u-email a::text').extract()
        landlord_number = landlord_data.css('div.row div.dd.p-tel a::text').extract()

        address = address + ", Germany"
        longitude, latitude = extract_location_from_address(address)
        longitude = str(longitude)
        latitude = str(latitude)
        zipcode, city, no_address = extract_location_from_coordinates(longitude, latitude)

        # # MetaData
        item_loader.add_value("external_link", response.url) # String
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
        item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters) # Int
        item_loader.add_value("room_count", room_count) # Int
        item_loader.add_value("bathroom_count", bathroom_count) # Int

        item_loader.add_value("available_date", available_date) # String => date_format

        item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking) # Boolean
        item_loader.add_value("elevator", elevator) # Boolean
        item_loader.add_value("balcony", balcony) # Boolean
        item_loader.add_value("terrace", terrace) # Boolean
        item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        #item_loader.add_value("washing_machine", washing_machine) # Boolean
        #item_loader.add_value("dishwasher", dishwasher) # Boolean

        # # Images
        item_loader.add_value("images", images) # Array
        item_loader.add_value("external_images_count", len(images)) # Int
        #item_loader.add_value("floor_plan_images", floor_plan_images) # Array

        # # Monetary Status
        item_loader.add_value("rent", rent) # Int
        item_loader.add_value("deposit", deposit) # Int
        #item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", "EUR") # String

        #item_loader.add_value("water_cost", water_cost) # Int
        item_loader.add_value("heating_cost", heating_cost) # Int

        item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name) # String
        item_loader.add_value("landlord_phone", landlord_number) # String
        item_loader.add_value("landlord_email", landlord_email) # String

        self.position += 1
        yield item_loader.load_item()
