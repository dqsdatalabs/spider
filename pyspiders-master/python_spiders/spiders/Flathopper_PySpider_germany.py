# -*- coding: utf-8 -*-
# Author: Asmaa Elshahat
import scrapy
from ..loaders import ListingLoader
from ..helper import extract_location_from_coordinates, extract_location_from_address


class FlathopperPyspiderGermanySpider(scrapy.Spider):
    name = "Flathopper"
    start_urls = ['https://flathopper.de/de/wohnen-auf-zeit/umkreissuche-ergebnis.php?dsstadt=0&dstyp=Object+type&dsmietevon=0&dsmietebis=0&geostart=&geokreis=3&dsfreiabselect=egal']
    allowed_domains = ["flathopper.de"]
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
        apartments_divs = response.css('div.container div.row div.col-xs-12 div.clearfix.line')
        for apartment_div in apartments_divs:
            title = apartment_div.css('div.colright div.text1::text').extract()
            city = apartment_div.css('div.colright div.inner div.clearfix div div.text2 strong::text').extract()
            apartment_info = apartment_div.css('div.colright div.inner div.clearfix div div.text2::text').extract()
            apartment_url = apartment_div.css('div.colright div.inner div.clearfix div div.text3 a.btn_detail::attr(href)').extract()
            external_id = apartment_div.css('div.colright div.inner div.clearfix div div.text3 strong::text').extract()
            url = "https://flathopper.de" + apartment_url[0]
            apartment_icons = apartment_div.css('div.colright div.inner div.icons img::attr(title)').extract()
            yield scrapy.Request(url, callback=self.populate_item, meta={
                'title': title,
                'city': city,
                'apartment_info': apartment_info,
                'apartment_icons': apartment_icons,
                'external_id': external_id,
            })

    # 3. SCRAPING level 3
    def populate_item(self, response):
        title = response.meta.get('title')

        apartment_info = response.meta.get('apartment_info')
        apartment_info_mod = []
        for item in apartment_info:
            item = item.replace("\n", "")
            item = item.replace("\t", "")
            item = item.strip()
            if len(item) > 1:
                apartment_info_mod.append(item)
        square_meters = apartment_info_mod[0]
        square_meters = square_meters.replace("ca.", "")
        square_meters = square_meters.strip()
        square_meters = square_meters.split()[0]
        square_meters = square_meters.replace(".", "")
        square_meters = square_meters.replace(",", ".")
        square_meters = round(float(square_meters))
        square_meters = int(square_meters)

        property_type = apartment_info_mod[1]
        property_type = property_type.lower()
        if property_type == "wohnung":
            property_type = "apartment"
        elif property_type == "haus":
            property_type = "house"
        elif property_type == "apartment":
            property_type = "apartment"
        elif property_type == "zimmer":
            property_type = "room"
        else:
            property_type = None

        rent = apartment_info_mod[2]
        rent = rent.split()[0]
        rent = rent.replace(".", "")
        rent = rent.replace(",", ".")
        rent = round(float(rent))
        rent = int(rent)

        available_date = None
        if len(apartment_info_mod) == 4:
            available_date = apartment_info_mod[3]
            available_date = available_date.split()
            for item in available_date:
                if "." in item:
                    item = item.split(".")
                    day = item[0]
                    month = item[1]
                    year = item[2]
                    available_date = year + "-" + month + "-" + day

        apartment_icons = response.meta.get('apartment_icons')
        dishwasher = None
        washing_machine = None
        balcony = None
        parking = None
        elevator = None
        terrace = None
        for amenity in apartment_icons:
            if "spülmaschine" in amenity.lower():
                dishwasher = True
            if "waschmaschine" in amenity.lower():
                washing_machine = True
            if "parken" in amenity.lower():
                parking = True
            if "balkon" in amenity.lower():
                balcony = True
            if "aufzug" in amenity.lower():
                elevator = True
            if "terrasse" in amenity.lower():
                terrace = True

        external_id = response.meta.get('external_id')[0]
        external_id = external_id.split(":")[1]
        external_id = external_id.strip()

        apartment_box_keys = response.css('div#jsShortbox div.z div.cz1::text').extract()
        apartment_box_keys_mod = []
        for key in apartment_box_keys:
            key = key.replace("\n", "")
            key = key.replace("\t", "")
            apartment_box_keys_mod.append(key)
        apartment_box_values = response.css('div#jsShortbox div.z div.cz2::text').extract()
        apartment_box_values_mod = []
        for value in apartment_box_values:
            value = value.replace("\n", "")
            value = value.replace("\t", "")
            apartment_box_values_mod.append(value)
        apartment_box = dict(zip(apartment_box_keys_mod, apartment_box_values_mod))

        city = apartment_box['Stadt:']
        district = apartment_box['Stadteil:']
        address = district + ", " + city
        longitude, latitude = extract_location_from_address(address)
        longitude = str(longitude)
        latitude = str(latitude)
        zipcode, no_city, no_address = extract_location_from_coordinates(longitude, latitude)

        room_count = apartment_box["Zimmer:"]
        room_count = round(float(room_count))
        room_count = int(room_count)

        floor = apartment_box["Etage:"]

        deposit = apartment_box["Kaution:"]
        if "nach Vereinbarung" in deposit:
            deposit = None
        elif "MM" in deposit:
            deposit = deposit.replace("MM", "")
            deposit = deposit.strip()
            deposit = int(deposit)
            deposit = deposit * rent
        else:
            deposit = deposit.split()[0]
            deposit = deposit.replace('.', '')
            deposit = deposit.replace(',', '.')
            deposit = round(float(deposit))
            deposit = int(deposit)

        images = response.css('div#slider ul li img::attr(src)').extract()

        description = response.css('div.content::text').extract()

        energy = response.css('div.content ul li::text').extract()
        energy_label = None
        furnished = None
        for item in energy:
            if "Effizienzklasse" in item:
                energy_label = item.split(":")[1]
                energy_label = energy_label.strip()
            if "möbliert" in item:
                furnished = True

        landlord_name = "Flathopper GmbH"
        landlord_email = "info@flathopper.de"
        landlord_number = "+49 (0) 8031 - 20 66 341"

        # Enforces rent between 0 and 40,000 please dont delete these lines
        if int(rent) <= 0 and int(rent) > 40000:
            return
        if property_type:
            item_loader = ListingLoader(response=response)
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
            #item_loader.add_value("bathroom_count", bathroom_count) # Int

            item_loader.add_value("available_date", available_date) # String => date_format

            #item_loader.add_value("pets_allowed", pets_allowed) # Boolean
            item_loader.add_value("furnished", furnished) # Boolean
            item_loader.add_value("parking", parking) # Boolean
            item_loader.add_value("elevator", elevator) # Boolean
            item_loader.add_value("balcony", balcony) # Boolean
            item_loader.add_value("terrace", terrace) # Boolean
            #item_loader.add_value("swimming_pool", swimming_pool) # Boolean
            item_loader.add_value("washing_machine", washing_machine) # Boolean
            item_loader.add_value("dishwasher", dishwasher) # Boolean

            # # Images
            item_loader.add_value("images", images) # Array
            item_loader.add_value("external_images_count", len(images)) # Int
            #item_loader.add_value("floor_plan_images", floor_plan_images) # Array

            # # Monetary Status
            item_loader.add_value("rent", rent) # Int
            item_loader.add_value("deposit", deposit) # Int
            #item_loader.add_value("prepaid_rent", prepaid_rent) # Int
            #item_loader.add_value("utilities", utilities) # Int
            item_loader.add_value("currency", "EUR") # String

            #item_loader.add_value("water_cost", water_cost) # Int
            #item_loader.add_value("heating_cost", heating_cost) # Int

            item_loader.add_value("energy_label", energy_label) # String

            # # LandLord Details
            item_loader.add_value("landlord_name", landlord_name) # String
            item_loader.add_value("landlord_phone", landlord_number) # String
            item_loader.add_value("landlord_email", landlord_email) # String

            self.position += 1
            yield item_loader.load_item()
