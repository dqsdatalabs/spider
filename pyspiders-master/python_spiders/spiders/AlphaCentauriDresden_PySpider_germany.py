# -*- coding: utf-8 -*-
# Author: Asmaa Elshahat
import scrapy
from ..loaders import ListingLoader
from ..helper import extract_location_from_address


class AlphacentauridresdenPyspiderGermanySpider(scrapy.Spider):
    name = "AlphaCentauriDresden"
    start_urls = [
        'https://www.ac-dresden.de/de/0__1_1_0__/immobilien-wohnungen.html',
        'https://www.ac-dresden.de/de/0__2_1_0__/immobilien-haeuser.html'
    ]
    allowed_domains = ["ac-dresden.de"]
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
        pages_number = response.css('div.ulist_wrapper a.menue1::attr(href)').extract()
        urls = [response.url]
        if len(pages_number) >= 1:
            urls = []
            for i in range(len(pages_number)):
                page_url = 'https://www.ac-dresden.de' + pages_number[i]
                urls.append(page_url)
        for url in urls:
            yield scrapy.Request(url, callback=self.parse_pages, dont_filter=True)

    def parse_pages(self, response):
        apartments_divs = response.css('div#listenansicht div.wrapper_liste')
        for apartment_div in apartments_divs:
            property_type = None
            if 'wohnungen' in response.url:
                property_type = 'apartment'
            elif 'haeuser' in response.url:
                property_type = 'house'
            title = apartment_div.css('div.mg15 div.lb10 h2::text')[0].extract()
            external_id = apartment_div.css('div.mg15 div.lb11::text').extract()
            apartment_url = apartment_div.css('div.ex_wrapper.mg15 div.uliste div.ex_wrapper_pic a::attr(href)').extract()
            url = 'https://www.ac-dresden.de' + apartment_url[0]
            address = apartment_div.css('div.ex_wrapper.mg15 div.uliste1 div.iaus2::text').extract()
            area = apartment_div.css('div.ex_wrapper.mg15 div.uliste1 div.iaus2 div.rp_Gebiet::text').extract()
            rent = apartment_div.css('div.ex_wrapper.mg15 div.uliste1 div.iaus2 div.rp_mtlKaltmiete::text').extract()
            square_meters = apartment_div.css('div.ex_wrapper.mg15 div.uliste1 div.iaus2 div.rp_Wohnflcheca::text').extract()
            room_count = apartment_div.css('div.ex_wrapper.mg15 div.uliste1 div.iaus2 div.rp_Zimmeranzahl::text').extract()
            yield scrapy.Request(url, callback=self.populate_item, meta={
                'external_id': external_id,
                'title': title,
                'square_meters': square_meters,
                'room_count': room_count,
                'address': address,
                'area': area,
                'rent': rent,
                'property_type': property_type,
            })

    # 3. SCRAPING level 3
    def populate_item(self, response):
        title = response.meta.get('title')

        address = response.meta.get('address')
        stop_index = len(address)
        for item in address:
            if 'Stadtteil:' in item:
                stop_index = address.index(item)
        address = address[:stop_index]
        zipcode_city = address[1]
        address = ', '.join(address)
        zipcode_city = zipcode_city.split()
        zipcode = zipcode_city[0]
        city = zipcode_city[1]
        longitude, latitude = extract_location_from_address(address)
        longitude = str(longitude)
        latitude = str(latitude)

        property_type = response.meta.get('property_type')

        rent = response.meta.get('rent')
        if len(rent) >= 1:
            item_loader = ListingLoader(response=response)
            rent = rent[0]
            rent = rent.split(':')[1]
            rent = rent.strip()
            rent = rent.split()[0]
            rent = rent.replace(".", "")
            rent = rent.replace(",", ".")
            rent = round(float(rent))
            rent = int(rent)
            # Enforces rent between 0 and 40,000 please dont delete these lines
            if int(rent) <= 0 and int(rent) > 40000:
                return

            external_id = response.meta.get('external_id')
            external_id = external_id[0]
            external_id = external_id.split(':')[1]
            external_id = external_id.strip()

            square_meters = response.meta.get('square_meters')
            square_meters = square_meters[0]
            square_meters = square_meters.split(':')[1]
            square_meters = square_meters.strip()
            square_meters = square_meters.split()[0]
            square_meters = square_meters.replace(".", "")
            square_meters = square_meters.replace(",", ".")
            square_meters = round(float(square_meters))
            square_meters = int(square_meters)

            room_count = response.meta.get('room_count')
            room_count = room_count[0]
            room_count = room_count.split(':')[1]
            room_count = room_count.strip()
            room_count = room_count.replace(",", ".")
            room_count = round(float(room_count))
            room_count = int(room_count)

            images_all = response.css('ul.bannerscollection_zoominout_list li div img::attr(src)').extract()
            images = []
            for image in images_all:
                image = "https://www.ac-dresden.de" + image
                images.append(image)

            apartment_table = response.css('div.tablewrapper1 div.row')
            apartment_keys = apartment_table.css('div.eze2::text')[1:].extract()
            apartment_values = apartment_table.css('div.iaus3::text')[3:].extract()
            apartment_dict = dict(zip(apartment_keys, apartment_values))

            heating_cost = None
            utilities = None
            bathroom_count = None
            floor = None

            if 'Etage:' in apartment_dict.keys():
                floor = apartment_dict['Etage:']

            if 'Heizkosten:' in apartment_dict.keys():
                heating_cost = apartment_dict['Heizkosten:']
                heating_cost = heating_cost.split()[0]
                heating_cost = heating_cost.replace(".", "")
                heating_cost = heating_cost.replace(",", ".")
                heating_cost = round(float(heating_cost))
                heating_cost = int(heating_cost)

            if 'Nebenkosten:' in apartment_dict.keys():
                utilities = apartment_dict['Nebenkosten:']
                utilities = utilities.split()[0]
                utilities = utilities.replace(".", "")
                utilities = utilities.replace(",", ".")
                utilities = round(float(utilities))
                utilities = int(utilities)

            if 'Anzahl Badezimmer:' in apartment_dict.keys():
                bathroom_count = apartment_dict['Anzahl Badezimmer:']
                bathroom_count = bathroom_count.replace(",", ".")
                bathroom_count = round(float(bathroom_count))
                bathroom_count = int(bathroom_count)

            elevator = None
            parking = None
            furnished = None
            terrace = None
            balcony = None
            available_date = None
            energy_label = None
            deposit = None
            description = None

            if 'Fahrstuhl:' in apartment_dict.keys():
                elevator_exist = apartment_dict['Fahrstuhl:']
                elevator_exist = elevator_exist.lower()
                if elevator_exist == 'ja':
                    elevator = True

            if 'Anzahl der Parkflächen:' in apartment_dict.keys():
                parking_exist = apartment_dict['Anzahl der Parkflächen:']
                if len(parking_exist) >= 1:
                    parking = True

            if 'Möbliert:' in apartment_dict.keys():
                furnished_exist = apartment_dict['Möbliert:']
                furnished_exist = furnished_exist.lower()
                if furnished_exist == 'ja':
                    furnished = True

            if 'Terrasse:' in apartment_dict.keys():
                terrace = True

            if 'Bezugsfrei ab:' in apartment_dict.keys():
                available_date = apartment_dict['Bezugsfrei ab:']
                if 'sofort' in available_date.lower():
                    available_date = None
                else:
                    available_date = available_date.split(".")
                    day = available_date[0]
                    month = available_date[1]
                    year = available_date[2]
                    available_date = year + "-" + month + "-" + day

            if 'Energieeffizienz-Klasse:' in apartment_dict.keys():
                energy_label = apartment_dict['Energieeffizienz-Klasse:']

            if 'Kaution/ Genossenschaftsanteile:' in apartment_dict.keys():
                deposit = apartment_dict['Kaution/ Genossenschaftsanteile:']
                deposit = deposit.split()[0]
                deposit = int(deposit)
                deposit = deposit * rent

            if 'Objektbeschreibung:' in apartment_dict.keys():
                description = apartment_dict['Objektbeschreibung:']
                if 'Balkon' in description:
                    balcony = True

            landlord_name = 'Alpha Centauri GmbH | Mr. Klaus Birkenzöller'
            landlord_number = '+49 173 7981078'
            landlord_email = 'birkenzoeller@ac-dresden.de'

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

            # item_loader.add_value("pets_allowed", pets_allowed) # Boolean
            item_loader.add_value("furnished", furnished) # Boolean
            item_loader.add_value("parking", parking) # Boolean
            item_loader.add_value("elevator", elevator) # Boolean
            item_loader.add_value("balcony", balcony) # Boolean
            item_loader.add_value("terrace", terrace) # Boolean
            #item_loader.add_value("swimming_pool", swimming_pool) # Boolean
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
