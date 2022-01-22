# -*- coding: utf-8 -*-
# Author: Asmaa Elshahat
import scrapy
from ..loaders import ListingLoader
from ..helper import extract_location_from_address


class SwbstadtischewohnungsbauPyspiderGermanySpider(scrapy.Spider):
    name = "SWBStadtischeWohnungsbau"
    start_urls = ['https://www.swb-schoenebeck.de/wohnungs-und-immobiliensuche/alle-angebote']
    allowed_domains = ["swb-schoenebeck.de"]
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
        pages_number = response.css('div.d-flex.mb-3 div.h2::text')[0].extract()
        pages_number = pages_number.strip()
        pages_number = pages_number.split()[0]
        pages_number = int(pages_number)
        pages_number = int(pages_number / 10) + (pages_number % 10 > 0)
        urls = ['https://www.swb-schoenebeck.de/wohnungs-und-immobiliensuche/alle-angebote']
        for i in range(pages_number - 1):
            page_url = 'https://www.swb-schoenebeck.de/wohnungs-und-immobiliensuche/alle-angebote/' + str(i + 2)
            urls.append(page_url)
        for url in urls:
            yield scrapy.Request(url, callback=self.parse_pages, dont_filter=True)

    def parse_pages(self, response):
        apartments_divs = response.css('div.expose-list div.expose.card.mb-3')
        for apartment_div in apartments_divs:
            url = apartment_div.css('div.d-md-flex div.data h2 a::attr(href)')[0].extract()
            address = apartment_div.css('div.d-md-flex div.data p.address::text')[0].extract()
            details = apartment_div.css('div.d-md-flex div.data div.details div.row div table tr')
            details_keys = details.xpath('.//td[1]/text()').extract()
            details_values = details.xpath('.//td[2]/text()').extract()
            details_dict = dict(zip(details_keys, details_values))
            floor_plan_images = apartment_div.css('div.d-md-flex div.data div.actions a.plan::attr(data-img)').extract()
            if len(floor_plan_images) >= 1:
                floor_plan_images = 'https://www.swb-schoenebeck.de/' + floor_plan_images[0]
                floor_plan_images = floor_plan_images.replace(' ', '%20')
            else:
                floor_plan_images = None
            yield scrapy.Request(url, callback=self.populate_item, meta={
                'address': address,
                'details_dict': details_dict,
                'floor_plan_images': floor_plan_images,
            })

    # 3. SCRAPING level 3
    def populate_item(self, response):
        address = response.meta.get('address')

        floor_plan_images = response.meta.get('floor_plan_images')

        details_dict = response.meta.get('details_dict')

        total_rent = details_dict['Gesamtmiete:']
        total_rent = total_rent.strip()
        if total_rent != '€':
            item_loader = ListingLoader(response=response)

            floor = details_dict['Lage im Haus:']
            floor = floor.replace('Etage', '')
            floor = floor.strip()
            floor = floor.replace('.', '')

            room_count = details_dict['Zimmer:']
            room_count = room_count.replace(",", ".")
            room_count = round(float(room_count))
            room_count = int(room_count)

            square_meters = details_dict['Wohnfläche:']
            square_meters = square_meters.replace('ca.', '')
            square_meters = square_meters.strip()
            square_meters = square_meters.split()[0]
            square_meters = round(float(square_meters))
            square_meters = int(square_meters)

            title = response.css('div.box.expose-info h1.h2::text').extract()

            external_id = response.css('div.box.expose-info p.lead::text')[0].extract()
            external_id = external_id.split(':')[1]
            external_id = external_id.strip()

            images_all_one = response.css('div.box.expose-info div.row div.object-images div.images a::attr(href)').extract()
            images_all_two = response.css('div.box.expose-info div.row div.object-images div.images div a::attr(href)').extract()
            images_all = images_all_one + images_all_two
            images = []
            for image in images_all:
                image = 'https://www.swb-schoenebeck.de/' + image
                image = image.replace(' ', '%20')
                if image != floor_plan_images:
                    images.append(image)

            apartment_keys_b = response.css('div.box.expose-info div.row div.object-details table tr th::text').extract()
            apartment_keys = []
            for key in apartment_keys_b:
                key = key.replace('\n', '')
                key = key.replace('\t', '')
                apartment_keys.append(key)
            apartment_values = response.css('div.box.expose-info div.row div.object-details table tr td::text').extract()
            apartment_dict = dict(zip(apartment_keys, apartment_values))

            rent = apartment_dict['Nettokaltmiete:']
            rent = rent.split()[0]
            rent = rent.replace(".", "")
            rent = rent.replace(",", ".")
            rent = round(float(rent))
            rent = int(rent)
            # Enforces rent between 0 and 40,000 please dont delete these lines
            if int(rent) <= 0 and int(rent) > 40000:
                return

            utilities = apartment_dict['Betriebskosten:']
            utilities = utilities.split()[0]
            utilities = utilities.replace(".", "")
            utilities = utilities.replace(",", ".")
            utilities = round(float(utilities))
            utilities = int(utilities)

            heating_cost = apartment_dict['Heizkosten:']
            heating_cost = heating_cost.split()[0]
            heating_cost = heating_cost.replace(".", "")
            heating_cost = heating_cost.replace(",", ".")
            heating_cost = round(float(heating_cost))
            heating_cost = int(heating_cost)

            deposit = None
            if 'Kaution:' in apartment_dict.keys():
                deposit = apartment_dict['Kaution:']
                deposit = deposit.split()[0]
                deposit = deposit.replace(".", "")
                deposit = deposit.replace(",", ".")
                deposit = round(float(deposit))
                deposit = int(deposit)

            amenities = response.css('div.box.expose-info div.row div.col-sm-6 table tr td::text').extract()
            balcony = None
            elevator = None
            for amenity in amenities:
                amenity = amenity.lower()
                if 'balkon' in amenity:
                    balcony = True
                if 'aufzug' in amenity:
                    elevator = True

            energy_values = response.css('div.box.expose-info table.table tr td::text').extract()
            energy_label = None
            bathroom_count = None
            for item in energy_values:
                if len(item) == 1 :
                    if not item.isdigit():
                        energy_label = item
                if 'bad' in item.lower():
                    bathroom_count = 1

            description = response.css('div.box.expose-info p::text').extract()
            description = ' '.join(description)
            terrace = None
            if 'Terrasse' in description:
                terrace = True

            zipcode_city = address.split(',')[1]
            zipcode_city = zipcode_city.strip()
            zipcode_city = zipcode_city.split()
            zipcode = zipcode_city[0]
            city = zipcode_city[1]
            longitude, latitude = extract_location_from_address(address)
            longitude = str(longitude)
            latitude = str(latitude)

            landlord_name = response.css('div.card.mt-5 div.card-body div.row div div.row div div.name::text').extract()
            landlord_data = response.css('div.card.mt-5 div.card-body div.row div div.row div address table tr td a::text').extract()
            landlord_number = landlord_data[0]
            landlord_email = landlord_data[1]

            property_type = 'apartment'

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

            # item_loader.add_value("available_date", available_date) # String => date_format

            #item_loader.add_value("pets_allowed", pets_allowed) # Boolean
            #item_loader.add_value("furnished", furnished) # Boolean
            #item_loader.add_value("parking", parking) # Boolean
            item_loader.add_value("elevator", elevator) # Boolean
            item_loader.add_value("balcony", balcony) # Boolean
            item_loader.add_value("terrace", terrace) # Boolean
            #item_loader.add_value("swimming_pool", swimming_pool) # Boolean
            #item_loader.add_value("washing_machine", washing_machine) # Boolean
            #item_loader.add_value("dishwasher", dishwasher) # Boolean

            # # Images
            item_loader.add_value("images", images) # Array
            item_loader.add_value("external_images_count", len(images)) # Int
            item_loader.add_value("floor_plan_images", floor_plan_images) # Array

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
