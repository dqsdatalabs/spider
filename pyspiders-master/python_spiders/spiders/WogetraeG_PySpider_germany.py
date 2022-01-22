# -*- coding: utf-8 -*-
# Author: Asmaa Elshahat
import scrapy
from ..loaders import ListingLoader
from ..helper import extract_location_from_address, extract_location_from_coordinates


class WogetraegPyspiderGermanySpider(scrapy.Spider):
    name = "WogetraeG"
    start_urls = ['https://www.wogetra.de/immobilien-nutzungsart/wohnen/?post_type=immomakler_object&center&radius=25&objekt-id&collapse=in&von-qm=0.00&bis-qm=90.00&von-zimmer=0.00&bis-zimmer=4.00&von-nettokaltmiete=0.00&bis-nettokaltmiete=600.00']
    allowed_domains = ["wogetra.de"]
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
        pages_number = response.css('div.paginator div.pages-nav span a::text')[-1].extract()
        pages_number = int(pages_number)
        urls = ['https://www.wogetra.de/immobilien-nutzungsart/wohnen/?post_type=immomakler_object&center&radius=25&objekt-id&collapse=in&von-qm=0.00&bis-qm=90.00&von-zimmer=0.00&bis-zimmer=4.00&von-nettokaltmiete=0.00&bis-nettokaltmiete=600.00']
        for i in range(pages_number - 1):
            url = 'https://www.wogetra.de/immobilien-nutzungsart/wohnen/page/' + str(i + 2) + '/?post_type=immomakler_object&center&radius=25&objekt-id&collapse=in&von-qm=0.00&bis-qm=90.00&von-zimmer=0.00&bis-zimmer=4.00&von-nettokaltmiete=0.00&bis-nettokaltmiete=600.00'
            urls.append(url)
        for url in urls:
            yield scrapy.Request(url, callback=self.parse_pages, dont_filter=True)

    def parse_pages(self, response):
        apartments_divs = response.css('div.properties div.row div.property div.property-container')
        for apartment_div in apartments_divs:
            url = apartment_div.css('div.property-details h3.property-title a::attr(href)')[0].extract()
            title = apartment_div.css('div.property-details h3.property-title a::text').extract()
            address = apartment_div.css('div.property-details div.property-subtitle::text').extract()
            apartment_keys = apartment_div.css('div.property-details div.property-data div div.dt::text').extract()
            apartment_values = apartment_div.css('div.property-details div.property-data div div.dd::text').extract()
            apartment_table = dict(zip(apartment_keys, apartment_values))
            yield scrapy.Request(url, callback=self.populate_item, meta={
                'title': title,
                'address': address,
                'apartment_table': apartment_table,
            })

    # 3. SCRAPING level 3
    def populate_item(self, response):
        title = response.meta.get('title')
        address = response.meta.get('address')
        apartment_table = response.meta.get('apartment_table')
        external_id = apartment_table['Objekt ID:']

        if 'Zimmer:' in apartment_table.keys():
            room_count = apartment_table['Zimmer:']
            room_count = room_count.replace(",", ".")
            room_count = round(float(room_count))
            room_count = int(room_count)
        else:
            room_count = None

        square_meters = apartment_table['Wohnfläche\xa0ca.:']
        square_meters = square_meters.split()[0]
        square_meters = square_meters.replace(".", "")
        square_meters = square_meters.replace(",", ".")
        square_meters = round(float(square_meters))
        square_meters = int(square_meters)

        available_date = apartment_table['Verfügbar ab:']
        if 'sofort' in available_date:
            available_date = None
        else:
            available_date = available_date.split(".")
            day = available_date[0]
            month = available_date[1]
            year = available_date[2]
            available_date = year + "-" + month + "-" + day

        rent = None
        if 'Nettokaltmiete:' in apartment_table.keys():
            rent = apartment_table['Nettokaltmiete:']
            rent = rent.split()[0]
            rent = rent.replace(".", "")
            rent = rent.replace(",", ".")
            rent = round(float(rent))
            rent = int(rent)
            # Enforces rent between 0 and 40,000 please dont delete these lines
            if int(rent) <= 0 and int(rent) > 40000:
                return

        images = response.css('div#immomakler-galleria a::attr(href)').extract()
        floor_plan_images = response.css('div.immomakler-grundrisse a::attr(href)').extract()

        apartment_details_div = response.css('div.property-details ul.list-group li.list-group-item')
        apartment_details_values = []
        apartment_details_keys = []
        for div in apartment_details_div:
            apartment_details_key = div.css('div div.dt::text').extract()
            apartment_details_keys.append(apartment_details_key[0])
            apartment_details_value = div.css('div div.dd::text').extract()
            if len(apartment_details_value) > 1:
                apartment_details_value = " ".join(apartment_details_value)
            apartment_details_values.append(apartment_details_value[0])
        apartment_details = dict(zip(apartment_details_keys, apartment_details_values))
        property_type = apartment_details['Objekttypen']
        if 'wohnung' in property_type.lower():
            property_type = 'apartment'
            floor = None
            if 'Etage' in apartment_details.keys():
                floor = apartment_details['Etage']

            if not room_count:
                if 'Zimmer' in apartment_details.keys():
                    room_count = apartment_details['Zimmer']

            utilities = None
            heating_cost = None
            if 'Nebenkosten' in apartment_details.keys():
                utilities = apartment_details['Nebenkosten']
                utilities = utilities.split()[0]
                utilities = utilities.replace(".", "")
                utilities = utilities.replace(",", ".")
                utilities = round(float(utilities))
                utilities = int(utilities)

            if 'Heizkosten' in apartment_details.keys():
                heating_cost = apartment_details['Heizkosten']
                heating_cost = heating_cost.split()[0]
                heating_cost = heating_cost.replace(".", "")
                heating_cost = heating_cost.replace(",", ".")
                heating_cost = round(float(heating_cost))
                heating_cost = int(heating_cost)

            amenities = response.css('div.property-features div.panel-body ul.list-group li.list-group-item::text').extract()
            balcony = None
            elevator = None
            bathroom_count = None
            for item in amenities:
                if 'balkon' in item.lower():
                    balcony = True
                if 'aufzug' in item.lower():
                    elevator = True
                if 'bad' in item.lower():
                    bathroom_count = 1
                if 'dusche' in item.lower():
                    bathroom_count = 1

            description = response.css('div.property-description div.panel-body p::text').extract()

            landlord_name = 'Wohnungsgenossenschaft Transport eG Leipzig'
            landlord_number = '0341 2238633'
            landlord_email = 'Wohnen@wogetra.de'

            address = address[0] + ', Germany'
            longitude, latitude = extract_location_from_address(address)
            longitude = str(longitude)
            latitude = str(latitude)
            zipcode, city, no_address = extract_location_from_coordinates(longitude, latitude)

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
            item_loader.add_value("bathroom_count", bathroom_count) # Int

            item_loader.add_value("available_date", available_date) # String => date_format

            #item_loader.add_value("pets_allowed", pets_allowed) # Boolean
            #item_loader.add_value("furnished", furnished) # Boolean
            #item_loader.add_value("parking", parking) # Boolean
            item_loader.add_value("elevator", elevator) # Boolean
            item_loader.add_value("balcony", balcony) # Boolean
            #item_loader.add_value("terrace", terrace) # Boolean
            #item_loader.add_value("swimming_pool", swimming_pool) # Boolean
            #item_loader.add_value("washing_machine", washing_machine) # Boolean
            #item_loader.add_value("dishwasher", dishwasher) # Boolean

            # # Images
            item_loader.add_value("images", images) # Array
            item_loader.add_value("external_images_count", len(images)) # Int
            item_loader.add_value("floor_plan_images", floor_plan_images) # Array

            # # Monetary Status
            item_loader.add_value("rent", rent) # Int
            #item_loader.add_value("deposit", deposit) # Int
            #item_loader.add_value("prepaid_rent", prepaid_rent) # Int
            item_loader.add_value("utilities", utilities) # Int
            item_loader.add_value("currency", "EUR") # String

            #item_loader.add_value("water_cost", water_cost) # Int
            item_loader.add_value("heating_cost", heating_cost) # Int

            #item_loader.add_value("energy_label", energy_label) # String

            # # LandLord Details
            item_loader.add_value("landlord_name", landlord_name) # String
            item_loader.add_value("landlord_phone", landlord_number) # String
            item_loader.add_value("landlord_email", landlord_email) # String

            self.position += 1
            yield item_loader.load_item()
