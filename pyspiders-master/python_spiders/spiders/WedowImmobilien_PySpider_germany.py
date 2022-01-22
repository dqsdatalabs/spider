# -*- coding: utf-8 -*-
# Author: Asmaa Elshahat
import scrapy
from ..loaders import ListingLoader
from ..helper import extract_location_from_address


class WedowimmobilienPyspiderGermanySpider(scrapy.Spider):
    name = "WedowImmobilien"
    start_urls = ['https://wedowimmobilien.de/angebotstyp/vermietung']
    allowed_domains = ["wedowimmobilien.de"]
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
        pages_number = response.css('div.post-nav div.post-nav-numbers a.page-numbers::text')[-2].extract()
        pages_number = int(pages_number)
        urls = ['https://wedowimmobilien.de/angebotstyp/vermietung']
        for i in range(pages_number-1):
            url = 'https://wedowimmobilien.de/angebotstyp/vermietung/page/' + str(i+2)
            urls.append(url)
        for url in urls:
            yield scrapy.Request(url, callback=self.parse_pages , dont_filter=True)

    def parse_pages(self, response):
        apartments_divs = response.css('div.prop-item div.propbox')
        for apartment_div in apartments_divs:
            url = apartment_div.css('a::attr(href)')[0].extract()
            title = apartment_div.css('div.prop-all-left h2::text')[0].extract()
            property_type = apartment_div.css('div.prop-all-left div.propdata div.prop-location::text')[0].extract()
            if "wohnung" in property_type.lower():
                rent = apartment_div.css('div.prop-all-left div.prop-price::text')[0].extract()
                square_meters = apartment_div.css('div.prop-all-left div.prop-icons span.prop-size::text')[0].extract()
                room_count = apartment_div.css('div.prop-all-left div.prop-icons span.prop-rooms::text')[0].extract()
                bathroom_count = apartment_div.css('div.prop-all-left div.prop-icons span.prop-bathrooms::text')[0].extract()
                yield scrapy.Request(url, callback=self.populate_item, meta={
                    'title': title,
                    'property_type': property_type,
                    'rent': rent,
                    'room_count': room_count,
                    'bathroom_count': bathroom_count,
                    'square_meters': square_meters,
                })

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        title = response.meta.get('title')

        property_type = response.meta.get('property_type')
        property_type = property_type.strip()
        property_type = " ".join(property_type.split())
        property_type = property_type.split('|')
        city = property_type[0]
        property_type = 'apartment'

        bathroom_count = response.meta.get('bathroom_count')
        bathroom_count = bathroom_count.split()[0]
        bathroom_count = bathroom_count.replace(",", ".")
        bathroom_count = round(float(bathroom_count))
        bathroom_count = int(bathroom_count)

        rent = response.meta.get('rent')
        rent = rent.split()[0]
        rent = rent.replace(".", "")
        rent = rent.replace(",", ".")
        rent = round(float(rent))
        rent = int(rent)
        # Enforces rent between 0 and 40,000 please dont delete these lines
        if int(rent) <= 0 and int(rent) > 40000:
            return

        square_meters = response.meta.get('square_meters')
        square_meters = square_meters.split('m')[0]
        square_meters = square_meters.replace(".", "")
        square_meters = square_meters.replace(",", ".")
        square_meters = round(float(square_meters))
        square_meters = int(square_meters)

        room_count = response.meta.get('room_count')
        room_count = room_count.split()[0]
        room_count = room_count.replace(",", ".")
        room_count = round(float(room_count))
        room_count = int(room_count)

        external_id = response.css('div.propfacts div::text')[0].extract()
        external_id = external_id.strip()

        address = response.css('div.propaddress p::text')[0].extract()
        address = address.strip()
        zipcode = address.split()[0]

        images_values = response.css('div.gallery-slider div.slick-slide img::attr(src)').extract()
        images_keys = response.css('div.gallery-slider div.slick-slide img::attr(alt)').extract()
        images_dict = dict(zip(images_keys, images_values))
        floor_plan_images = []
        images = []
        for key in images_dict.keys():
            if 'Grundriss' in key:
                floor_plan_images.append(images_dict[key])
            else:
                images.append(images_dict[key])

        description = response.css('div#tabContainer div#tab1 p::text').extract()
        description = " ".join(description)
        if 'Haben wir' in description:
            description = description[:description.index('Haben wir')]
        description = " ".join(description.split())

        balcony = None
        terrace = None
        parking = None
        floor = None
        utilities = None
        heating_cost = None
        deposit = None
        energy_label = None
        elevator = None
        pets_allowed = None

        if 'balkon' in description.lower():
            balcony = True

        apartment_details = response.css('div#tabContainer div#tab2 div.prop-all-data table tr')
        apartment_details_keys = apartment_details.css('td.keys::text').extract()
        apartment_details_keys_new = []
        for key in apartment_details_keys:
            key = key.strip()
            apartment_details_keys_new.append(key)
        apartment_details_values = apartment_details.xpath('.//td[2]/text()').extract()
        apartment_details_dict = dict(zip(apartment_details_keys_new, apartment_details_values))

        available_date = None
        if 'Verfügbar ab:' in apartment_details_dict.keys():
            available_date = apartment_details_dict['Verfügbar ab:']
            if 'sofort' in available_date.lower():
                available_date = None
            else:
                if ' ' in available_date:
                    available_date = available_date.split()[1]
                available_date = available_date.split(".")
                day = available_date[0]
                month = available_date[1]
                year = available_date[2]
                available_date = year + "-" + month + "-" + day

        if 'Nebenkosten:' in apartment_details_dict.keys():
            utilities = apartment_details_dict['Nebenkosten:']
            utilities = utilities.split()[0]
            utilities = utilities.replace(".", "")
            utilities = utilities.replace(",", ".")
            utilities = round(float(utilities))
            utilities = int(utilities)

        if 'Heizkosten:' in apartment_details_dict.keys():
            heating_cost = apartment_details_dict['Heizkosten:']
            heating_cost = heating_cost.split()[0]
            heating_cost = heating_cost.replace(".", "")
            heating_cost = heating_cost.replace(",", ".")
            heating_cost = round(float(heating_cost))
            heating_cost = int(heating_cost)

        if 'Kaution:' in apartment_details_dict.keys():
            deposit = apartment_details_dict['Kaution:']
            deposit = deposit.split()[0]
            deposit = deposit.replace(".", "")
            deposit = deposit.replace(",", ".")
            deposit = round(float(deposit))
            deposit = int(deposit)

        if 'Etage:' in apartment_details_dict.keys():
            floor = apartment_details_dict['Etage:']

        if 'Warmmiete:' in apartment_details_dict.keys():
            warm_rent = apartment_details_dict['Warmmiete:']
            warm_rent = warm_rent.split()[0]
            warm_rent = warm_rent.replace(".", "")
            warm_rent = warm_rent.replace(",", ".")
            warm_rent = round(float(warm_rent))
            warm_rent = int(warm_rent)
            if not heating_cost:
                heating_cost = warm_rent - rent

        if 'Anzahl TG-Stellplätze:' in apartment_details_dict.keys():
            parking = True
        if 'Stellplätze:' in apartment_details_dict.keys():
            parking = True

        if 'Balkone:' in apartment_details_dict.keys():
            balcony = True

        if 'Terrassen:' in apartment_details_dict.keys():
            terrace = apartment_details_dict['Terrassen:']

        apartment_energy = response.css('div#tabContainer div#tab6 div.prop-all-data table tr')
        apartment_energy_keys = apartment_energy.css('td.keys::text').extract()
        apartment_energy_keys_new = []
        for key in apartment_energy_keys:
            key = key.strip()
            apartment_energy_keys_new.append(key)
        apartment_energy_values = apartment_energy.xpath('.//td[2]/text()').extract()
        apartment_energy_dict = dict(zip(apartment_energy_keys_new, apartment_energy_values))

        if 'Energieeffizienzklasse:' in apartment_energy_dict.keys():
            energy_label = apartment_energy_dict['Energieeffizienzklasse:']

        amenities = response.css('div#tabContainer div#tab3 div.propfeatures span::text').extract()
        for amenity in amenities:
            if 'balkon' in amenity.lower():
                balcony = True
            if 'terrasse' in amenity.lower():
                terrace = True
            if 'aufzug' in amenity.lower():
                elevator = True
            if 'haustiere' in amenity.lower():
                pets_allowed = True

        if len(floor_plan_images) < 1:
            floor_plan_images = response.css('div#tabContainer div#tab8 div div a::attr(href)').extract()
            if len(floor_plan_images) < 1:
                floor_plan_images = None

        landlord_name = response.css('div.profilebox span.profilename span::text').extract()
        landlord_email = response.css('div.profilebox div.profilecontact span.profilemail span a::text').extract()
        landlord_number = response.css('div.profilebox div.profilecontact span.profilephone span a::text').extract()

        longitude, latitude = extract_location_from_address(address)
        longitude = str(longitude)
        latitude = str(latitude)

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
        #item_loader.add_value("furnished", furnished) # Boolean
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
