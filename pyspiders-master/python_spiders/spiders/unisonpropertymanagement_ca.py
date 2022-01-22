# -*- coding: utf-8 -*-
# Author: Fill with the developer's Name
import scrapy
from ..loaders import ListingLoader
from ..helper import *


class UnisonpropertymanagementCaSpider(scrapy.Spider):
    name = "unisonpropertymanagement_ca"
    start_urls = ['https://www.unisonpropertymanagement.ca/calgary-rentals/?wplpage=1']
    allowed_domains = ["unisonpropertymanagement.ca"]
    country = 'canada'  # Fill in the Country's name
    locale = 'ca'  # Fill in the Country's locale, look up the docs if unsure
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
        pages_array = response.css('.pagination a::attr(href)').extract()
        pages_array.append(response.url)
        clean_urls = sorted(set(pages_array))[1:]
        for url in clean_urls:
            yield scrapy.Request(url, callback=self.parse_page)

    # 3. SCRAPING level 3
    def parse_page(self, response, **kwargs):
        all_pages = sorted(set(response.css('.wpl_prp_cont a:nth-child(1)::attr(href)').extract()))
        for page in all_pages:
            yield scrapy.Request(page, callback=self.populate_item)

    # 4. SCRAPING level 4
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        # rented?
        basic_labels = response.css('.wpl_category_1 .other::text').extract()
        basic_values = response.css('.wpl_category_1 span::text').extract()[1:]
        t = 0
        for i in range(1, len(basic_labels)):
            if basic_labels[i] == basic_labels[0]:
                t = i
        basic_labels = basic_labels[:t]
        basic_values = basic_values[:t]
        if 'Rented : ' in basic_labels:
            return

        # rent
        rent_string = response.css('.price_box::text').get()
        if rent_string == '':
            return
        rent = int(float(rent_string.split(' ')[1].replace(',', '')))

        # Read Images
        images = sorted(set(response.css(".wpl_gallery_image::attr(src)").extract()))

        # Description
        description_array = response.css('#wpl_prp_show_container p::text').extract()
        description = ''.join(description_array)

        # title
        title = response.css('.title_text::text').get()
        if title[0].isdigit():
            title = title[1:]

        # ID, Square meters, Room Count, bathroom Count, Property_type
        external_id = None
        property_type = None
        furnished = None
        room_count = None
        bathroom_count = None

        for i in range(len(basic_labels)):
            if basic_labels[i] == 'Listing ID : ':
                external_id = basic_values[i]
                continue

            if basic_labels[i] == 'Square Footage : ':
                sq_feet = int(float(basic_values[i].split(' ')[0].replace(',', '')))

                continue

            if basic_labels[i] == 'Bedrooms : ':
                room_count = int(float(basic_values[i]))
                continue

            if basic_labels[i] == 'Bathrooms : ':
                bathroom_count = int(float(basic_values[i]))
                continue

            if basic_labels[i] == 'Property Type : ':
                if basic_values[i] == "condos/townhouses":
                    property_type = 'apartment'
                else:
                    property_type = 'house'
                continue

            if basic_labels[i] == 'Furnishing : ':
                if basic_values[i] == 'Furnished':
                    furnished = True
                else:
                    furnished = False
                continue

        # currency
        currency = 'CAD'

        # Home features
        home_features = response.css('.wpl_category_5 .single::text').extract()
        t = 0
        for i in range(1, len(home_features)):
            if home_features[i] == home_features[0]:
                t = i
        home_features = home_features[:i]

        # dishwasher, Washing machine, Balcony
        dishwasher = None
        washing_machine = None
        for i in range(len(home_features)):
            if home_features[i] == 'Dishwasher':
                dishwasher = True
                continue

            if home_features[i] == 'Laundry - In Suite':
                washing_machine = True
                continue

            if home_features[i] == 'Balcony':
                balcony = True

        # Building features
        building_features = response.css('.wpl_category_12 .single::text').extract()

        # elevator, terrace
        elevator = None
        for i in range(len(building_features)):
            if building_features[i] == 'Elevator':
                elevator = True
                break
            if building_features[i] == 'Common Terrace':
                terrace = True

        # Address map
        address_map_labels = response.css('.wpl_category_2 .rows::text').extract()
        address_map_values = response.css('.wpl_category_2 span::text').extract()[1:]
        t = 0
        for i in range(1, len(address_map_labels)):
            if address_map_labels[i] == address_map_labels[0]:
                t = i
        address_map_labels = address_map_labels[:t]
        address_map_values = address_map_values[:t]

        # City, Street, name, zipcode, Floor Number, Longitude, Latitude
        floor = None
        for i in range(len(address_map_labels)):
            if address_map_labels[i] == 'Floor Number : ':
                floor = address_map_values[i]
        loc = response.css('.wpl-location::text').get().split()[1:]
        loca = ' '.join(loc)

        longitude, latitude = extract_location_from_address(loca)
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)

        # Neighborhood
        neighborhood = response.css(".neighborhood::text").extract()

        t = 0
        for i in range(1, len(neighborhood)):
            if neighborhood[i] == neighborhood[0]:
                t = i
        neighborhood = neighborhood[:t]

        # Parking, Swimming pool
        parking = None
        balcony = None
        swimming_pool = None
        terrace = None
        for i in range(len(neighborhood)):
            if neighborhood[i] == 'Playground/Park':
                parking = True

            if neighborhood[i] == 'Outdoor Pool':
                swimming_pool = True

        # landlord name
        landlord_name = response.css('.wpl_prp_container_content_top .name a::text').get()

        # landlord number
        landlord_number = response.css('.tel a::text').get()
        if landlord_number == '':
            landlord_number = response.css('.elementor-mobile-align-center .elementor-button-text::text').get()

        # landlord email (encoded)
        encoded_string = response.css('.email a::attr(href)').get().split('#')[1]
        r = int(encoded_string[:2], 16)
        landlord_email = ''.join([chr(int(encoded_string[i:i + 2], 16) ^ r) for i in range(2, len(encoded_string), 2)])

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
        item_loader.add_value("latitude", str(latitude))  # String
        item_loader.add_value("longitude", str(longitude))  # String
        item_loader.add_value("floor", floor)  # String
        item_loader.add_value("property_type",
                              property_type)  # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", sq_feet)  # Int
        item_loader.add_value("room_count", room_count)  # Int
        item_loader.add_value("bathroom_count", bathroom_count)  # Int

        # item_loader.add_value("available_date", available_date) # String => date_format

        # item_loader.add_value("pets_allowed", pets_allowed)  # Boolean
        item_loader.add_value("furnished", furnished)  # Boolean
        item_loader.add_value("parking", parking)  # Boolean
        item_loader.add_value("elevator", elevator)  # Boolean
        item_loader.add_value("balcony", balcony)  # Boolean
        item_loader.add_value("terrace", terrace)  # Boolean
        item_loader.add_value("swimming_pool", swimming_pool)  # Boolean
        item_loader.add_value("washing_machine", washing_machine)  # Boolean
        item_loader.add_value("dishwasher", dishwasher)  # Boolean

        # # Images
        item_loader.add_value("images", images)  # Array
        item_loader.add_value("external_images_count", len(images))  # Int
        # item_loader.add_value("floor_plan_images", floor_plan_images) # Array

        # # Monetary Status
        item_loader.add_value("rent", rent)  # Int
        # item_loader.add_value("deposit", deposit)  # Int
        # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        # item_loader.add_value("utilities", utilities)  # Int
        item_loader.add_value("currency", currency)  # String

        # item_loader.add_value("water_cost", water_cost) # Int
        # item_loader.add_value("heating_cost", heating_cost) # Int

        # item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name)  # String
        item_loader.add_value("landlord_phone", landlord_number)  # String
        item_loader.add_value("landlord_email", landlord_email)  # String

        self.position += 1

        yield item_loader.load_item()
