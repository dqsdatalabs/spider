# -*- coding: utf-8
# Author: Fill with the developer's Name
import scrapy
from ..loaders import ListingLoader
from ..helper import *


class GrassrootspropertymanagementManagebuildingComSpider(scrapy.Spider):
    name = "grassrootspropertymanagement_managebuilding_com"
    start_urls = ['https://grassrootspropertymanagement.managebuilding.com/Resident/public/rentals']
    allowed_domains = ["grassrootspropertymanagement.managebuilding.com"]
    country = 'canada'  # Fill in the Country's name
    locale = 'en'  # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'

    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    # 2. SCRAPING level 2dat
    def parse(self, response, **kwargs):
        all_urls = response.xpath('/html/body/div[5]/div/div[1]/a[*]/@href').extract()

        for url in all_urls:
            yield scrapy.Request('https://grassrootspropertymanagement.managebuilding.com' + url,
                                 callback=self.populate_item)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        # Images
        images = response.css('.lightbox-link::attr(src)').extract()

        # Title
        title_raw = response.css('.title--margin-bottom::text').get().replace('\r\n', ' ').strip()

        title_raw = title_raw.replace(',','')

        title_split = title_raw.split()
        new_title=[]
        for i in title_split:
            if '#' not in i:
                new_title.append(i)
        title = ' '.join(new_title)

        # longitude, latitude
        longitude, latitude = extract_location_from_address(title)

        # Zipcode, city, address
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)
        longitude = str(longitude)
        latitude = str(latitude)
        # external id
        external_id = title_split[0]

        # Rent
        rent = int(float(response.css('.title--small::text').get().strip().replace('$', '').split(' ')[0].replace(',', '')))

        # Available date
        available_date = response.css('.unit-detail__available-date::text').get().strip().split(' ')[1]

        # Description
        description = response.css(".unit-detail__description::text").get()
        desc_split = description.split()


        # Unit info
        unit_info = response.css('.unit-detail__unit-info-item::text').extract()

        # Bedrooms, Bathrooms, Square meter, property type
        room_count = None
        bathroom_count = None
        square_meters = None
        property_type = 'house'

        for i in unit_info:
            if "Bed" in i:
                room_count = int(float(i.split(' ')[0]))
            if '+' in i:
                for j in range(len(desc_split)):
                    if 'bed' in desc_split[j].lower() or 'bedroom' in desc_split[j].lower():
                        room_count = int(float(desc_split[j - 1]))
                        break
            if 'Bath' in i:
                bathroom_count = int(float(i.split(' ')[0]))
            if 'sqft' in i:
                square_meters = int(float(i.split(' ')[0]))
            if 'Studio' in i:
                property_type = 'studio'
                room_count = description.count("room")

        if bathroom_count is None:
            for j in range(len(desc_split)):
                if 'bath' in desc_split[j].lower() or 'bathroom' in desc_split[j].lower():
                    bathroom_count = int(float(desc_split[j - 1]))
                    break

        if bathroom_count is None and room_count is None:
            return

        # Features
        features = response.css('.column--sm5::text').extract()

        # pets_allowed, furnished, parking, elevator, balcony, terrace, swimming_pool, washing_machine, dishwasher
        pets_allowed = None
        furnished = None
        parking = None
        elevator = None
        balcony = None
        terrace = None
        washing_machine = None
        dishwasher = None

        for i in features:
            if 'furnished' in i.lower():
                furnished = True
            if 'laundry' in i.lower() or 'washer' in i.lower():
                washing_machine = True
            if 'dishwasher' in i.lower():
                dishwasher = True
            if 'balcony' in i.lower():
                balcony = True
            if 'parking' in i.lower():
                parking = True
            if 'pet friendly' in i.lower():
                pets_allowed = True
            if 'yard' in i.lower() or 'terrace' in i.lower():
                terrace = True

        for i in desc_split:
            if 'pet friendly' in i.lower() or 'cat friendly' in i.lower() or 'dog friendly' in i.lower() :
                pets_allowed = True
            if 'no pets' in i.lower():
                pets_allowed = False
            if 'furnished' in i.lower():
                furnished = True
            if 'laundry' in i.lower() or 'washer' in i.lower() or 'washing machine' in i.lower():
                washing_machine = True
            if 'unfinished' in i.lower():
                furnished = False
            if 'dishwasher' in i.lower():
                dishwasher = True
            if 'parking' in i.lower():
                parking = True
            if 'balcony' in i.lower():
                balcony = True
            if 'yard' in i.lower() or 'terrace' in i.lower():
                terrace = True

        # Deposit
        deposit = int(float(response.css('.column--sm12 div p::text').get().replace('$','').split(' ')[0].replace(',', '')))

        # Utilities
        utilities = None
        for i in range(len(desc_split)):
            if desc_split[i].lower() == 'utilities':
                for j in desc_split[i:]:
                    if '$' in j:
                        utilities = int(float(j.replace('$', '').split('/')[0]))
                        break

        # landlord name
        landlord_name = response.css('.text--ellipsis::text').get()

        # landlord phone
        landlord_number = response.css('p+ div a:nth-child(1)::text').get()

        # landlord email
        landlord_email = response.css('.company__email::text').get()

        new_desc = []
        for i in desc_split:
            if 'https://' in i.lower() or landlord_name in i.lower() or landlord_number in i.lower() or landlord_email in i.lower():
                continue
            else:
                new_desc.append(i)

        description = ' '.join(new_desc)

        # # MetaData
        item_loader.add_value("external_link", response.url)  # String
        item_loader.add_value("external_source", self.external_source)  # String

        item_loader.add_value("external_id", external_id) # String
        item_loader.add_value("position", self.position)  # Int
        item_loader.add_value("title", title)  # String
        item_loader.add_value("description", description)  # String

        # # Property Details
        item_loader.add_value("city", city) # String
        item_loader.add_value("zipcode", zipcode) # String
        item_loader.add_value("address", address) # String
        item_loader.add_value("latitude", latitude) # String
        item_loader.add_value("longitude", longitude) # String
        # item_loader.add_value("floor", floor) # String
        item_loader.add_value("property_type",
                              property_type)  # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters)  # Int
        item_loader.add_value("room_count", room_count)  # Int
        item_loader.add_value("bathroom_count", bathroom_count)  # Int

        item_loader.add_value("available_date", available_date)  # String => date_format

        item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking) # Boolean
        item_loader.add_value("elevator", elevator) # Boolean
        item_loader.add_value("balcony", balcony) # Boolean
        item_loader.add_value("terrace", terrace) # Boolean
        # item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        item_loader.add_value("washing_machine", washing_machine) # Boolean
        item_loader.add_value("dishwasher", dishwasher) # Boolean

        # # Images
        item_loader.add_value("images", images)  # Array
        item_loader.add_value("external_images_count", len(images)) # Int
        # item_loader.add_value("floor_plan_images", floor_plan_images) # Array

        # # Monetary Status
        item_loader.add_value("rent", rent)  # Int
        item_loader.add_value("deposit", deposit) # Int
        # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", "CAD") # String

        # item_loader.add_value("water_cost", water_cost) # Int
        # item_loader.add_value("heating_cost", heating_cost) # Int

        # item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name) # String
        item_loader.add_value("landlord_phone", landlord_number) # String
        item_loader.add_value("landlord_email", landlord_email) # String

        self.position += 1
        yield item_loader.load_item()
