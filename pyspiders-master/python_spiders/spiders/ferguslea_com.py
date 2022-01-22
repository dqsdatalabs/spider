# -*- coding: utf-8 -*-
# Author: Fill with the developer's Name
import scrapy
from ..loaders import ListingLoader
from ..helper import *


class FergusleaComSpider(scrapy.Spider):
    name = "ferguslea_com"
    start_urls = ['https://accoravillage.com/find-your-home/']
    allowed_domains = ["accoravillage.com"]
    country = 'canada' # Fill in the Country's name
    locale = 'en' # Fill in the Country's locale, look up the docs if unsure
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
        all_urls = response.css('#accora-unit-listing a::attr(href)').extract()
        unique_urls = set()
        for i in all_urls:
            unique_urls.add(i)
        urls = list(unique_urls)

        for url in urls:
            yield scrapy.Request(url, callback=self.populate_item)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        # Description
        description = response.css('p::text').get()

        # Title
        title = response.css('.elementor-element-b06f601 .elementor-size-default::text').get()

        # Rent
        rent = int(float(response.css('.jet-listing-dynamic-field__content::text').get().replace('$','').split('/')[0].replace(',','')))

        # Room Count, Property type
        property_type = 'apartment'
        room_count = None
        title_split = title.split()
        for i in range(len(title_split)):
            if 'room' in title_split[i].lower():
                room_count = int(title_split[i-1])
            if 'studio' in title_split[i].lower():
                property_type = 'studio'
            if 'home' in title_split[i].lower():
                property_type = 'house'

        # square_meters
        feat_2 = response.css('.jet-listing-dynamic-field__content li::text').extract()
        square_meters = None
        for i in feat_2:
            if 'sq. ft.' in i.lower():
                square_meters = sq_feet_to_meters(i.split()[0])

        # Features
        features = response.css('.jet-listing-dynamic-repeater__item div::text').extract()

        washing_machine = None
        dishwasher = None
        pets_allowed = None
        balcony = None
        parking = None
        swimming_pool = None
        terrace = None
        for i in features:
            if 'laundry' in i.lower():
                washing_machine = True
            if 'dishwasher' in i.lower():
                dishwasher = True
            if 'balcon' in i.lower():
                balcony = True
            if 'parking' in i.lower():
                parking = True
            if 'pet-friendly' in i.lower():
                pets_allowed = True
            if 'pool' in i.lower():
                swimming_pool = True
            if 'courtyard' in i.lower():
                terrace = True

        for i in description.split():
            if 'laundry' in i.lower():
                washing_machine = True
            if 'dishwasher' in i.lower():
                dishwasher = True
            if 'balcon' in i.lower():
                balcony = True
            if 'parking' in i.lower():
                parking = True
            if 'pet-friendly' in i.lower():
                pets_allowed = True
            if 'pool' in i.lower():
                swimming_pool = True
            if 'courtyard' in i.lower():
                terrace = True

        if room_count is None or square_meters is None:
            return

        # Landlord Number
        landlord_number = response.css('.elementor-element-53653061 a::text').get()

        # Images
        imgs_doc = response.xpath('//*[@id="jet-tabs-content-1491"]/div/div/section[1]').get().split()
        images = []
        for i in imgs_doc:
            if 'data-lazy-src="' not in i and 'src="' in i:
                images.append(i.replace('src=', '').replace('"', ''))

        # # MetaData
        item_loader.add_value("external_link", response.url) # String
        item_loader.add_value("external_source", self.external_source) # String

        # item_loader.add_value("external_id", external_id) # String
        item_loader.add_value("position", self.position) # Int
        item_loader.add_value("title", title) # String
        item_loader.add_value("description", description) # String

        # # Property Details
        item_loader.add_value("city", 'Ottawa') # String
        #item_loader.add_value("zipcode", zipcode) # String
        item_loader.add_value("address", 'Ottawa') # String
        #item_loader.add_value("latitude", latitude) # String
        #item_loader.add_value("longitude", longitude) # String
        #item_loader.add_value("floor", floor) # String
        item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters) # Int
        item_loader.add_value("room_count", room_count) # Int
        #item_loader.add_value("bathroom_count", bathroom_count) # Int

        #item_loader.add_value("available_date", available_date) # String => date_format

        item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        #item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking) # Boolean
        #item_loader.add_value("elevator", elevator) # Boolean
        item_loader.add_value("balcony", balcony) # Boolean
        item_loader.add_value("terrace", terrace) # Boolean
        item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        item_loader.add_value("washing_machine", washing_machine) # Boolean
        item_loader.add_value("dishwasher", dishwasher) # Boolean

        # # Images
        item_loader.add_value("images", images) # Array
        item_loader.add_value("external_images_count", len(images)) # Int
        #item_loader.add_value("floor_plan_images", floor_plan_images) # Array

        # # Monetary Status
        item_loader.add_value("rent", rent) # Int
        #item_loader.add_value("deposit", deposit) # Int
        #item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        #item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", "USD") # String

        #item_loader.add_value("water_cost", water_cost) # Int
        #item_loader.add_value("heating_cost", heating_cost) # Int

        #item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", 'Accora Village') # String
        item_loader.add_value("landlord_phone", landlord_number) # String
        item_loader.add_value("landlord_email", 'connect@accoravillage.com') # String

        self.position += 1
        yield item_loader.load_item()
