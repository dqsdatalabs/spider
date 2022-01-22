# -*- coding: utf-8 -*-
# Author: Fill with the developer's Name
import scrapy
from ..loaders import ListingLoader
from ..helper import *


class NorthlandmanagementCaSpider(scrapy.Spider):
    name = "northlandmanagement_ca"
    start_urls = ['https://www.northlandmanagement.ca/residential-properties/']
    allowed_domains = ["northlandmanagement.ca"]
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
        for url in response.css('.read-more::attr(href)').extract():
            yield scrapy.Request(url, callback=self.populate_item)

    # 3. SCRAPING level 3
    def populate_item(self, response):


        # title
        title = response.css('.banner-title::text').get().strip()

        # description
        description = ' '.join(response.css('.column-center p+ p::text').extract())

        # landlord
        landlord_email = 'enquiries@northland.ca'
        landlord_number = '604-730-6630'
        landlord_name = 'northlandmanagement'
        if 'contact' in response.css('.column-center p:nth-child(1)::text').get().split():
            ind = response.css('.column-center p:nth-child(1)::text').get().split().index("contact")
            landlord_name = response.css('.column-center p:nth-child(1)::text').get().split()[ind+1]
        if re.search(r'(\d{3}[-\.\s]??\d{3}[-\.\s]??\d{4}|\(\d{3}\)\s*\d{3}[-\.\s]??\d{4}|\d{3}[-\.\s]??\d{4})',
                              description):
            landlord_email = re.search(r'(\d{3}[-\.\s]??\d{3}[-\.\s]??\d{4}|\(\d{3}\)\s*\d{3}[-\.\s]??\d{4}|\d{3}[-\.\s]??\d{4})',
                              description)[0]
        if re.search(r'[\w.+-]+@[\w-]+\.[\w.-]+', description):
            landlord_email = re.search(r'[\w.+-]+@[\w-]+\.[\w.-]+', description)[0]

        description = description_cleaner(description)
        # images
        images = [i for i in response.css('.grouped_elements::attr(href)').extract() if '.jpg' in i]

        # location
        loc = response.css('.map-location::attr(data-url)').get()

        longitude, latitude = extract_location_from_address(loc)
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)

        # details
        details = response.css('.feature-list p::text').extract()
        # terrace, balcony, elevator
        pets_allowed = furnished = parking = elevator = balcony = terrace = swimming_pool = washing_machine = dishwasher = None
        pets_allowed, furnished, parking, elevator, balcony, terrace, swimming_pool, washing_machine, dishwasher = get_amenities(
            description, ' '.join(details))

        names = response.css('.suite-block div:nth-child(1) p::text').extract()
        size = response.css('.suite-block div:nth-child(2) p::text').extract()
        price = response.css('.suite-block div:nth-child(3) p::text').extract()
        avail = response.css('.suite-block div:nth-child(4) p::text').extract()
        room_count = bathroom_count = 1
        property_type = 'apartment'
        for i in range(len(avail)):
            item_loader = ListingLoader(response=response)
            if 'No' in avail[i]:
                return

            name = names[i]
            for j in ["apartment", "house", "room", "student_apartment", "studio"]:
                if j in name.lower():
                    property_type = j
            fin_title = title+' '+name
            rent = int(float(price[i].replace(',','').split('$')[1].split('/')[0]))
            square_feet = int(float(re.search(r'(.*)sq.', size[i])[0].split()[-1].replace('sq.', '').replace(',', '')))

            if re.search('\d+', name):
                room_count = re.search('\d+', name)[0]
                if 'bed' in name.split():
                    bathroom_count = re.search('\d+', name.split('bed')[1])[0]

            # Enforces rent between 0 and 40,000 please dont delete these lines
            if 0 >= int(rent) > 40000:
                return


            # # MetaData
            item_loader.add_value("external_link", response.url+'#'+str(self.position))
            item_loader.add_value("external_source", self.external_source) # String

            #item_loader.add_value("external_id", external_id) # String
            item_loader.add_value("position", self.position) # Int
            item_loader.add_value("title", fin_title) # String
            item_loader.add_value("description", description) # String

            # # Property Details
            item_loader.add_value("city", city) # String
            item_loader.add_value("zipcode", zipcode) # String
            item_loader.add_value("address", address) # String
            item_loader.add_value("latitude", latitude) # String
            item_loader.add_value("longitude", longitude) # String
            #item_loader.add_value("floor", floor) # String
            item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
            item_loader.add_value("square_meters", square_feet) # Int
            item_loader.add_value("room_count", room_count) # Int
            item_loader.add_value("bathroom_count", bathroom_count) # Int

            #item_loader.add_value("available_date", available_date) # String => date_format

            item_loader.add_value("pets_allowed", pets_allowed) # Boolean
            item_loader.add_value("furnished", furnished) # Boolean
            item_loader.add_value("parking", parking) # Boolean
            item_loader.add_value("elevator", elevator) # Boolean
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
            # item_loader.add_value("deposit", deposit) # Int
            #item_loader.add_value("prepaid_rent", prepaid_rent) # Int
            #item_loader.add_value("utilities", utilities) # Int
            item_loader.add_value("currency", "CAD") # String

            #item_loader.add_value("water_cost", water_cost) # Int
            #item_loader.add_value("heating_cost", heating_cost) # Int

            #item_loader.add_value("energy_label", energy_label) # String

            # # LandLord Details
            item_loader.add_value("landlord_name", landlord_name) # String
            item_loader.add_value("landlord_phone", landlord_number) # String
            item_loader.add_value("landlord_email", landlord_email) # String

            self.position += 1
            yield item_loader.load_item()

