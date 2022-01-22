# -*- coding: utf-8 -*-
# Author: Adham Mansour
import scrapy
from scrapy import Request

from ..helper import remove_unicode_char, extract_number_only, extract_location_from_coordinates
from ..loaders import ListingLoader


class SkylinelivingCaSpider(scrapy.Spider):
    name = 'skylineliving_ca'
    allowed_domains = ['skylineliving.ca']
    start_urls = ['https://www.skylineliving.ca/en/apartments/?bed=&bath=']  # https not http
    country = 'canada'
    locale = 'en'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'
    buildings_counter = 0
    position = 1
    keywords = {
        'pets_allowed': ['pet'],
        'furnished': ['furnish'],
        'parking': ['parking', 'garage'],
        'elevator': ['elevator'],
        'balcony': ['balcon'],
        'terrace': ['terrace'],
        'swimming_pool': ['pool', 'swim'],
        'washing_machine': ['washing', ' washer', 'laundry'],
        'dishwasher': ['dishwasher']
    }

    # 1. SCRAPING level 1
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    # 2. SCRAPING level 2
    def parse(self, response, **kwargs):
        total_rentals_number = int(extract_number_only(response.css('.page-total-items::text')))
        rentals = response.css('#main .grey::attr(href)').extract()
        for rental in rentals:
            self.buildings_counter +=1
            yield Request(url='https://www.skylineliving.ca'+rental,
                          callback=self.populate_item)
        if self.buildings_counter < total_rentals_number:
            yield Request(f'https://www.skylineliving.ca/en/apartments?bed=&bath=&start={self.buildings_counter}',callback=self.parse)


    # 3. SCRAPING level 3
    def populate_item(self, response):
        title = response.css('.property-header__title::text').extract_first()
        description = remove_unicode_char((((' '.join(response.css('.property-overview__description-text p ::text').extract()).replace('\n','')).replace('\t','')).replace('\r','')))
        lat_lng = response.css('.property-header::attr(data-latlng)').extract_first()
        lat_lng = lat_lng.split(',')
        latitude = lat_lng[0]
        longitude = lat_lng[1]

        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)
        if zipcode == '':
            zipcode = response.css('.\.property-header__details::text').extract_first()

        amenities = remove_unicode_char((((' '.join(response.css('.d-block::text').extract()).replace('\n','')).replace('\t','')).replace('\r','')))

        images = response.css('.property-overview__carousel a::attr(href)').extract()
        images = ['https://www.skylineliving.ca'+i for i in images]

        pets_allowed = None
        if any(word in description.lower() for word in self.keywords['pets_allowed']) or any(word in amenities.lower() for word in self.keywords['pets_allowed']):
            pets_allowed = True

        furnished = None
        if any(word in description.lower() for word in self.keywords['furnished']) or any(word in amenities.lower() for word in self.keywords['furnished']):
            furnished = True

        parking = None
        if any(word in description.lower() for word in self.keywords['parking']) or any(word in amenities.lower() for word in self.keywords['parking']):
            parking = True

        elevator = None
        if any(word in description.lower() for word in self.keywords['elevator']) or any(word in amenities.lower() for word in self.keywords['elevator']):
            elevator = True

        balcony = None
        if any(word in description.lower() for word in self.keywords['balcony']) or any(word in amenities.lower() for word in self.keywords['balcony']):
            balcony = True

        terrace = None
        if any(word in description.lower() for word in self.keywords['terrace']) or any(word in amenities.lower() for word in self.keywords['terrace']):
            terrace = True

        swimming_pool = None
        if any(word in description.lower() for word in self.keywords['swimming_pool']) or any(word in amenities.lower() for word in self.keywords['swimming_pool']):
            swimming_pool = True

        washing_machine = None
        if any(word in description.lower() for word in self.keywords['washing_machine']) or any(word in amenities.lower() for word in self.keywords['washing_machine']):
            washing_machine = True

        dishwasher = None
        if any(word in description.lower() for word in self.keywords['dishwasher']) or any(word in amenities.lower() for word in self.keywords['dishwasher']):
            dishwasher = True

        property_type = 'apartment'

        rentals = response.css('.mb-3.flex-column')
        counter = 1
        for rental in rentals:
            rent = int(extract_number_only(extract_number_only(rental.css('.property-types-rent__starting-at::text').extract_first())))
            room_count = int(extract_number_only(extract_number_only(rental.css('.property-types-rent__room-detail:nth-child(1)::text').extract_first())))
            if room_count == 0:
                room_count = 1
            bathroom_count = int(extract_number_only(extract_number_only(rental.css('.property-types-rent__room-detail:nth-child(2)::text').extract_first())))
            if bathroom_count == 0:
                bathroom_count = 1
            available_date = rental.css('.property-types-rent__now-available--available')
            if available_date:
                available_date = 'available now'
            else:
                available_date = None

            # # MetaData
            item_loader = ListingLoader(response=response)
            item_loader.add_value("external_link", response.url+'#'+str(counter))  # String
            item_loader.add_value("external_source", self.external_source)  # String

            # item_loader.add_value("external_id", external_id) # String
            item_loader.add_value("position", self.position)  # Int
            item_loader.add_value("title", title) # String
            item_loader.add_value("description", description) # String

            # # Property Details
            item_loader.add_value("city", city) # String
            item_loader.add_value("zipcode", zipcode) # String
            item_loader.add_value("address", address) # String
            item_loader.add_value("latitude", latitude) # String
            item_loader.add_value("longitude", longitude) # String
            # item_loader.add_value("floor", floor) # String
            item_loader.add_value("property_type", property_type) # String
            # item_loader.add_value("square_meters", square_meters) # Int
            item_loader.add_value("room_count", room_count) # Int
            item_loader.add_value("bathroom_count", bathroom_count) # Int

            item_loader.add_value("available_date", available_date) # String => date_format also "Available", "Available Now" ARE allowed

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
            # item_loader.add_value("floor_plan_images", floor_plan_images) # Array

            # # Monetary Status
            item_loader.add_value("rent", rent) # Int
            # item_loader.add_value("deposit", deposit) # Int
            # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
            # item_loader.add_value("utilities", utilities) # Int
            item_loader.add_value("currency", "CAD") # String

            # item_loader.add_value("water_cost", water_cost) # Int
            # item_loader.add_value("heating_cost", heating_cost) # Int

            # item_loader.add_value("energy_label", energy_label) # String

            # # LandLord Details
            item_loader.add_value("landlord_name", 'skyline living') # String
            item_loader.add_value("landlord_phone", '705-561-9034') # String
            # item_loader.add_value("landlord_email", landlord_email) # String
            counter +=1
            self.position += 1
            yield item_loader.load_item()
