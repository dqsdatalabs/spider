# -*- coding: utf-8 -*-
# Author: Adham Mansour
import scrapy
from scrapy import Request

from ..helper import remove_unicode_char, extract_number_only, extract_location_from_address, \
    extract_location_from_coordinates
from ..loaders import ListingLoader


class ProkeyCaSpider(scrapy.Spider):
    name = 'prokey_ca'
    allowed_domains = ['prokey.ca']
    start_urls = ['https://prokey.ca/']  # https not http
    country = 'canada'
    locale = 'en'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'
    keywords = {
        'pets_allowed': ['pet ', 'pets'],
        'furnished': ['furnish'],
        'parking': ['parking', 'garage'],
        'elevator': ['elevator'],
        'balcony': ['balcon'],
        'terrace': ['terrace'],
        'swimming_pool': ['pool', 'swim'],
        'washing_machine': ['washing', ' washer', 'laundry'],
        'dishwasher': ['dishwasher']
    }
    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    # 2. SCRAPING level 2
    def parse(self, response, **kwargs):
        cities = response.css('.menu-item-734 a::attr(href)').extract()
        for city in cities:
            if 'rent.prokey' in city:
                yield Request(url=city,
                              callback=self.cityparse)
    # 3. SCRAPING level 3
    def cityparse(self, response, **kwargs):
        rentals = response.css('.link-details .AddClickTrackParams::attr(href)').extract()

        for rental in rentals:
            yield Request(url=rental,
                          callback=self.populate_item)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        title = response.css('h1::text').extract_first()
        description =remove_unicode_char((((' '.join(response.css('.normaltext::text').extract()).replace('\n','')).replace('\t','')).replace('\r','')))
        latitude = response.css('.propertyLat::text').extract_first()
        longitude = response.css('.propertyLng::text').extract_first()
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)
        zipcode = response.css('.propertyZipCode::text').extract_first()
        amenities = response.css('#amenities-accordion li::text').extract()
        amenities =remove_unicode_char((((' '.join(amenities).replace('\n','')).replace('\t','')).replace('\r','')))
        property_type = 'apartment'

        images = response.css('.carousel-inner img::attr(src)').extract()
        images = [(i.split('?&width'))[0] for i in images]

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
        rentals = response.css('table')
        landlord_phone = response.css('.click_to_call::text').extract_first()
        counter = 1
        for rental in rentals:
            square_meters = None  # METERS #int(response.css('::text').extract_first())
            room_count = None  # int(response.css('::text').extract_first())
            bathroom_count = None  # int(response.css('::text').extract_first())
            deposit = None
            rent = None
            for info_box in rental.css('tr'):
                header = info_box.css('td:nth-child(1) ::text').extract_first()
                if header:
                    if header.lower() =='bed':
                        room_count = info_box.css('td:nth-child(2) ::text').extract_first()
                        if room_count:
                            room_count = int(extract_number_only(room_count))
                    elif header.lower() =='bath':
                        bathroom_count = info_box.css('td:nth-child(2) ::text').extract_first()
                        if bathroom_count:
                            bathroom_count = int(extract_number_only(bathroom_count))
                    elif header.lower() =='sq.ft.':
                        square_meters = info_box.css('td:nth-child(2) ::text').extract_first()
                        if square_meters:
                            square_meters = int(extract_number_only(extract_number_only(square_meters)))
                    elif header.lower() =='rent':
                        rent = info_box.css('td:nth-child(2) ::text').extract_first()
                        if rent:
                            rent = int(extract_number_only(extract_number_only(rent)))
                    elif header.lower() =='deposit':
                        deposit = info_box.css('td:nth-child(2) ::text').extract_first()
                        if deposit:
                            deposit = int(extract_number_only(extract_number_only(deposit)))



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
            item_loader.add_value("square_meters", square_meters) # Int
            item_loader.add_value("room_count", room_count) # Int
            item_loader.add_value("bathroom_count", bathroom_count) # Int

            # item_loader.add_value("available_date", available_date) # String => date_format also "Available", "Available Now" ARE allowed

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
            item_loader.add_value("deposit", deposit) # Int
            # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
            # item_loader.add_value("utilities", utilities) # Int
            item_loader.add_value("currency", "CAD") # String

            # item_loader.add_value("water_cost", water_cost) # Int
            # item_loader.add_value("heating_cost", heating_cost) # Int

            # item_loader.add_value("energy_label", energy_label) # String

            # # LandLord Details
            item_loader.add_value("landlord_name", 'pro key living') # String
            item_loader.add_value("landlord_phone", landlord_phone) # String
            # item_loader.add_value("landlord_email", landlord_email) # String

            self.position += 1
            counter += 1
            yield item_loader.load_item()
