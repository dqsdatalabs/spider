# -*- coding: utf-8 -*-
# Author: Adham Mansour
import scrapy
from scrapy import Request

from ..helper import remove_unicode_char, extract_number_only, extract_location_from_address, \
    extract_location_from_coordinates, remove_white_spaces
from ..loaders import ListingLoader


class LabergeqcCaSpider(scrapy.Spider):
    name = 'laberge_qc_ca'
    allowed_domains = ['laberge.qc.ca']
    start_urls = ['https://laberge.qc.ca/search']  # https not http
    country = 'canada'
    locale = 'en'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'
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
    apart_size={'1':[2,1],
                '2':[2,1],
                '3':[2,1],
                '4':[3,1],
                '5':[4,1],
                '6':[5,1]}
    # 1. SCRAPING level 1
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    # 2. SCRAPING level 2
    def parse(self, response, **kwargs):
        rentals = response.css('.bg-white .title-link::attr(href)').extract()
        for rental in rentals:
            external_link = 'https://laberge.qc.ca'+rental
            yield Request(url=external_link,
                          callback=self.buildingparse)

    # 3. SCRAPING level 3
    def buildingparse(self, response):
        amenities = remove_unicode_char((((' '.join(response.css('.mb-3 p::text').extract()).replace('\n','')).replace('\t','')).replace('\r','')))
        rentals = response.css('tbody >tr')
        counter = 1
        address = response.css('.mb-0::text').extract_first()
        longitude, latitude = extract_location_from_address(address)
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)
        images = response.css('.owl-carousel img::attr(src)').extract()
        images = ['https://laberge.qc.ca/'+i for i in images]
        for rental in rentals:
            external_link = rental.css('td a::attr(href)').extract_first()
            if external_link:
                yield Request(url='https://laberge.qc.ca'+external_link+'#'+str(counter),callback=self.populateitems,
                              meta={'amenities' : amenities,
                                    'latitude' : str(latitude),
                                    'longitude': str(longitude),
                                    'zipcode' : zipcode,
                                    'city': city,
                                    'address': address,
                                    'images' : images})

    def populateitems(self, response):
        title = response.css('h1::text').extract_first()
        description = remove_unicode_char((((' '.join(response.css('.col-appart-none ::text').extract()).replace('\n','')).replace('\t','')).replace('\r','')))
        property_type = 'apartment'
        info_col = response.css('.col-sm-4')
        size = None
        floor = None
        available_date = None
        room_count = None
        bathroom_count = None
        for i in info_col:
            header = i.css('h3 ::text').extract_first()
            if header:
                header = header.lower()
                if header == 'size':
                    size = i.css('p ::text').extract_first()
                    room_count, bathroom_count = self.apart_size[size[0]]
                if header == 'floor':
                    floor = i.css('p ::text').extract_first()
                if header == 'availability':
                    available_date = i.css('p ::text').extract_first()
                    if available_date:
                        available_date = remove_unicode_char(((((available_date).replace('\n','')).replace('\t', '')).replace('\r', '')))
                        if available_date.lower() == 'now':
                            available_date = 'available now'
                        else:
                            available_date = available_date.split(' ')
                            available_date = available_date[-1]+'-'+available_date[0]+'-'+'1'

        images = response.css('.owl-carousel img::attr(src)').extract()
        images = ['https://laberge.qc.ca' + i for i in images]
        images = images + response.meta['images']
        images = list(dict.fromkeys(images))
        amenities = response.css('.caracteristiques-appart ul li::text').extract()
        amenities = remove_unicode_char((((' '.join(amenities).replace('\n','')).replace('\t','')).replace('\r','')))
        amenities = remove_white_spaces(amenities)
        amenities = amenities +" "+ response.meta['amenities']
        floor_plan_images = response.css('.img-fluid::attr(src)').extract()
        floor_plan_images = ['https://laberge.qc.ca' + i for i in floor_plan_images]
        floor_plan_images = list(dict.fromkeys(floor_plan_images))

        pets_allowed = None
        if any(word in description.lower() for word in self.keywords['pets_allowed']) or any(word in amenities.lower() for word in self.keywords['pets_allowed']):
            pets_allowed = True

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

        rent_text = response.css('.description_appart-detailed_price-container p')
        counter = 1
        for j in rent_text:
            rent_header = j.css('::text').extract()
            rent_header = ' '.join(rent_header)
            if 'unfurnish' in rent_header:
                furnished = False
            elif 'furnish' in rent_header:
                furnished = True

            rent = j.css('span::text').extract_first()
            if rent:
                rent = rent.replace(' ','')
                rent = int(extract_number_only(extract_number_only(rent)))
            else:
                rent = None
        # # MetaData
            if rent:
                item_loader = ListingLoader(response=response)
                item_loader.add_value("external_link", response.url+'#'+str(counter))  # String
                item_loader.add_value("external_source", self.external_source)  # String

                # item_loader.add_value("external_id", external_id) # String
                item_loader.add_value("position", self.position)  # Int
                item_loader.add_value("title", title) # String
                item_loader.add_value("description", description) # String

                # # Property Details
                item_loader.add_value("city", response.meta['city']) # String
                item_loader.add_value("zipcode", response.meta['zipcode']) # String
                item_loader.add_value("address", response.meta['address']) # String
                item_loader.add_value("latitude", response.meta['latitude']) # String
                item_loader.add_value("longitude", response.meta['longitude']) # String
                item_loader.add_value("floor", floor) # String
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

            #     # # Images
                item_loader.add_value("images", images) # Array
                item_loader.add_value("external_images_count", len(images)) # Int
                item_loader.add_value("floor_plan_images", floor_plan_images) # Array
            #
            #     # # Monetary Status
                item_loader.add_value("rent", rent) # Int
            #     # item_loader.add_value("deposit", deposit) # Int
            #     # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
            #     # item_loader.add_value("utilities", utilities) # Int
                item_loader.add_value("currency", "CAD") # String
            #
            #     # item_loader.add_value("water_cost", water_cost) # Int
            #     # item_loader.add_value("heating_cost", heating_cost) # Int
            #
            #     # item_loader.add_value("energy_label", energy_label) # String
            #
            #     # # LandLord Details
                item_loader.add_value("landlord_name", 'laberge') # String
                item_loader.add_value("landlord_phone", '418-353-1133') # String
                item_loader.add_value("landlord_email", 'place.annemayrand@laberge.qc.ca') # String
            #
                self.position += 1
                yield item_loader.load_item()
