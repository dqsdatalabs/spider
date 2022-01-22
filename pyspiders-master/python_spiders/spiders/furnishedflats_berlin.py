# -*- coding: utf-8 -*-
# Author: Adham Mansour
import scrapy
from scrapy import Request

from ..helper import remove_unicode_char, extract_number_only, extract_location_from_coordinates
from ..loaders import ListingLoader


class FurnishedflatsBerlinSpider(scrapy.Spider):
    name = 'furnishedflats_berlin'
    allowed_domains = ['furnishedflats.berlin']
    start_urls = ['https://www.furnishedflats.berlin/immobilienangebote.xhtml']  # https not http
    country = 'germany'
    locale = 'de'
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
        rentals = response.css('.list-object')
        for rental in rentals:
            if not rental.css('.status-reserved'):
                square_meters = extract_number_only(rental.css('.area-details div:nth-child(1) span span::text').extract_first())
                room_count = int(rental.css('.area-details div:nth-child(2) span span::text').extract_first())
                available_date =rental.css('.verfuegbar-ab span span::text').extract_first()
                if available_date:
                    available_date = available_date.split('.')
                    available_date = available_date[-1] + '-' + available_date [1] + '-' + available_date [0]
                rent = extract_number_only(extract_number_only(rental.css('.pauschalmiete span span::text').extract_first()))
                external_link = 'https://www.furnishedflats.berlin/'+ rental.css('.image a::attr(href)').extract_first()
                yield Request(url=external_link,
                              callback=self.populate_item,
                              meta={
                                  'square_meters' : square_meters,
                                  'room_count' : room_count,
                                  'available_date' : available_date,
                                  'rent' : int(rent)
                              })

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        list_dict = {}
        list_details = response.css('.details-desktop')
        for row_detail in list_details.css('tr'):
            key1 = row_detail.css('td:nth-child(1) strong::text').extract_first()
            value1 = row_detail.css('td:nth-child(2) span::text').extract_first()
            key2 = row_detail.css('td:nth-child(3) strong::text').extract_first()
            value2 = row_detail.css('td:nth-child(4) span::text').extract_first()
            if key1:
                list_dict[key1.lower()] = value1.lower()
            if key2:
                list_dict[key2.lower()] = value2.lower()
        if 'propno' in list_dict:
            external_id = list_dict['propno']
        title = response.css('.detail h2::text').extract_first()
        if title.lower() == 'your contact person':
            title = None
        location_para= None
        equipment_para = None
        others_para = None
        for i in response.css('.information span'):
            header = i.css('strong::text').extract_first()
            if header == 'Property description':
                description =remove_unicode_char((((' '.join(i.css('span::text').extract()).replace('\n','')).replace('\t','')).replace('\r','')))
            if header == 'Location':
                location_para = remove_unicode_char((((' '.join(i.css('span::text').extract()).replace('\n', '')).replace('\t', '')).replace('\r', '')))
                location_para = location_para .lower()
            if header == 'Equipment:':
                equipment_para = remove_unicode_char(
                    (((' '.join(i.css('span::text').extract()).replace('\n', '')).replace('\t', '')).replace('\r', '')))
                equipment_para = equipment_para.lower()
            if header == 'Others:':
                others_para = remove_unicode_char((((' '.join(i.css('span::text').extract()).replace('\n', '')).replace('\t', '')).replace('\r', '')))
                others_para = others_para.lower()

        latitude = None
        if 'latitude' in list_dict:
            latitude = list_dict['latitude']

        longitude = None
        if 'longitude' in list_dict:
            longitude = list_dict['longitude']
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)

        property_type = None
        if 'property type' in list_dict:
            property_type = list_dict['property type']

        images = response.css('.fotorama div::attr(data-img)').extract()
        deposit = None
        if 'kaution' in description.lower():
            deposit = response.meta['rent']*2


        dishwasher = None
        furnished = None
        washing_machine = None
        if equipment_para:
            if 'möbliert' in equipment_para.lower():
                furnished = True
            else:
                furnished = False

            if 'waschmaschine' in equipment_para.lower():
                washing_machine = True

            if 'geschirrspüler' in equipment_para.lower():
                dishwasher = True

        floor = None
        if 'floor' in list_dict.keys():
            floor = list_dict['floor']

        parking = None
        if 'parking space hire' in list_dict.keys():
            parking = True

        elevator = None
        if 'lift' in list_dict.keys():
            if list_dict['lift'] == 'elevator':
                elevator = True
            elif list_dict['lift'] == 'no elevator':
                elevator = False

        pets_allowed = None
        balcony = None
        swimming_pool = None
        terrace = None
        if description:
            if 'pets allowed' in description.lower():
                pets_allowed = True
            elif'no pets allowed' in description.lower():
                pets_allowed = False

            if 'balkon' in description.lower():
                balcony = True

            if 'terrasse' in description.lower():
                terrace = True

            if 'schwimmbades' in description.lower():
                swimming_pool = True



        landlord_name = response.css('.center > strong::text').extract_first()
        landlord_phone = response.css('.center span span::text').extract_first()

        # # MetaData
        item_loader.add_value("external_link", response.url)  # String
        item_loader.add_value("external_source", self.external_source)  # String

        item_loader.add_value("external_id", external_id) # String
        item_loader.add_value("position", self.position)  # Int
        item_loader.add_value("title", title) # String
        item_loader.add_value("description", description) # String

        # # Property Details
        item_loader.add_value("city", city) # String
        item_loader.add_value("zipcode", zipcode) # String
        item_loader.add_value("address", address) # String
        item_loader.add_value("latitude", latitude) # String
        item_loader.add_value("longitude", longitude) # String
        item_loader.add_value("floor", floor) # String
        item_loader.add_value("property_type", property_type) # String
        item_loader.add_value("square_meters", response.meta['square_meters']) # Int
        item_loader.add_value("room_count", response.meta['room_count']) # Int
        # item_loader.add_value("bathroom_count", bathroom_count) # Int

        item_loader.add_value("available_date", response.meta['available_date']) # String => date_format also "Available", "Available Now" ARE allowed

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
        item_loader.add_value("rent", response.meta['rent']) # Int
        item_loader.add_value("deposit", deposit) # Int
        # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        # item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", "EUR") # String

        # item_loader.add_value("water_cost", water_cost) # Int
        # item_loader.add_value("heating_cost", heating_cost) # Int

        # item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name) # String
        item_loader.add_value("landlord_phone", landlord_phone) # String
        item_loader.add_value("landlord_email", 'info@furnishedflats.de') # String

        self.position += 1
        yield item_loader.load_item()
