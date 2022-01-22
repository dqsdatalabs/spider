# -*- coding: utf-8 -*-
# Author: Adham Mansour
import math
import re

import scrapy
from scrapy import Request

from ..helper import remove_unicode_char, extract_number_only, remove_white_spaces, extract_location_from_address, \
    extract_location_from_coordinates
from ..loaders import ListingLoader
from parsel import Selector

class Seeger_russwurmDeSpider(scrapy.Spider):
    name = 'seeger_russwurm_de'
    allowed_domains = ['seeger-russwurm.de']
    start_urls = ['https://www.seeger-russwurm.de/immobilienangebote/alle-angebote/?mt=rent&category&city&address&sort=sort%7Cdesc#immobilien']  # https not http
    country = 'germany'
    locale = 'de'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'
    keywords = {
        'pets_allowed' : ['Haustiere erlaubt'],
        'furnished' : ['m bliert', 'ausstattung'],
        'parking' : ['garage', 'Stellplatz' 'Parkh user'],
        'elevator' : ['fahrstuhl','aufzug'],
        'balcony' : ['balkon'],
        'terrace' : ['terrasse'],
        'swimming_pool' : ['baden', 'schwimmen','schwimmbad','pool','Freibad'],
        'washing_machine' : ['waschen','w scherei','waschmaschine'],
        'dishwasher' :['geschirrspulmaschine','geschirrsp ler']
    }
    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    # 2. SCRAPING level 2
    def parse(self, response, **kwargs):
        rentals = response.css('.immo-listing__wrapper')
        for rental in rentals:
            property_type = (rental.css('.h3::text').extract_first()).lower()
            if 'büro' not in property_type:
                external_link = rental.css('a::attr(href)').extract_first()
                yield Request(url=external_link,
                              callback=self.populate_item)
        next_page = response.css('a.next.page-numbers::attr(href)').extract_first()
        if next_page:
            yield Request(url=next_page,
                          callback=self.parse)


    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)


        external_id = remove_white_spaces((response.css('.lh-large::text').extract_first()).replace('Objekt-Nr.: ',''))
        title = response.css('h1::text').extract_first()

        javascripts = response.css("script.vue-tabs::text").extract()
        desc_html_data = Selector(text=javascripts[0], type="html")
        description = remove_unicode_char((((' '.join(desc_html_data.css('v-card-text p::text').extract()).replace('\n','')).replace('\t','')).replace('\r','')))
        address = response.css('.clearfix::text').extract_first()
        longitude, latitude = extract_location_from_address(address)
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)
        address = response.css('.clearfix::text').extract_first()


        property_type = response.css('.badge-secondary::text').extract_first()
        if 'haus' in property_type.lower():
            property_type = 'house'
        else:
            property_type ='apartment'

        info_dict = {}
        info_html_data = Selector(text=javascripts[1], type="html")
        items = info_html_data.css('li')
        for item in items:
            if item.css('::attr(class)').extract_first() == 'no' or item.css('::attr(class)').extract_first() == 'yes':
                key = item.css('::text').extract_first()
                value = item.css('::attr(class)').extract_first()
            else:
                key = item.css('.key::text').extract_first()
                value = (item.css('.value::text').extract_first())
            if key and value:
                key = remove_unicode_char((key))
                info_dict[key] =remove_unicode_char(value)
        square_meters = 0
        if 'Wohnfl' in ' '.join(info_dict.keys()):
            square_meters = int(extract_number_only(info_dict['Wohnfl che (ca.):']))
        if square_meters == 0:
            square_meters = None


        room_count = None
        if 'Zimmer:' in info_dict.keys():
            room_count = int(math.floor(float(extract_number_only(info_dict['Zimmer:']))))
        available_date = None
        if 'verf gbar ab:' in info_dict.keys():
            available_date = info_dict['verf gbar ab:']
            available_date = available_date.split('.')
            available_date.reverse()
            available_date = '-'.join(available_date)

        floor_plan_images = []
        images = []
        photos =response.css('#exGallery a')
        for i in photos:
            if i.css('::attr(title)').extract_first() == 'Grundriss':
                floor_plan_images.append(i.css('::attr(href)').extract_first())
            else:
                images.append(i.css('::attr(href)').extract_first())

        rent = 0
        if 'Kaltmiete:' in info_dict.keys() or 'Miete zzgl. NK:'  in info_dict.keys():
            if 'Kaltmiete:' in info_dict.keys():
                rent = info_dict['Kaltmiete:']
                rent = extract_number_only(extract_number_only(rent))
            elif 'Miete zzgl. NK:'  in info_dict.keys():
                rent = info_dict['Miete zzgl. NK:']
                rent = extract_number_only(extract_number_only(rent))

        if int(rent) > 150:
            deposit = re.findall('Kaution: (\d) Monatsmieten|Kaution \d MM',description)
            if deposit !=[''] and deposit !=[] and deposit is not None:
                deposit = int(rent) * int(deposit[0])

            pets_allowed = None
            if 'Haustiere erlaubt' in info_dict.keys():
                if info_dict['Haustiere erlaubt'] == 'yes':
                    pets_allowed = True
                elif info_dict['Haustiere erlaubt'] == 'no':
                    pets_allowed = False


            furnished = None
            if any(word in description.lower() for word in self.keywords['furnished']):
                furnished = True

            parking = None
            if any(word in description.lower() for word in self.keywords['parking']):
                parking = True

            elevator = None
            if any(word in description.lower() for word in self.keywords['elevator']):
                elevator = True

            balcony = None
            if any(word in description.lower() for word in self.keywords['balcony']):
                balcony = True

            terrace = None
            if any(word in description.lower() for word in self.keywords['terrace']):
                terrace = True


            swimming_pool = None
            if any(word in description.lower() for word in self.keywords['swimming_pool']):
                swimming_pool = True

            washing_machine = None
            if any(word in description.lower() for word in self.keywords['washing_machine']):
                washing_machine = True

            dishwasher = None
            if any(word in description.lower() for word in self.keywords['dishwasher']):
                dishwasher = True


            landlord_name = response.css('strong::text').extract_first()
            landlord_name = (landlord_name.split(','))[0]
            if landlord_name is None:
                landlord_name = 'seeger russwurm'
            landlord_email = response.css('br+ a::text').extract_first()
            if landlord_email is None:
                landlord_email = 'welcome@seeger-russwurm.de'
            landlord_phone = response.css('.mb-4 p:nth-child(3)::text').extract_first()
            landlord_phone = (landlord_phone.split(':'))[1]
            landlord_phone = landlord_phone.replace(' ','')
            if landlord_phone is None:
                landlord_phone = '+49(0)721/17089–0'

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
            item_loader.add_value("latitude", str(latitude)) # String
            item_loader.add_value("longitude", str(longitude)) # String
            # item_loader.add_value("floor", floor) # String
            item_loader.add_value("property_type", property_type) # String
            item_loader.add_value("square_meters", square_meters) # Int
            item_loader.add_value("room_count", room_count) # Int
            # item_loader.add_value("bathroom_count", bathroom_count) # Int

            item_loader.add_value("available_date", available_date)

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
            item_loader.add_value("floor_plan_images", floor_plan_images) # Array

            # # Monetary Status
            item_loader.add_value("rent", int(rent)) # Int
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
            item_loader.add_value("landlord_email", landlord_email) # String

            self.position += 1
            yield item_loader.load_item()
