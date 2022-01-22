# -*- coding: utf-8 -*-
# Author: Adham Mansour
import json
import re
from datetime import datetime
from math import ceil

import dateparser
import scrapy
from scrapy import Selector

from ..helper import remove_unicode_char, extract_number_only, extract_location_from_address, \
    extract_location_from_coordinates, remove_white_spaces
from ..loaders import ListingLoader

class Getmore_immobilienDeSpider(scrapy.Spider):
    name = 'getmore_immobilien_de'
    page_number = 1
    unique_id = 'a761e7060536444997dfda8e7f27e214'
    start_urls = [
        f'https://homepagemodul.immowelt.de/list/api/list/?callback=listcallback&guid={unique_id}&area=&eType=-1&eCat=-1&geoid=-1&livingarea=&page={page_number}&price=&rentfactor=&room=&squareprice=&windowarea=&stype=0'
    ]
    country = 'germany'
    locale = 'de'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'
    duplicates = {}
    position = 1
    keywords = {
        'pets_allowed': ['Haustiere erlaubt'],
        'furnished': ['m bliert', 'ausstattung'],
        'parking': ['garage', 'Stellplatz' 'parkh user', 'Parkplatz'],
        'elevator': ['fahrstuhl', 'aufzug'],
        'balcony': ['balkon'],
        'terrace': ['terrasse'],
        'swimming_pool': [' baden', 'schwimmen', 'schwimmbad', 'pool', 'Freibad'],
        'washing_machine': ['waschen', 'w scherei', 'waschmaschine', 'waschk che'],
        'dishwasher': ['geschirrspulmaschine', 'geschirrsp ler', ],
        'floor': ['etage'],
        'bedroom': ['Schlafzimmer']
    }
    formdatas = f'callback=listcallback&guid={unique_id}&area=&eType=-1&eCat=-1&geoid=-1&livingarea=&page={page_number}&price=&rentfactor=&room=&squareprice=&windowarea=&stype=0'
    header = {
        'Host': 'homepagemodul.immowelt.de',
        'referer': 'https://1574689-fix4this.u-web4business.de/',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-mode': 'no-cors',
        'sec-fetch-dest': 'script',
        'sec-ch-ua-platform': '"Windows"',
        'sec-ch-ua': '" Not;A Brand";v="99", "Google Chrome";v="97", "Chromium";v="97"',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.71 Safari/537.36',
        'accept-language': 'en-US,en;q=0.9,ar-EG;q=0.8,ar;q=0.7'
    }

    # 1. SCRAPING level 1
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.FormRequest(url, callback=self.parse, body=self.formdatas, headers=self.header, method='POST')

    # 2. SCRAPING level 2
    def parse(self, response, **kwargs):
        rentals = re.findall('Expose\(\\\\"([\w\d-]+)', response.text)
        for rental in rentals:
            formdata = f'callback=exposecallback&guid={self.unique_id}&id={rental}&isVorschau=&isStatistic=false&_=1642086722828'
            yield scrapy.FormRequest(url=f'https://homepagemodul.immowelt.de/home/api/Expose/?callback=exposecallback&guid={self.unique_id}&id={rental}&isVorschau=&isStatistic=false&_=1642086722828',
                          callback=self.populate_item, method='GET',headers=self.header,body=formdata,meta={'id':rental})
        internal_response = Selector(text=response.text.encode('utf-8').decode("unicode-escape"), type="html")
        next_button = internal_response.css('div.hm_pagination a ::text').extract()
        next_button_A = internal_response.css('div.hm_pagination a::attr(href)').extract()
        page = {}
        for n,i in enumerate(next_button):
            page[i] = next_button_A[n]
        if page['Â»'] !='#':
            self.page_number +=1
            yield scrapy.FormRequest(f'https://homepagemodul.immowelt.de/list/api/list/?callback=listcallback&guid={self.unique_id}&area=&eType=-1&eCat=-1&geoid=-1&livingarea=&page={self.page_number}&price=&rentfactor=&room=&squareprice=&windowarea=&stype=0', callback=self.parse, body=self.formdatas, headers=self.header, method='GET')


    # 3. SCRAPING level 3
    def populate_item(self, response):
        html = response.body.decode('unicode-escape').encode('latin-1')
        internal_response = Selector(text=html, type="html")
        item_loader = ListingLoader(response=response)
        title = internal_response.css('h1::text').extract_first()
        list_items = internal_response.css('#hm_objectdata li')
        list_items_dict = {}
        for list_item in list_items:
            head_val = list_item.css('::text').extract()
            head_val = [remove_white_spaces(remove_unicode_char(i)) for i in head_val]
            head_val = [i for i in head_val if i ]
            if len(head_val) == 2:
                list_items_dict[head_val[0]] = head_val[1]
        external_id = None
        if 'Ref.-Nr.:' in list_items_dict.keys():
            external_id = list_items_dict['Ref.-Nr.:']
        description = ((((' '.join(internal_response.css('#exposeview p::text').extract()).replace('\n','')).replace('\t','')).replace('\r','')))
        address = ' '.join(internal_response.css('#hm_energy+ .hm_expose_half_width span ::text,#hm_features+ .hm_expose_half_width span ::text').extract())
        longitude, latitude = extract_location_from_address(address)
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)
        property_type = None  # response.css('::text').extract_first()
        if 'Immobilienart:' in list_items_dict.keys():
            property_type = list_items_dict['Immobilienart:']
            if 'wohnung' in property_type.lower():
                property_type = 'apartment'

        square_meters = None  # METERS #int(response.css('::text').extract_first())
        if 'Wohnfl che:' in list_items_dict.keys():
            square_meters = list_items_dict['Wohnfl che:']
            square_meters = int(ceil(float(extract_number_only(square_meters))))
        room_count = None  # int(response.css('::text').extract_first())
        if 'Zimmer:' in list_items_dict.keys():
            room_count = list_items_dict['Zimmer:']
            room_count = int(ceil(float(extract_number_only(room_count))))
        amenities = ((((' '.join(internal_response.css('.hm_features li::text').extract()).replace('\n','')).replace('\t','')).replace('\r','')))
        amenities = (remove_white_spaces(remove_unicode_char(amenities)))
        bathroom_count = None  # int(response.css('::text').extract_first())
        if 'bad' in amenities.lower():
            bathroom_count = 1
        available_date = None  # response.css('.availability .meta-data::text').extract_first()
        if 'Bezug:' in list_items_dict.keys():
            available_date = list_items_dict['Bezug:']
            available_date = extract_number_only(available_date)
            if len(str(available_date)) == 8:
                available_date=available_date[0:2]+'/'+available_date[2:4]+'/'+available_date[4:8]
                available_date = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if available_date > datetime.now():
                    available_date = available_date.strftime("%Y-%m-%d")
                else:
                    available_date = None
            else:
                available_date = None
        images_content = internal_response.css('.hm_img_thumb_list .hm_image')
        floor_plan_images = []
        images = []
        for i in images_content:
            img_title = i.css('img::attr(title)').extract_first()
            img_src = i.css('input::attr(value)').extract_first()
            if img_title == 'Grundriss':
                floor_plan_images.append(img_src)
            else:
                images.append(img_src)

        rent = None  # int(response.css('::text').extract_first())
        if 'Kaltmiete:' in list_items_dict.keys():
            rent = list_items_dict['Kaltmiete:']
            rent = int(ceil(float(extract_number_only(rent))))
        if rent:
            deposit = None
            if 'Kaution:' in list_items_dict.keys():
                deposit = list_items_dict['Kaution:']
                deposit = int(ceil(float(extract_number_only(deposit))))
                if len(str(deposit)) == 1:
                    deposit = deposit*rent
                if deposit == 0:
                    deposit = None
            prepaid_rent = None
            utilities = None
            if 'Nebenkosten:' in list_items_dict.keys():
                utilities = list_items_dict['Nebenkosten:']
                utilities = int(ceil(float(extract_number_only(utilities))))
                if utilities == 0:
                    utilities = None
            heating_cost = None
            if 'Heizkosten:' in list_items_dict.keys():
                heating_cost = list_items_dict['Heizkosten:']
                heating_cost = int(ceil(float(extract_number_only(heating_cost))))
                if heating_cost == 0:
                    heating_cost = None

            floor = None  # response.css('::text').extract_first()
            if 'Geschoss:' in list_items_dict.keys():
                floor = list_items_dict['Geschoss:']
            pets_allowed = None
            if any(word in remove_unicode_char(description.lower()) for word in self.keywords['pets_allowed']):
                pets_allowed = True

            furnished = None
            if any(word in remove_unicode_char(description.lower()) for word in self.keywords['furnished']):
                furnished = True

            parking = None
            if any(word in remove_unicode_char(description.lower()) for word in self.keywords['parking']):
                parking = True

            elevator = None
            if any(word in remove_unicode_char(description.lower()) for word in self.keywords['elevator']):
                elevator = True

            balcony = None
            if any(word in remove_unicode_char(description.lower()) for word in self.keywords['balcony']):
                balcony = True

            terrace = None
            if any(word in remove_unicode_char(description.lower()) for word in self.keywords['terrace']):
                terrace = True

            swimming_pool = None
            if any(word in remove_unicode_char(description.lower()) for word in self.keywords['swimming_pool']):
                swimming_pool = True

            washing_machine = None
            if any(word in remove_unicode_char(description.lower()) for word in self.keywords['washing_machine']):
                washing_machine = True

            dishwasher = None
            if any(word in remove_unicode_char(description.lower()) for word in self.keywords['dishwasher']):
                dishwasher = True


            # # MetaData
            item_loader.add_value("external_link", f'https://getmore-immobilien.de/Angebote/Miete#'+str(self.position))  # String
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
            item_loader.add_value("floor", floor) # String
            item_loader.add_value("property_type", property_type) # String
            item_loader.add_value("square_meters", square_meters) # Int
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
            item_loader.add_value("floor_plan_images", floor_plan_images) # Array

            # # Monetary Status
            item_loader.add_value("rent", rent) # Int
            item_loader.add_value("deposit", deposit) # Int
            # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
            item_loader.add_value("utilities", utilities) # Int
            item_loader.add_value("currency", "EUR") # String

            # item_loader.add_value("water_cost", water_cost) # Int
            item_loader.add_value("heating_cost", heating_cost) # Int

            # item_loader.add_value("energy_label", energy_label) # String

            # # LandLord Details
            item_loader.add_value("landlord_name", 'getmore immobilien GmbH') # String
            item_loader.add_value("landlord_phone", '0531/6183600') # String
            item_loader.add_value("landlord_email", 'info@getmore-immobilien.de') # String

            self.position += 1
            yield item_loader.load_item()
