# -*- coding: utf-8 -*-
# Author: Abanoub Moris
import scrapy
from parsel import Selector
from scrapy.http.request import Request
from ..loaders import ListingLoader
from ..helper import *
import re
import json


class MainstSpider(scrapy.Spider):

    name = "mainst"

    country = 'canada'  # Fill in the Country's name
    locale = 'en'  # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'
    allowed_domains = ['mainst.biz']
    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):

        url = f'https://www.mainst.biz/home_search_ajax'
        yield Request(url,
                      callback=self.parse,
                      dont_filter=True)

    # 2. SCRAPING level 2

    def parse(self, response):
        Cities_ids = re.search(
            r'cities="(.+)"', response.text).groups()[0].split(',')
        for id in Cities_ids:
  
            url = f'https://api.theliftsystem.com/v2/search?auth_token=sswpREkUtyeYjeoahA2i&client_id=364&client_key=mainstreet&offset=0&limit=1000&city_id={id}&order=featured%20DESC%2C%20building_name%20ASC&show_custom_fields=true&min_bed=0&max_bed=9999&min_bath=0&max_bath=10&min_rate=0&max_rate=9999&min_sqft=0&max_sqft=10000&'
            yield Request(url,dont_filter=True, callback=self.parseApartment)

    def parseApartment(self, response):

        apartments = json.loads(response.text)

        for apartment in apartments:

            external_id = apartment['id']
            external_link = apartment['permalink']
            title = apartment['website']['title']
            city = apartment['address']['city']
            postal_code = apartment['address']['postal_code']
            address = apartment['address']['address']+', '+city+', ' + \
                apartment['address']['province_code']+', '+postal_code

            latitude = apartment['geocode']['latitude']
            longitude = apartment['geocode']['longitude']

            available_date = apartment['availability_status_label']
            pets_allowed = apartment['pet_friendly']

            landlord_name = apartment['contact']['name'] if len(
                apartment['contact']['name']) > 0 else 'Mainst'
            landlord_phone = apartment['contact']['phone'] if len(
                apartment['contact']['phone']) > 0 else '403-215-6060'
            landlord_email = apartment['contact']['email'] if len(
                apartment['contact']['email']) > 0 else 'customers@mainst.biz'

            description = remove_white_spaces(
                "".join(Selector(apartment['details']['overview']).css('*::text').getall()))
            description = re.sub(r'call.+|Call.+|Email.+|email.+|contact.+|Contact.+|Apply.+', "", description)

            if len(description) < 10:
                continue

            datausage = {
                'external_id': external_id,
                'title': title,
                'latitude': latitude,
                'address': address,
                'longitude': longitude,
                'city': city,
                'postal_code': postal_code,
                'description': description,
                'available_date': available_date,
                'pets_allowed': pets_allowed,
                'landlord_name': landlord_name,
                'landlord_phone': landlord_phone,
                'landlord_email': landlord_email,
            }

            yield Request(external_link, meta=datausage, dont_filter=True,headers={
                'Accept':'*/*',
                'Accept-Encoding':'gzip, deflate, br',
                'upgrade-insecure-requests': '1',
                'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.61 Safari/537.36',
                'Connection':'keep-alive'
            },method='get', callback=self.populate_item)

    

    # 3. SCRAPING level 3
    def populate_item(self, response):

        external_id = str(response.meta['external_id'])
        title = response.meta['title']
        latitude = response.meta['latitude']
        address = response.meta['address']
        longitude = response.meta['longitude']
        city = response.meta['city']
        zipcode = response.meta['postal_code']

        available_date = response.meta['available_date']
        pets_allowed = response.meta['pets_allowed']
        landlord_name = response.meta['landlord_name']
        landlord_phone = response.meta['landlord_phone']
        landlord_email = response.meta['landlord_email']
        description = response.meta['description']

        images = response.css(
            ".gallery-container .background-image::attr('data-lazyload')").getall()
        images = [x.replace('128', '512') for x in images]

        i = 0

        for unit in response.css(".suite-row"):
            i+=1
            external_link = response.url+"#"+str(i)
            print(external_link)
            rent = '0'
            try:
                rent = re.search(
                    r'\d+', unit.css(".price .value::text").get())[0]
            except:
                rent = '0'
                continue

            
            rex = re.search(
                r'\d+', unit.css(".bedrooms .value::text").get())
            if rex:
                room_count=rex[0]
            if not rex or rex == '':
                room_count = '1'
            #bathroom_count = unit.css('.content .baths .value::text').get()
            square_meters = unit.css(
                ".sqft .value::text").get()
            if square_meters:
                if '-' in square_meters:
                    val1 = square_meters.split('-')[0]
                    if val1 == '0':
                        square_meters = re.search(
                            r'\d+', square_meters.split('-')[1])[0]
                    else:
                        val2 = square_meters.split('-')[1]
                    square_meters = str(int((int(val1)+int(val2))/2))
                else:
                    square_meters = re.search(r'\d+', square_meters)[0]

            else:
                square_meters=''

            deposit = ''
            try:
                deposit = re.search(
                    r'\d+', unit.css(".deposit .value::text").get())[0]
            except:
                deposit = '0'
                continue

            floor_plan_images = unit.css(".image .cover::attr(style)").getall()
            floor_plan_images = [x.replace('background-image:url(', '').replace(
                '\'', '').replace(' ', '').replace(')', '') for x in floor_plan_images]

            property_type = 'apartment'

            if int(rent) > 0 and int(rent) < 20000:
                item_loader = ListingLoader(response=response)

                # # MetaData
                item_loader.add_value("external_link", external_link)  # String
                item_loader.add_value(
                    "external_source", self.external_source)  # String

                item_loader.add_value("external_id", external_id)  # String
                item_loader.add_value("position", self.position)  # Int
                item_loader.add_value("title", title)  # String
                item_loader.add_value("description", description)  # String

                # # Property Details
                item_loader.add_value("city", city)  # String
                item_loader.add_value("zipcode", zipcode)  # String
                item_loader.add_value("address", address)  # String
                item_loader.add_value("latitude", latitude)  # String
                item_loader.add_value("longitude", longitude)  # String
                # item_loader.add_value("floor", floor)  # String
                item_loader.add_value("property_type", property_type)  # String
                item_loader.add_value("square_meters", square_meters)  # Int
                item_loader.add_value("room_count", room_count)  # Int
                # item_loader.add_value("bathroom_count", bathroom_count)  # Int

                item_loader.add_value("available_date", available_date)

                self.get_features_from_description(
                    description+" ".join(response.css(".featured-amenities li .text::text").getall()), response, item_loader)

                # # Images
                item_loader.add_value("images", images)  # Array
                item_loader.add_value(
                    "external_images_count", len(images))  # Int
                item_loader.add_value(
                    "floor_plan_images", floor_plan_images)  # Array

                # # Monetary Status
                item_loader.add_value("rent", rent)  # Int
                item_loader.add_value("deposit", deposit)  # Int
                # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
                # item_loader.add_value("utilities", utilities)  # Int
                item_loader.add_value("currency", "CAD")  # String

                # item_loader.add_value("water_cost", water_cost) # Int
                # item_loader.add_value("heating_cost", heating_cost) # Int

                # item_loader.add_value("energy_label", energy_label)  # String

                # # LandLord Details
                item_loader.add_value(
                    "landlord_name", landlord_name)  # String
                item_loader.add_value(
                    "landlord_phone", landlord_phone)  # String
                item_loader.add_value(
                    "landlord_email", landlord_email)  # String

                self.position += 1
                yield item_loader.load_item()

    Amenties = {
        'pets_allowed': ['pet'],
        'furnished': ['furnish', 'MÖBLIERTES'.lower()],
        'parking': ['parking', 'garage', 'parcheggio'],
        'elevator': ['elevator', 'aufzug'],
        'balcony': ['balcon', 'balkon'],
        'terrace': ['terrace', 'terrazz'],
        'swimming_pool': ['pool'],
        'washing_machine': [' washer', 'laundry', 'washing_machine', 'waschmaschine'],
        'dishwasher': ['dishwasher', 'geschirrspüler']
    }

    def get_features_from_description(self, description, response, item_loader):
        description = description.lower()
        pets_allowed = True if any(
            x in description for x in self.Amenties['pets_allowed']) else False
        furnished = True if any(
            x in description for x in self.Amenties['furnished']) else False
        parking = True if any(
            x in description for x in self.Amenties['parking']) else False
        elevator = True if any(
            x in description for x in self.Amenties['elevator']) else False
        balcony = True if any(
            x in description for x in self.Amenties['balcony']) else False
        terrace = True if any(
            x in description for x in self.Amenties['terrace']) else False
        swimming_pool = True if any(
            x in description for x in self.Amenties['swimming_pool']) else False
        washing_machine = True if any(
            x in description for x in self.Amenties['washing_machine']) else False
        dishwasher = True if any(
            x in description for x in self.Amenties['dishwasher']) else False

        item_loader.add_value("pets_allowed", pets_allowed)  # Boolean
        item_loader.add_value("furnished", furnished)  # Boolean
        item_loader.add_value("parking", parking)  # Boolean
        item_loader.add_value("elevator", elevator)  # Boolean
        item_loader.add_value("balcony", balcony)  # Boolean
        item_loader.add_value("terrace", terrace)  # Boolean
        item_loader.add_value("swimming_pool", swimming_pool)  # Boolean
        item_loader.add_value("washing_machine", washing_machine)  # Boolean
        item_loader.add_value("dishwasher", dishwasher)  # Boolean
        return pets_allowed, furnished, parking, elevator, balcony, terrace, swimming_pool, washing_machine, dishwasher
