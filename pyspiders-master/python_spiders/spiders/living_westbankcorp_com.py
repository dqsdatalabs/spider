# -*- coding: utf-8 -*-
# Author: Fill with the developer's Name
import scrapy
from ..loaders import ListingLoader
from ..helper import *
from scrapy.http import HtmlResponse
import json
import requests


class LivingWestbankcorpComSpider(scrapy.Spider):
    name = "living_westbankcorp_com"
    start_urls = ['http://living.westbankcorp.com/']
    allowed_domains = ["living.westbankcorp.com"]
    country = 'canada'  # Fill in the Country's name
    locale = 'en'  # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'
    position = 1
    headers = {'Accept': '*/*',
               'Accept-Encoding': 'gzip, deflate, br',
               'Accept-Language': 'en-US,en;q=0.9,ar;q=0.8',
               'Connection': 'keep-alive',
               'Content-Type': 'application/json',
               'Cookie': '_gcl_au=1.1.452952085.1641995261; _ga=GA1.2.419550048.1641995261; _gid=GA1.2.1240589189.1641995261; _fbp=fb.1.1641995262128.1809968958; _gat_UA-41405859-1=1',
               'Host': 'living.westbankcorp.com',
               'Origin': 'https://living.westbankcorp.com',
               'sec-ch-ua': '" Not;A Brand";v="99", "Google Chrome";v="97", "Chromium";v="97"',
               'sec-ch-ua-mobile': '?0',
               'sec-ch-ua-platform': '"Windows"',
               'Sec-Fetch-Dest': 'empty',
               'Sec-Fetch-Mode': 'cors',
               'Sec-Fetch-Site': 'same-origin',
               'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.71 Safari/537.36'}

    # 1. SCRAPING level 1
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    # 2. SCRAPING level 2
    def parse(self, response, **kwargs):
        urls = ['https://living.westbankcorp.com' + i for i in
                response.xpath('//*[@id="__next"]/div/main/section[2]/div/div[*]/figure/a/@href').extract()]
        payloads = [{'yardiPropertyId': "vh1n2res"}, {"yardiPropertyId": "sky-res"}, {"yardiPropertyId": "pendrell"},
                    {"yardiPropertyId": "lauren"}, {"yardiPropertyId": "zephyr"}]
        for i in range(len(urls)):
            yield scrapy.Request(urls[i], meta={'payload': payloads[i]}, callback=self.parse_city)

    # 3. SCRAPING level 3
    def parse_city(self, response, **kwargs):
        urls = ['https://living.westbankcorp.com' + i for i in
                response.css('.page__property--homes .btn--light-grey::attr(href)').extract()]
        description = ' '.join(response.css('.wrap--flex p::text').extract())
        details = ' '.join(response.css('.accordion p::text').extract())
        url = 'https://living.westbankcorp.com/api/yardi/units'

        yield scrapy.FormRequest(url, method='POST', headers=self.headers, body=json.dumps(response.meta['payload']),
                                 meta={'description': description, 'urls': urls, 'details': details}, callback=self.populate_item)

    # 4. SCRAPING level 4
    def populate_item(self, response):
        images = []
        for i in response.meta['urls']:
            req = requests.get(i)
            res = HtmlResponse(url=req.url, body=req.text, encoding='utf-8')
            images.append(['https:' + i.split('?')[0] for i in res.css('img::attr(src)').extract()])

        apartments = json.loads(response.text)
        k = int(len(apartments) / len(response.meta['urls']))

        unique = []
        description = response.meta['description']

        i = 0
        t = 0
        f = 0
        for prop in apartments:
            f += 1
            item_loader = ListingLoader(response=response)
            pets_allowed = furnished = parking = elevator = balcony = terrace = swimming_pool = washing_machine \
                = dishwasher = None
            pets_allowed, furnished, parking, elevator, balcony, terrace, swimming_pool, washing_machine, \
            dishwasher = get_amenities(description, response.meta['details'], item_loader)

            i += 1
            if i > k:
                i = 0
                t += 1

            if t == len(images):
                t = len(images) - 1

            image = images[t]
            external_link = response.meta['urls'][t]
            external_id = str(prop['UnitID']['UniqueID'])
            if external_id in unique:
                continue
            else:
                unique.append(external_id)

            title = square_feet = None
            rent = 0
            bathroom_count = room_count = 1

            longitude, latitude = extract_location_from_address(
                prop['Unit']['Address']['PostalCode'] + ' ' + prop['Unit']['Address']['City'])
            zipcode, city, address = extract_location_from_coordinates(longitude, latitude)

            title = prop['Unit']['FloorplanName']
            bathroom_count = int(prop['Unit']['UnitBathrooms'])
            room_count = int(prop['Unit']['UnitBedrooms'])
            if room_count ==0:
                room_count = 1
            if bathroom_count ==0:
                bathroom_count = 1

            rent = int(prop['Unit']['MarketRent'])
            square_feet = int(prop['Unit']['MaxSquareFeet'])

            property_type = 'apartment'
            for j in ["apartment", "studio", "house"]:
                if j in title.lower():
                    property_type = j
                    break

            if 0 >= int(rent) > 40000:
                return

            # # MetaData
            item_loader.add_value("external_link", external_link + '#' + str(self.position))  # String
            item_loader.add_value("external_source", self.external_source)  # String
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
            # item_loader.add_value("floor", floor) # String
            item_loader.add_value("property_type",
                                  property_type)  # String => ["apartment", "house", "room", "student_apartment", "studio"]
            item_loader.add_value("square_meters", square_feet)  # Int
            item_loader.add_value("room_count", room_count)  # Int
            item_loader.add_value("bathroom_count", bathroom_count)  # Int
            # item_loader.add_value("available_date", available_date) # String => date_format
            item_loader.add_value("pets_allowed", pets_allowed)  # Boolean
            item_loader.add_value("furnished", furnished)  # Boolean
            item_loader.add_value("parking", parking)  # Boolean
            item_loader.add_value("elevator", elevator)  # Boolean
            item_loader.add_value("balcony", balcony)  # Boolean
            item_loader.add_value("terrace", terrace)  # Boolean
            item_loader.add_value("swimming_pool", swimming_pool)  # Boolean
            item_loader.add_value("washing_machine", washing_machine)  # Boolean
            item_loader.add_value("dishwasher", dishwasher)  # Boolean
            # # Images
            item_loader.add_value("images", image)  # Array
            item_loader.add_value("external_images_count", len(images))  # Int
            # item_loader.add_value("floor_plan_images", floor_plan_images) # Array
            # # Monetary Status
            item_loader.add_value("rent", rent)  # Int
            # item_loader.add_value("deposit", deposit) # Int
            # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
            # item_loader.add_value("utilities", utilities) # Int
            item_loader.add_value("currency", "CAD")  # String
            # item_loader.add_value("water_cost", water_cost) # Int
            # item_loader.add_value("heating_cost", heating_cost) # Int
            # item_loader.add_value("energy_label", energy_label) # String
            # # LandLord Details
            item_loader.add_value("landlord_name", 'living westbankcorp')  # String
            item_loader.add_value("landlord_phone", '604-893-1678')  # String
            item_loader.add_value("landlord_email", 'hello@westbankliving.ca')  # String
            self.position += 1
            yield item_loader.load_item()
