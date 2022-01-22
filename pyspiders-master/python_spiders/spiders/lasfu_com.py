# -*- coding: utf-8 -*-
# Author: Fill with the developer's Name
import re

import scrapy
from ..loaders import ListingLoader
from ..helper import *
import json
import string


class LasfuComSpider(scrapy.Spider):
    name = "lasfu_com"
    start_urls = [
        'https://8777.lasfu.roro3.com/lasfu-back-end/api/V2/postList?sort=updatedAt,desc&postType=origin&size=30&page=0&lat=49.278664299999996&lng=-123.12197189999999&radius=100000&packageWay=list&language=zh'
        ,
        'https://8777.lasfu.roro3.com/lasfu-back-end/api/V2/postList?sort=updatedAt,desc&postType=origin&size=30&page=1&lat=49.278664299999996&lng=-123.12197189999999&radius=100000&packageWay=list&language=zh']
    allowed_domains = ["lasfu.com"]
    country = 'canada'  # Fill in the Country's name
    locale = 'en'  # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'

    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse_pages)

    # 2. SCRAPING level 2
    def parse_pages(self, response):
        pages = json.loads(response.text)
        for page in pages['posts']:
            ident = page['id']
            page_url = f'https://8777.lasfu.roro3.com/lasfu-back-end/api/selectAPost?id={ident}&language=en'
            yield scrapy.Request(page_url, callback=self.populate_item, dont_filter=True)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        props = json.loads(response.text)
        if props['post']["price"] != 0:

            item_loader = ListingLoader(response=response)
            # Description
            description = None
            if props['post']['description'] != '':
                description = props['post']['description']
            description = re.sub(r'(\d{3}[-\.\s]??\d{3}[-\.\s]??\d{4}|\(\d{3}\)\s*\d{3}[-\.\s]??\d{4}|\d{3}[-\.\s]??\d{4})','',description)
            description = re.sub(r'[\S]+\.(net|com|org|info|edu|gov|uk|de|ca|jp|fr|au|us|ru|ch|it|nel|se|no|es|mil)[\S]*\s?', '',description)

            description = re.sub(r"[_,.*+(){}';@#?!&$/-]+\ *", " ", description)

            # Title
            title = props['post']['title']
            title = re.sub(r"[_,.*+(){};@#?!&$/-]+\ *", " ", title)

            # Room count, bathroom
            room_count = None
            bathroom_count = None

            if props['post']['numberOfBathroom'] is not None:
                bathroom_count = int(props['post']['numberOfBathroom'])

            if props['post']['numberOfBedroom'] is not None:
                room_count = int(props['post']['numberOfBedroom'])

            count_str = {'two' : 2,
                         'one': 1,
                         'three':3,
                         'four': 4,
                         'five': 5,
                         'six': 6,
                         '1': 1,
                         '2': 2,
                         '3': 3,
                         '4': 4,
                         '5': 5,
                         '6': 6,
                         'single': 1,
                         'double': 2

                         }
            desc_split = description.split()
            for i in range(len(desc_split)):
                if  desc_split[i].lower() == 'bedrooms' or desc_split[i].lower() == 'bedroom' or desc_split[i].lower() == 'rooms' or desc_split[i].lower() == 'room':
                    if desc_split[i - 1] in count_str.keys():
                        room_count = count_str[desc_split[i-1].lower()]
                if bathroom_count is None and desc_split[i].lower() == 'bathroom' or desc_split[i].lower() == 'bathrooms':
                    if desc_split[i-1] in count_str.keys():
                        bathroom_count = count_str[desc_split[i-1].lower()]

            title_split = title.split()
            for i in range(len(title_split)):
                if title_split[i].lower() == 'bedroom' or title_split[i].lower() == 'bedrooms' or title_split[i].lower() == 'rooms' or title_split[i].lower() == 'room':
                    if title_split[i - 1] in count_str.keys():
                        room_count = count_str[title_split[i - 1].lower()]
                if bathroom_count is None and title_split[i].lower() == 'bathroom' or title_split[i].lower() == 'bathrooms':
                    if title_split[i - 1] in count_str.keys():
                        bathroom_count = count_str[title_split[i - 1].lower()]

            if room_count is None:
                room_count = 1

            # Deposit
            deposit = int(props['post']['deposit'])

            # Property Type
            prop_type = ["apartment", "house", "student_apartment", "studio"]

            property_type = props['post']['propertyType']

            if property_type is None:
                if "student apartment" in description.lower():
                    property_type = prop_type[2]
                elif "house" in description.lower():
                    property_type = prop_type[1]
                elif "apartment" in description.lower():
                    property_type = prop_type[0]
                elif "studio" in description.lower() or title.lower():
                    property_type = prop_type[3]

            if property_type is None:
                if '1 room' in title.lower() or 'one room' in title.lower():
                    property_type = 'room'
                else:
                    property_type = 'apartment'

            # Latitude, longitude
            latitude = props['post']['lat']
            longitude = props['post']['lng']

            # zipcode, city, address
            zipcode, city, address = extract_location_from_coordinates(longitude, latitude)

            # Available date
            available_date = None
            if props['post']['availableDay'] != '':
                available_date = props['post']['availableDay']

            # Furnished
            furnished = props['post']['including']['includeFurniture']

            # Pets allowed
            pets_allowed = props['post']['including']['isPetAllowed']
            if pets_allowed is False and ('pet friendly' in title.lower() or 'pet friendly' in description.lower() or 'pets friendly' in title.lower() or 'pets friendly' in description.lower() or 'pets allowed' in title.lower() or 'pets allowed' in description.lower() ):
                pets_allowed = True

            # Parking
            parking = props['post']['including']['includeParking']

            # Balcony
            balcony = props['post']['including']['includeBalcony']

            if balcony is False and (re.search('balcony',description) or re.search('balcony',title) or re.search('balconies',description) or re.search('balconies',title)):
                balcony = True

            # Terrace
            terrace = None
            if balcony is False and (re.search('terrace',description) or re.search('terrace',title) or re.search('terraces', description) or re.search('terraces', title)):
                terrace = True

            # Washing_machine
            washing_machine = props['post']['including']['hasLaundry']

            # landlord info
            landlord_name = props['post']['user']['displayName']

            landlord_phone = props['post']['phone']
            if landlord_phone is None:
                landlord_phone = props['post']['user']['phone']

            landlord_email = props['post']['email']
            if landlord_email is None:
                landlord_email = props['post']['user']['email']

            emails_from_desc = re.findall(r'[\w.+-]+@[\w-]+\.[\w.-]+', description)
            if landlord_email is None and len(emails_from_desc):
                landlord_email = emails_from_desc[0]

            if landlord_name is None:
                landlord_name = 'lasfu.com'
            if landlord_email is None:
                landlord_email = 'lasfu@lasfu.com'

            # Images
            images = []
            for im in props['post']['images']:
                images.append(im['imageUrl'])

            description = re.sub(landlord_phone, '', description)
            description = re.sub(landlord_email, '', description)
            description = re.sub(landlord_name, '', description)

            # # MetaData
            item_loader.add_value("external_link", props['post']['shareUrl'])  # String
            item_loader.add_value("external_source", self.external_source)  # String

            item_loader.add_value("external_id", str(props['post']["id"]))  # String
            item_loader.add_value("position", self.position)  # Int
            item_loader.add_value("title", title)  # String
            item_loader.add_value("description", description)  # String

            # # Property Details
            item_loader.add_value("city", city)  # String
            item_loader.add_value("zipcode", zipcode)  # String
            item_loader.add_value("address", address)  # String
            item_loader.add_value("latitude", str(latitude))  # String
            item_loader.add_value("longitude", str(longitude))  # String
            # item_loader.add_value("floor", floor) # String
            item_loader.add_value("property_type",
                                  property_type)  # String => ["apartment", "house", "room", "student_apartment", "studio"]
            item_loader.add_value("square_meters", None)  # Int
            item_loader.add_value("room_count", room_count)  # Int
            item_loader.add_value("bathroom_count", bathroom_count)  # Int

            item_loader.add_value("available_date", available_date)  # String => date_format
            item_loader.add_value("pets_allowed", pets_allowed)  # Boolean
            item_loader.add_value("furnished", furnished)  # Boolean
            item_loader.add_value("parking", parking)  # Boolean
            # item_loader.add_value("elevator", elevator) # Boolean
            item_loader.add_value("balcony", balcony)  # Boolean
            item_loader.add_value("terrace", terrace)  # Boolean
            # item_loader.add_value("swimming_pool", swimming_pool)  # Boolean
            item_loader.add_value("washing_machine", washing_machine)  # Boolean
            # item_loader.add_value("dishwasher", dishwasher)  # Boolean

            # # Images
            item_loader.add_value("images", images)  # Array
            item_loader.add_value("external_images_count", len(images))  # Int
            # item_loader.add_value("floor_plan_images", floor_plan_images) # Array

            # # Monetary Status
            item_loader.add_value("rent", int(props['post']['price']))  # Int
            item_loader.add_value("deposit", deposit)  # Int
            # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
            # item_loader.add_value("utilities", utilities) # Int
            item_loader.add_value("currency", "CAD")  # String

            # item_loader.add_value("water_cost", water_cost) # Int
            # item_loader.add_value("heating_cost", heating_cost) # Int

            # item_loader.add_value("energy_label", energy_label) # String

            # # LandLord Details
            item_loader.add_value("landlord_name", landlord_name)  # String
            item_loader.add_value("landlord_phone", landlord_phone)  # String
            item_loader.add_value("landlord_email", landlord_email)  # String

            self.position += 1
            yield item_loader.load_item()
