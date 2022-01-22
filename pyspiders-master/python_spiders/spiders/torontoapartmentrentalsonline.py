# -*- coding: utf-8 -*-
# Author: Omar Hammad
import scrapy, json, re, requests
from ..loaders import ListingLoader
from ..helper import extract_number_only, sq_feet_to_meters
from lxml import html

class TorontoapartmentrentalsonlineSpider(scrapy.Spider):
    name = "torontoapartmentrentalsonline"
    start_urls = ['https://www.torontoapartmentrentalsonline.com/feed/all_data.json']
    allowed_domains = ["www.torontoapartmentrentalsonline.com"]
    country = 'canada' # Fill in the Country's name
    locale = 'en' # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing' 

    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        headers = {
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en',
                    }
        for url in self.start_urls:
            yield scrapy.Request(url, headers=headers, meta={'download_timeout': 100000, 'download_fail_on_dataloss':False}, callback=self.populate_item)

    # 2. SCRAPING level 2
    def parse(self, response, **kwargs):
        # Ignore this level since we read the JSON data directly
        pass

    # 3. SCRAPING level 3
    def populate_item(self, response):
        # Read API's json response as dictionary
        data = json.loads(response.text)

        # Iterate through the whole dataset
        for listing in data:
            item_loader = ListingLoader(response=response)

            if listing['TransactionType'] != "For rent":
                pass

            # External id
            external_id = listing['ListingID']

            #External link
            external_link = listing['Detail_Link']

            # Title
            title = listing['ShowTitle']

            # Images
            try:
                images = []
                for image in listing['gallery']:
                    images.append(image['Image'])
            except:
                # Images not found - continue
                continue

            # Address info
            city = listing['City']
            zipcode = listing['PostalCode']
            address = listing['StreetAddressNoUnit']
            latitude = listing['latitude']
            longitude = listing['longitude']

            if not address:
                continue

            # Rent
            rent = int(float(listing['Lease']))

            # Rooms
            room_count = listing['BedroomsAboveGround']
            bathroom_count = listing['BathroomTotal']

            if not int(room_count):
                continue

            # Description
            desc = listing['post_content']
            description = re.search(r'<p>(.*?)</p>', desc).group(1)

            # Size - we need to access the actual listing to get this
            listing_response = requests.get(external_link)
            tree = html.fromstring(listing_response.content)
            square_meters = int(extract_number_only(tree.xpath('//*[@id="torontoapartmentrentalsonline"]/section/div/div/div[1]/div[3]/div/div[1]/ul/li[4]/text()')[0]))
            if square_meters <=0:
                # Square meters is missing
                continue

            # Contact info
            phone = listing['gen_phone2']
            landlord_number = re.search(r'>(.*?)</', phone).group(1)
            landlord_email = "info@torontoapartmentrentalsonline.com"
            landlord_name = "TorontoApartmentRentalsOnline"

            ########################################################################

            # # MetaData
            item_loader.add_value("external_link", external_link) # String
            item_loader.add_value("external_source", self.external_source) # String

            item_loader.add_value("external_id", external_id) # String
            item_loader.add_value("position", self.position) # Int
            item_loader.add_value("title", title) # String
            item_loader.add_value("description", description) # String

            # # Property Details
            item_loader.add_value("city", city) # String
            item_loader.add_value("zipcode", zipcode) # String
            item_loader.add_value("address", address) # String
            item_loader.add_value("latitude", latitude) # String
            item_loader.add_value("longitude", longitude) # String
            #item_loader.add_value("floor", floor) # String
            item_loader.add_value("property_type", "apartment") # String => ["apartment", "house", "room", "student_apartment", "studio"] || Website provides apartments only
            item_loader.add_value("square_meters", square_meters) # Int
            item_loader.add_value("room_count", room_count) # Int
            item_loader.add_value("bathroom_count", bathroom_count) # Int

            #item_loader.add_value("available_date", available_date) # String => date_format

            #item_loader.add_value("pets_allowed", pets_allowed) # Boolean
            #item_loader.add_value("furnished", furnished) # Boolean
            #item_loader.add_value("parking", parking) # Boolean
            #item_loader.add_value("elevator", elevator) # Boolean
            #item_loader.add_value("balcony", balcony) # Boolean
            #item_loader.add_value("terrace", terrace) # Boolean
            #item_loader.add_value("swimming_pool", swimming_pool) # Boolean
            #item_loader.add_value("washing_machine", washing_machine) # Boolean
            #item_loader.add_value("dishwasher", dishwasher) # Boolean

            # # Images
            item_loader.add_value("images", images) # Array
            item_loader.add_value("external_images_count", len(images)) # Int
            #item_loader.add_value("floor_plan_images", floor_plan_images) # Array

            # # Monetary Status
            item_loader.add_value("rent", rent) # Int
            #item_loader.add_value("deposit", deposit) # Int
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
