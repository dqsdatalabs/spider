# -*- coding: utf-8 -*-
# Author: Omar Hammad
import scrapy, json
from ..loaders import ListingLoader
from ..helper import extract_number_only

MONTHS = {
    'Jan':'1',
    'Feb':'2',
    'Mar':'3',
    'Apr':'4',
    'May':'5',
    'Jun':'6',
    'Jul':'7',
    'Aug':'8',
    'Sep':'9',
    'Oct':'10',
    'Nov':'11',
    'Dec':'12'
}

extra_data = {}


class MarwestrentalsSpider(scrapy.Spider):
    name = "marwestrentals"
    start_urls = ['https://api.theliftsystem.com/v2/search?client_id=552&auth_token=sswpREkUtyeYjeoahA2i&city_id=2356&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=700&max_rate=1700&local_url_only=true&region=&keyword=false&property_types=apartments%2Chouse%2Cmulti-unit-house%2Csingle-family-home%2Csemi%2Cduplex%2Ctriplex%2Cfourplex%2Cmobile-home%2Crooms&ownership_types=&exclude_ownership_types=&housing_types=&exclude_housing_types=senior&custom_field_key=&custom_field_values=&suite_availabilities=&order=min_rate+DESC&limit=50&neighbourhood=&amenities=&promotions=&city_ids=3377%2C2356&pet_friendly=&offset=0&count=false']
    allowed_domains = ["api.theliftsystem.com"]
    country = 'canada' # Fill in the Country's name
    locale = 'en' # Fill in the Country's locale, look up the docs if unsure
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
        global extra_data

        # Read API's json response as dictionary
        data = json.loads(response.text)

        # Iterate through the dataset
        for listing in data:
            contact = {}
            link = listing['permalink'] # Get permanent link of apartment

            # Extract some extra information
            contact['id'] = listing['id']
            contact['title'] = listing['name']
            contact['address'] = listing['address']['address']
            contact['city'] = listing['address']['city']
            contact['postal_code'] = listing['address']['postal_code']
            contact['landlord_email'] = listing['contact']['email']
            contact['landlord_name'] = 'Marwest Rentals' #listing['contact']['name']
            contact['landlord_phone'] = listing['contact']['phone']
            contact['latitude'] = listing['geocode']['latitude']
            contact['longitude'] = listing['geocode']['longitude']
            contact['pets_allowed'] = listing['pet_friendly']
            
            extra_data[link] = contact
            yield scrapy.Request(url=link, dont_filter=True, callback=self.populate_item)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        # Get description
        descriptions = response.xpath('/html/body/section/section[2]/div/div/div[1]/div/p/text()')
        description = ""
        for d in descriptions:
            description += d.extract() + " "

        # Contact info
        contact = extra_data[response.url]

        # Title
        title = contact['title']

        pets_allowed = contact['pets_allowed']

        external_id = str(contact['id'])

        landlord_name = contact['landlord_name']
        landlord_email = contact['landlord_email']
        landlord_number = contact['landlord_phone']

        # Location
        address = contact['address']
        zipcode = contact['postal_code']
        city = contact['city']

        latitude = contact['latitude']
        longitude = contact['longitude']

        # Get images
        gallery = response.css('.gallery-image').xpath('./@href').extract()

        # Get amenities
        suite_amenities = response.css('div#suite-amenities').xpath('./div/div/div[2]/li/text()').extract()
        building_amenities = response.css('div#building-amenities').xpath('./div/div/div[2]/li/text()').extract()

        dishwasher = None
        washing_machine = None
        elevator = None
        balcony = None

        if 'Dishwasher available' in suite_amenities:
            dishwasher = True
        if  'Laundry facilities' in building_amenities or 'Washer in suite' in suite_amenities:
            washing_machine = True
        if 'Elevators' in building_amenities:
            elevator = True
        if 'Balconies' in suite_amenities:
            balcony = True

        # Get each suite type
        suites = response.css('.suite-display')

        for idx, suite in enumerate(suites):
            item_loader = ListingLoader(response=response)

            # Get rent
            rent = int(extract_number_only(suite.css('.rate').xpath('./p[2]/text()').extract()).replace('.', ''))

            try:
                available_date = suite.css('.available').xpath("./p[2]/text()").extract()[0]
                if available_date != 'Available Now':
                    if available_date == 'Waiting List':
                        pass
                    else:
                        # Parse the date
                        date = available_date.split('/')
                        available_date = f"{date[2]}-{date[0]}-{date[1]}"
            except IndexError:
                continue
            
            rooms = suite.css('.suite-type').xpath('./h1/text()').extract()[0].split()[0]
            if rooms == 'BACHELOR':
                room_count = 1
            else:
                room_count = int(rooms)

            bathroom_count = int(suite.css('.bath').xpath('./p[2]/text()').extract()[0])
            
            try:
                square_meters = int(suite.css('.sqft').xpath('./p[2]/text()').extract()[0])
            except:
                # square feet is missing
                continue

            try:
                floor_plan_images = suite.css('.floorplan').xpath('./a/@href').extract()
            except IndexError:
                pass

            images = suite.css('.suite-photos').xpath('./a/@href').extract()
            if not images:
                images = gallery

            ###############################################################################

            # # MetaData
            item_loader.add_value("external_link", response.url+f"#{idx}") # String
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
            item_loader.add_value("property_type", "apartment") # String => ["apartment", "house", "room", "student_apartment", "studio"]
            item_loader.add_value("square_meters", square_meters) # Int
            item_loader.add_value("room_count", room_count) # Int
            item_loader.add_value("bathroom_count", bathroom_count) # Int

            item_loader.add_value("available_date", available_date) # String => date_format

            item_loader.add_value("pets_allowed", pets_allowed) # Boolean
            #item_loader.add_value("furnished", furnished) # Boolean
            #item_loader.add_value("parking", parking) # Boolean
            item_loader.add_value("elevator", elevator) # Boolean
            item_loader.add_value("balcony", balcony) # Boolean
            #item_loader.add_value("terrace", terrace) # Boolean
            #item_loader.add_value("swimming_pool", swimming_pool) # Boolean
            item_loader.add_value("washing_machine", washing_machine) # Boolean
            item_loader.add_value("dishwasher", dishwasher) # Boolean

            # # Images
            item_loader.add_value("images", images) # Array
            #item_loader.add_value("external_images_count", len(images)) # Int
            item_loader.add_value("floor_plan_images", floor_plan_images) # Array

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
