# -*- coding: utf-8 -*-
# Author: Fill with the developer's Name
import scrapy, json
from ..loaders import ListingLoader

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

class RehabitatLws1Spider(scrapy.Spider):
    name = "rehabitat_lws1"
    start_urls = ['https://api.theliftsystem.com/v2/search?locale=en&client_id=1081&auth_token=sswpREkUtyeYjeoahA2i&city_id=1485&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=0&max_rate=1800&min_sqft=0&max_sqft=100000&show_custom_fields=true&show_promotions=true&region=&keyword=false&property_types=apartments%2C+houses&ownership_types=&exclude_ownership_types=&custom_field_key=&custom_field_values=&order=min_rate+ASC&limit=66&neighbourhood=&amenities=&promotions=&city_ids=1863%2C1098%2C33066%2C1484%2C3218%2C3326%2C1485&pet_friendly=&offset=0&count=false']
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
        data = json.loads(response.text)

        for listing in data:
            contact = {}
            link = listing['permalink']
            # Extract some extra information
            contact['id'] = listing['id']
            contact['title'] = listing['name']
            contact['address'] = listing['address']['address']
            contact['city'] = listing['address']['city']
            contact['postal_code'] = listing['address']['postal_code']
            try:
                contact['landlord_email'] = listing['contact']['email'].split(',')[0]
            except:
                contact['landlord_email'] = 'info@rehabitat.ca'
            contact['landlord_name'] = listing['contact']['name']
            contact['landlord_phone'] = listing['contact']['phone']
            contact['latitude'] = listing['geocode']['latitude']
            contact['longitude'] = listing['geocode']['longitude']
            contact['pets_allowed'] = listing['pet_friendly']
            
            extra_data[link] = contact
            yield scrapy.Request(link, dont_filter=True, callback=self.populate_item)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        suites = response.css('div.suite-row')   # Get all suites

        property_type = "apartment"

        # Get extra data from API
        extra_info = extra_data[response.url]
        title = extra_info['title']

        external_id = str(extra_info['id'])

        landlord_name = extra_info['landlord_name']
        landlord_email = extra_info['landlord_email']
        landlord_number = extra_info['landlord_phone']

        pets_allowed = extra_info['pets_allowed']

        address = extra_info['address']
        city = extra_info['city']
        zipcode = extra_info['postal_code']

        latitude = extra_info['latitude']
        longitude = extra_info['longitude']

        # Get description
        description = ''.join(response.css('div.main').xpath('./p/text()').extract())

        # Get amenities
        furnished = None # Furnished
        balcony = None # Balconies
        washing_machine = None # 'Laundry facilities'
        dishwasher = None # 'Dishwasher available'
        parking = None # 'Underground parking'

        amenities = response.css('div.main').css('div.amenities')
        for amen in amenities:
            name = amen.css('h2').xpath('./text()').extract()[0]
            if name.strip() == 'Suite amenities':
                # Map the strings using the strip function to remove spaces - then filter them to remove empty strings
                amens = list(filter(lambda x: x != '', map(str.strip, amen.css('div').xpath('./text()').extract())))
                
                if 'Pets not allowed' in amens:
                    pets_allowed = False
                if 'Furnished' in amens:
                    furnished = True
                if  'Balconies' in amens:
                    balcony = True
                if 'Laundry facilities' in amens:
                    washing_machine = True
                if 'Dishwasher available' in amens:
                    dishwasher = True
                if 'Underground parking' in amens:
                    parking = True

        for idx, suite in enumerate(suites):
            item_loader = ListingLoader(response=response)

            items = suite.xpath('./div/ul/li')
            labels = [item.xpath('./span/text()').extract()[0].strip() for item in items] # Get all the available labels

            if 'Rent' not in labels or 'Square feet' not in labels:
                # Rent or sq ft not available, can't scrap this
                continue

            # Get main gallery
            gallery = response.css('section.slickslider_container').css("a[rel='property']::attr('href')").extract()
        
            available_date = None
            images = None
            floor_plan_images = None
            for item in items:
                label = item.xpath('./span/text()').extract()[0].strip()
            
                if label == 'Availability':
                    a = item.xpath('./span[2]/a/text()').extract()[0]
                    if a.strip() != 'Available Now':
                        # Parse the date
                        date = a.strip().split()
                        available_date = f"{date[-1]}-{MONTHS[date[0]]}-{date[1].replace(',', '')}"
                elif label == 'Suite Photos':
                    # Get the images
                    images = item.xpath('./span[2]/a/@href').extract()
                elif label == 'Floorplans':
                    floor_plan_images = item.xpath('./span[2]/a/@href').extract()
                else:
                    info = item.xpath('./span[2]/text()').extract()[0].strip()
                    if label == 'Rent':
                        rent = int(info[1:].replace(',',''))
                    elif label == 'Bedrooms':
                        room_count = int(info)
                        if room_count == 0:
                            room_count = 1
                            property_type = 'studio'
                    elif label == 'Bathrooms':
                        bathroom_count = int(info)
                    elif label == 'Square feet':
                        square_meters = int(info)

            # No apartment images avaialable - use building
            if not images:
                images = gallery

            ########################################################################
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
            item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
            item_loader.add_value("square_meters", square_meters) # Int
            item_loader.add_value("room_count", room_count) # Int
            item_loader.add_value("bathroom_count", bathroom_count) # Int

            item_loader.add_value("available_date", available_date) # String => date_format

            item_loader.add_value("pets_allowed", pets_allowed) # Boolean
            item_loader.add_value("furnished", furnished) # Boolean
            item_loader.add_value("parking", parking) # Boolean
            #item_loader.add_value("elevator", elevator) # Boolean
            item_loader.add_value("balcony", balcony) # Boolean
            #item_loader.add_value("terrace", terrace) # Boolean
            #item_loader.add_value("swimming_pool", swimming_pool) # Boolean
            item_loader.add_value("washing_machine", washing_machine) # Boolean
            item_loader.add_value("dishwasher", dishwasher) # Boolean

            # # Images
            item_loader.add_value("images", images) # Array
            item_loader.add_value("external_images_count", len(images)) # Int
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
