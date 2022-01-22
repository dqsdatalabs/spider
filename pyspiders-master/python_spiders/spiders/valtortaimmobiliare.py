# -*- coding: utf-8 -*-
# Author: Fill with the developer's Name
import scrapy, re
from ..loaders import ListingLoader
from ..helper import extract_location_from_address, extract_location_from_coordinates, extract_number_only


class ValtortaimmobiliareSpider(scrapy.Spider):
    name = "valtortaimmobiliare"
    start_urls = ['https://www.valtortaimmobiliare.it/risultati-ricerca/?filter_search_type%5B%5D=residenziale&adv6_search_tab=residenziale&term_id=248&venditaaffitto=Affitto&advanced_city=&metri-quadri=&prezzo=&filter_search_action%5B%5D=&advanced_area=&locali=&submit=Property+Search#searchResults']
    allowed_domains = ["it"]
    country = 'italy' # Fill in the Country's name
    locale = 'it' # Fill in the Country's locale, look up the docs if unsure
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
        rentals = response.xpath('//*[@id="listing_ajax_container"]/div/div/div/div/h4/a/@href').extract()
        for url in rentals:
            yield scrapy.Request(url=url, callback=self.populate_item)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        # Extract Title and Description
        title = response.xpath('//*[@id="all_wrapper"]/div/div[3]/div/div[2]/div[3]/div/div/div/div/div/section/div[1]/div[2]/div/div/h1/text()').extract()[0]
        
        desc = response.xpath('//*[@id="all_wrapper"]/div/div[3]/div/div[2]/div[3]/div/div/div/div/div/section/div[1]/div[2]/div/div/div[5]/div').extract()
        description = re.search(r'<p>(.*?)</p>', desc[0]).group(1).replace('<br>', '\n')

        # Property Type // Villa == House?
        prop = response.xpath('//*[@id="all_wrapper"]/div/div[3]/div/div[2]/div[3]/div/div/div/div/div/section/div[1]/div[2]/div/div/div[1]/text()').extract()
        if "appartamento" in prop[0].lower():
            property_type = "apartment" 
        elif "villa" in prop[0].lower():
            property_type = "house"
        else:
            property_type = None

        # Extract rent // Monthly only
        rent = response.xpath('//*[@id="all_wrapper"]/div/div[3]/div/div[2]/div[3]/div/div/div/div/div/section/div[1]/div[2]/div/div/div[3]/span[2]/text()').extract()
        rent = int(rent[0].replace('.', ''))

        # Extract area
        area = response.xpath('//*[@id="all_wrapper"]/div/div[3]/div/div[2]/div[3]/div/div/div/div/div/section/div[2]/div/div/div/div[3]/text()').extract()[0]
        square_meters = int(area.split()[0])

        # Extract number of rooms
        room_element = response.xpath('//*[@id="all_wrapper"]/div/div[3]/div/div[2]/div[3]/div/div/div/div/div/section/div[2]/div/div/div/div[4]/text()').extract()[0]
        if room_element.lower() == 'trilocale':
            room_count = 3
        elif room_element.lower() == 'bilocale':
            room_count = 2
        else:
            room_count = int(room_element.split()[0])

        # Extract images
        images = response.xpath('//*[@id="all_wrapper"]/div/div[3]/div/div[2]/div[3]/div/div/div/div/div/section/div[1]/div[1]/div/div/div[1]/div/p/a/@href').extract()

        # Floot plan images
        floor_plan_images = response.xpath('//*[@id="all_wrapper"]/div/div[3]/div/div[2]/div[3]/div/div/div/div/div/section/div[1]/div[1]/div/div/div[2]/div/p/a/@href').extract()

        # Contact info
        landlord_name = "Valtorta Immobiliari"
        landlord_number = "039 2363" # It's static for all the ads
        landlord_email = "info@valtortaimmobiliare.it"  # This is static too

        # Optional information
        # Location - get coordinates from title
        longitude, latitude = extract_location_from_address(title)

        # Get data from coordinates
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)

        # Floors
        floor = extract_number_only(response.xpath('//*[@id="all_wrapper"]/div/div[3]/div/div[2]/div[3]/div/div/div/div/div/section/div[2]/div/div/div/div[20]/text()').extract()[0])

        # Energy Label
        energy_label = response.xpath('//*[@id="all_wrapper"]/div/div[3]/div/div[2]/div[3]/div/div/div/div/div/section/div[2]/div/div/div/div[13]/text()').extract()[0][-1]

        # Elevator
        elev = response.xpath('//*[@id="all_wrapper"]/div/div[3]/div/div[2]/div[3]/div/div/div/div/div/section/div[2]/div/div/div/div[22]/text()').extract()
        elevator = True if elev else False

        # Balcony
        balc = response.xpath('//*[@id="all_wrapper"]/div/div[3]/div/div[2]/div[3]/div/div/div/div/div/section/div[2]/div/div/div/div[8]/text()').extract()
        balcony = True if balc else False

        # Terrace
        tera = response.xpath('//*[@id="all_wrapper"]/div/div[3]/div/div[2]/div[3]/div/div/div/div/div/section/div[2]/div/div/div/div[9]/text()').extract()
        terrace = True if tera else False

        # Bathrooms
        baths = response.xpath('//*[@id="all_wrapper"]/div/div[3]/div/div[2]/div[3]/div/div/div/div/div/section/div[2]/div/div/div/div[6]/text()').extract()[0]
        bathroom_count = 1 if baths.lower() == 'un bagno' else extract_number_only(baths)

        # Convert long and lat to correct format (string)
        longitude, latitude = str(longitude), str(latitude)

        ########################################################

        # # MetaData
        item_loader.add_value("external_link", response.url) # String
        item_loader.add_value("external_source", self.external_source) # String

        #item_loader.add_value("external_id", external_id) # String
        item_loader.add_value("position", self.position) # Int
        item_loader.add_value("title", title) # String
        item_loader.add_value("description", description) # String

        # # Property Details
        item_loader.add_value("city", city) # String
        item_loader.add_value("zipcode", zipcode) # String
        item_loader.add_value("address", address) # String
        item_loader.add_value("latitude", latitude) # String
        item_loader.add_value("longitude", longitude) # String
        item_loader.add_value("floor", floor) # String
        item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters) # Int
        item_loader.add_value("room_count", room_count) # Int
        item_loader.add_value("bathroom_count", bathroom_count) # Int

        #item_loader.add_value("available_date", available_date) # String => date_format

        #item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        #item_loader.add_value("furnished", furnished) # Boolean
        #item_loader.add_value("parking", parking) # Boolean
        item_loader.add_value("elevator", elevator) # Boolean
        item_loader.add_value("balcony", balcony) # Boolean
        item_loader.add_value("terrace", terrace) # Boolean
        #item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        #item_loader.add_value("washing_machine", washing_machine) # Boolean
        #item_loader.add_value("dishwasher", dishwasher) # Boolean

        # # Images
        item_loader.add_value("images", images) # Array
        item_loader.add_value("external_images_count", len(images)) # Int
        item_loader.add_value("floor_plan_images", floor_plan_images) # Array

        # # Monetary Status
        item_loader.add_value("rent", rent) # Int
        #item_loader.add_value("deposit", deposit) # Int
        #item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        #item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", "EUR") # String

        #item_loader.add_value("water_cost", water_cost) # Int
        #item_loader.add_value("heating_cost", heating_cost) # Int

        item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name) # String
        item_loader.add_value("landlord_phone", landlord_number) # String
        item_loader.add_value("landlord_email", landlord_email) # String

        self.position += 1
        yield item_loader.load_item()
