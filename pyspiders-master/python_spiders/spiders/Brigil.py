# -*- coding: utf-8 -*-
# Author: Muhammad Alaa
import scrapy
from ..loaders import ListingLoader
from ..helper import get_amenities, extract_location_from_address, extract_location_from_coordinates, extract_number_only

class BrigilSpider(scrapy.Spider):
    name = "Brigil"
    start_urls = ['http://www.brigil.com']
    allowed_domains = ["brigil.com"]
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
        items = response.css('div.containerBlanc.padding_bottom_18')
        for item in items:
            item_type = item.css('div.info span::text').get()
            if item_type != 'À louer':
                continue
            title = item.css('div.name a::text').get()
            address = item.css('div.address::text').get()
            landlord_number = item.css('div.buttons a::text').get()
            url = self.start_urls[0] + item.css('div.name a::attr(href)').get()
            yield scrapy.Request(url, callback=self.populate_item, meta={'title': title, 'address': address, 'landlord_number': landlord_number})

    # 3. SCRAPING level 3
    def populate_item(self, response):

        landlord_number = response.meta['landlord_number']
        address = response.meta['address']
        title = response.meta['title']
        longitude , latitude = extract_location_from_address(address)
        zipcode, city, _ = extract_location_from_coordinates(longitude, latitude)
        
        
        
        descriptions = response.css('div.caracteristiques p::text').getall()
        description = ''
        if descriptions != []:
            for d in descriptions:
                description += d 

        amenties = response.css('div.right.community li::text').getall()
        if amenties != []:
            Amenties_text = ''
            for amenty in amenties:
                Amenties_text += amenty
        beds_counts = response.css('div.chambre div.info span::text').getall()
        beds = []
        for bed in beds_counts:
            if bed[0].isnumeric():
                beds.append(bed[0])
        
        prices = response.css('div.chambre div.info::text').getall()
        for inx, b in enumerate(prices):
            if b.strip() == '':
                continue
            prices[inx] = b.replace('&nbsp;','').encode("ascii", "ignore").strip()
            prices[inx] = prices[inx].strip()
        while '' in prices:
            prices.remove('')
        prices_new = []
        for price in prices:
            if 'partir' in str(price):
                prices_new.append(extract_number_only(price))

        if len(prices_new) == 0:
            return
        images = response.css('div.slideshow.slideshow_slogan img::attr(src)').getall()
        for inx, img in enumerate(images):
            images[inx] = self.start_urls[0] + img

        for inx in range(len(beds)):
    
            item_loader = ListingLoader(response=response)
            get_amenities(description, Amenties_text, item_loader)
            # # MetaData
            item_loader.add_value("external_link", response.url + '#' + str(inx)) # String
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
            #item_loader.add_value("floor", floor) # String
            item_loader.add_value("property_type", 'apartment') # String => ["apartment", "house", "room", "student_apartment", "studio"]
            #item_loader.add_value("square_meters", square_meters) # Int
            item_loader.add_value("room_count", beds[inx]) # Int
            #item_loader.add_value("bathroom_count", bathroom_count) # Int

            #item_loader.add_value("available_date", available_date) # String => date_format


            # # Images
            item_loader.add_value("images", images) # Array
            item_loader.add_value("external_images_count", len(images)) # Int
            #item_loader.add_value("floor_plan_images", floor_plan_images) # Array

            # # Monetary Status
            item_loader.add_value("rent", prices_new[inx]) # Int
            #item_loader.add_value("deposit", deposit) # Int
            #item_loader.add_value("prepaid_rent", prepaid_rent) # Int
            #item_loader.add_value("utilities", utilities) # Int
            item_loader.add_value("currency", "CAD") # String

            #item_loader.add_value("water_cost", water_cost) # Int
            #item_loader.add_value("heating_cost", heating_cost) # Int

            #item_loader.add_value("energy_label", energy_label) # String

            # # LandLord Details
            item_loader.add_value("landlord_name", 'Brigil') # String
            item_loader.add_value("landlord_phone", landlord_number) # String
            #item_loader.add_value("landlord_email", landlord_email) # String

            self.position += 1
            yield item_loader.load_item()
