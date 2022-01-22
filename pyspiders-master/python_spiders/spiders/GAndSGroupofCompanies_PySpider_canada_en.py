# -*- coding: utf-8 -*-
# Author: Muhammad Ahmad Hesham
import scrapy
from ..loaders import ListingLoader
from ..helper import extract_location_from_address, extract_location_from_coordinates, extract_number_only
import re
from scrapy.http import HtmlResponse
from scrapy.selector import Selector


class GandsgroupofcompaniesPyspiderCanadaEnSpider(scrapy.Spider):
    name = "GAndSGroupofCompanies_PySpider_canada_en"
    start_urls = ['https://gsrentals.ca/greater-toronto/', 'https://gsrentals.ca/ottawa/']
    allowed_domains = []
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
        yield scrapy.Request(response.css("div.property-listings > script::attr(src)").get(), callback=self.listings_page, meta={'url': response.url})
    
    def listings_page(self, response, **kwargs):
        url = response.meta['url']
        for listing in response.css("div > a::attr(href)").getall():
            listing = listing.replace('\\"', '')
            yield scrapy.Request(url + listing, callback=self.populate_item)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        counter = 0
        for i in range(len(response.css("section.floorplan-bedroom-set").getall())):
            x1 = response.css("section")[i].css("div.floorplan-list-item").getall()
            if not i == len(response.css("section.floorplan-bedroom-set").getall()) - 1:
                x2 = response.css("section")[i+1].css("div.floorplan-list-item").getall()
            else:
                x2 = []
            section = Selector(text=''.join(list(set(x1) - set(x2))))
            room_count = response.css("section")[0].css("h2.aligncenter-text::text").getall()[i].split(' ')[0]
            if 'two' in room_count.lower():
                room_count = 2
            elif 'three' in room_count.lower():
                room_count = 3
            elif 'four' in room_count.lower():
                room_count = 4
            elif 'five' in room_count.lower():
                room_count = 1
            else:
                room_count = 1
            property_type = 'apartment' if room_count > 1 else 'studio' 
            for index, unit in enumerate(section.css("div.floorplan-list-item").getall()):
                floor_plan_images = section.css("div.floorplan-list-item")[index].css("img::attr(src)").getall()
                if floor_plan_images[0].strip() == "":
                    floor_plan_images = None
                rent = section.css("div.floorplan-list-item")[index].css("span.floorplan-rent::text").get()
                rent = int(float(extract_number_only(rent, thousand_separator=',', scale_separator='.')))
                counter += 1
                if rent == 0:
                    continue
                meta = {
                    'index': index,
                    'i': i,
                    'url': response.url,
                    'property_type': property_type,
                    'room_count': room_count,
                    'floor_plan_images': floor_plan_images,
                    'rent': rent,
                    'counter': counter
                }
                yield scrapy.Request(response.css("article >script::attr(src)").get() + f'#{counter}' , callback=self.populate_rest, meta=meta, dont_filter=True)
        
    def populate_rest(self, response):
        meta = response.meta
        clean = "<table>" + re.findall('<table>(.*)</table>', response.body.decode('utf-8').replace('\\', ''))[0] + "</table>"
        response = HtmlResponse(url=response.url, body=clean, encoding='utf-8')
        address = ' '.join(response.css("div.property-address > p::text").getall())
        longitude, latitude = extract_location_from_address(address)
        zipcode, city, address = extract_location_from_coordinates(longitude=longitude, latitude=latitude)
        description = response.css("section> div > p::text").get()
        title = response.css("h1.entry-title::text").get()
        landlord = response.css("div.property-address > p > a::attr(href)").getall()
        for item in landlord:
            if 'tel' in item.lower():
                landlord_number = item.split(':')[1]
            elif 'mailto' in item.lower():
                landlord_email = item.split(':')[1]
        amenities = response.css("section.features-area::attr(data-conditional)").get().split('|')
        parking = balcony = dishwasher = pets_allowed = elevator = swimming_pool = washing_machine = None
        for amenity in amenities:
            if 'balcon' in amenity.lower():
                balcony = True
            if 'dishwasher' in amenity.lower():
                dishwasher = True
            if 'parking' in amenity.lower():
                parking = True
            if 'pet' in amenity.lower():
                pets_allowed = False if 'no' in amenity.lower() else True
            if 'elevator' in amenity.lower():
                elevator = True
            if 'pool' in amenity.lower():
                swimming_pool = True
            if 'laundry' in amenity.lower():
                washing_machine = True
        
        images = re.findall('"(.*)"', response.css("script::text").get())[0].split('|')
        
        item_loader = ListingLoader(response=response)

        # # MetaData
        item_loader.add_value("external_link",  meta['url'] + f'#{meta["counter"]}') # String
        item_loader.add_value("external_source", self.external_source) # String
        item_loader.add_value("position", self.position) # Int

        item_loader.add_value("external_id", meta['url'].split('=')[1]) # String
        item_loader.add_value("title", title) # String
        item_loader.add_value("description", description) # String
        

        # # Property Details
        item_loader.add_value("city", city) # String
        item_loader.add_value("zipcode", zipcode) # String
        item_loader.add_value("address", address) # String
        item_loader.add_value("latitude", str(latitude)) # String
        item_loader.add_value("longitude", str(longitude)) # String
        #item_loader.add_value("floor", floor) # String
        item_loader.add_value("property_type", meta['property_type']) # String => ["apartment", "house", "room", "student_apartment", "studio"]
        #item_loader.add_value("square_meters", square_meters) # Int
        item_loader.add_value("room_count", meta['room_count']) # Int
        #item_loader.add_value("bathroom_count", bathroom_count) # Int

        #item_loader.add_value("available_date", available_date) # String => date_format


        item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        #item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking) # Boolean
        item_loader.add_value("elevator", elevator) # Boolean
        item_loader.add_value("balcony", balcony) # Boolean
        #item_loader.add_value("terrace", terrace) # Boolean
        item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        item_loader.add_value("washing_machine", washing_machine) # Boolean
        item_loader.add_value("dishwasher", dishwasher) # Boolean

        # # Images
        item_loader.add_value("images", images) # Array
        item_loader.add_value("external_images_count", len(images)) # Int
        item_loader.add_value("floor_plan_images", meta['floor_plan_images']) # Array

        # # Monetary Status
        item_loader.add_value("rent", meta['rent']) # Int
        #item_loader.add_value("deposit", deposit) # Int
        #item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        #item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", "CAD") # String

        #item_loader.add_value("water_cost", water_cost) # Int
        #item_loader.add_value("heating_cost", heating_cost) # Int

        #item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", "G&S Group of Companies") # String
        item_loader.add_value("landlord_phone", landlord_number) # String
        item_loader.add_value("landlord_email", landlord_email) # String

        self.position += 1
        
        
        yield item_loader.load_item()
        
