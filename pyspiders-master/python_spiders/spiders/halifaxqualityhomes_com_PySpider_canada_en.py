# -*- coding: utf-8 -*-
# Author: Ahmed Hegab
import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import json
from ..helper import extract_location_from_coordinates


class halifaxqualityhomes_com_PySpider_canadaSpider(scrapy.Spider):
    name = "halifaxqualityhomes_com"
    start_urls = ['https://halifaxqualityhomes.com/index.php?option=com_zoo&view=category&layout=category&Itemid=153&page=1',
    'https://halifaxqualityhomes.com/index.php?option=com_zoo&view=category&layout=category&Itemid=236&page=1'
    ]
    allowed_domains = ["halifaxqualityhomes.com"]
    country = 'Canada' 
    locale = 'en'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing' 

    position = 1
    page_number = 2
    page_numbers = 2


    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)



    def parse(self, response, **kwargs):
        urls = response.css("#yoo-zoo > div.items.items-col-1 > div > div > div > div.pos-details > h1 > a::attr(href)").extract()
        for i in range(len(urls)):
            urls[i] = 'https://halifaxqualityhomes.com' + urls[i]
            rented = None
            try:
                rented = response.css("#yoo-zoo > div.items.items-col-1 > div > div:nth-child("+str(i+1)+") > div > div.pos-details > div.Active::text").get()
            except:
                pass
            if rented is not None:
                rent = response.css('#yoo-zoo > div.items.items-col-1 > div > div:nth-child('+str(i+1)+') > div > div.pos-details > h2::text').get()
                room_count = response.css('#yoo-zoo > div.items.items-col-1 > div > div:nth-child('+str(i+1)+') > div > div.pos-details > div.pos-content > div.element.element-text.first').get() 
                bathroom_count = response.css('#yoo-zoo > div.items.items-col-1 > div > div:nth-child('+str(i+1)+') > div > div.pos-details > div.pos-content > div:nth-child(2)').get()
                title = response.css('#yoo-zoo > div.items.items-col-1 > div > div:nth-child('+str(i+1)+') > div > div.pos-details > h1 > a::text').get()
                external_id = urls[i].split('item_id=')[1].split('&')[0]
                property_type = urls[i].split('Itemid=')[1]
                if '153' in property_type:
                    property_type = 'house'
                else:
                    property_type = 'apartment'
                yield Request(url = urls[i],
                callback=self.populate_item,
                meta={
                    'title':title,
                    'rent':rent,
                    'room_count':room_count,
                    'bathroom_count':bathroom_count,
                    'property_type':property_type,
                    'external_id':external_id
                })
        next_page = ("https://halifaxqualityhomes.com/index.php?option=com_zoo&view=category&layout=category&Itemid=153&page="+ str(halifaxqualityhomes_com_PySpider_canadaSpider.page_number))
        if halifaxqualityhomes_com_PySpider_canadaSpider.page_number <= 3:
            halifaxqualityhomes_com_PySpider_canadaSpider.page_number += 1
            yield response.follow(next_page, callback=self.parse)
        next_page = ("https://halifaxqualityhomes.com/index.php?option=com_zoo&view=category&layout=category&Itemid=236&page="+ str(halifaxqualityhomes_com_PySpider_canadaSpider.page_numbers))
        if halifaxqualityhomes_com_PySpider_canadaSpider.page_numbers <= 2:
            halifaxqualityhomes_com_PySpider_canadaSpider.page_numbers += 1
            yield response.follow(next_page, callback=self.parse)



    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        title = response.meta.get('title')
        external_id = response.meta.get('external_id')
        property_type = response.meta.get('property_type')
        room_count = response.meta.get('room_count')
        bathroom_count = response.meta.get('bathroom_count')
        rent = response.meta.get('rent')
        
        rent = int(rent.replace('$',''))
        room_count = room_count.split('</h3>')[1].split('<')[0]
        bathroom_count = bathroom_count.split('</h3>')[1].split('<')[0]

        if '+' in room_count:
            room_count = int(room_count.split('+')[0])+1
        elif 'plus' in room_count:
            room_count = int(room_count.split('plus')[0])+1
        elif 'Studio' in room_count:
            room_count = 1
        else:
            room_count = int(room_count)

        if 'half bath' in bathroom_count:
            bathroom_count = int(bathroom_count.split(' ')[0])+1
        elif 'One full bathroom' in bathroom_count:
            bathroom_count = 1
        else:
            bathroom_count = int(bathroom_count.split(' ')[0])        

        description = response.css('#yoo-zoo > div > ul > div.element.element-textarea.last *::text').extract()
        tempo = ''
        for i in range(len(description)):
            tempo = tempo + ' ' + description[i]
        description = tempo.strip()

        extra_info = description.lower()
        amenities = response.css('#yoo-zoo > div > div:nth-child(6) > div:nth-child(1) > div.element.element-select.last').get().lower()
        all_amenities = extra_info + amenities
        washing_machine = None
        dishwasher = None
        furnished = None
        if 'furnished' in all_amenities:
            furnished = True
        if 'washer' in all_amenities:
            washing_machine = True
        if 'dishwasher' in all_amenities:
            dishwasher = True
        square_meters = None
        try:
            square_meters = response.css('#yoo-zoo > div > div:nth-child(6) > div:nth-child(1) > div.element.element-text.first').get()
            square_meters = int(square_meters.split('Square Feet</h3>')[1].split('<')[0])
        except:
            pass
        
        images = response.css('#yoo-zoo img::attr(src)').extract()

        pets_allowed = None
        balcony = None
        terrace = None
        swimming_pool = None
        parking = None
        if 'parking' in all_amenities:
            parking = True
        if 'no pets' in all_amenities:
            pets_allowed = False
        if 'pool' in all_amenities:
            swimming_pool = True
        if 'balcony' in all_amenities:
            balcony = True
        if 'terrace' in all_amenities:
            terrace = True

        latlng = response.css('#yoo-zoo > div > div:nth-child(7) > div:nth-child(1) > a::attr(href)').get()
        latitude = latlng.split('@')[1].split(',')[0]
        longitude = latlng.split('@')[1].split(',')[1]
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)

        item_loader.add_value("external_link", response.url) # String
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

        #item_loader.add_value("available_date", available_date) # String => date_format

        item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking) # Boolean
        #item_loader.add_value("elevator", elevator) # Boolean
        item_loader.add_value("balcony", balcony) # Boolean
        item_loader.add_value("terrace", terrace) # Boolean
        item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        item_loader.add_value("washing_machine", washing_machine) # Boolean
        item_loader.add_value("dishwasher", dishwasher) # Boolean

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
        item_loader.add_value("landlord_name", "halifaxqualityhomes") # String
        item_loader.add_value("landlord_phone", "902-445-4952") # String
        item_loader.add_value("landlord_email", "info@HalifaxQualityHomes.com") # String

        self.position += 1
        yield item_loader.load_item()
