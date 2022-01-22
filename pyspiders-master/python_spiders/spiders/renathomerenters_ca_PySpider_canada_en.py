# -*- coding: utf-8 -*-
# Author: Ahmed Hegab
import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import json


class renathomerenters_ca_PySpider_canadaSpider(scrapy.Spider):
    name = "renathomerenters_ca"
    start_urls = ['https://renathomerenters.ca/properties_status/for-rent/page/1/']
    allowed_domains = ["renathomerenters.ca"]
    country = 'Canada' 
    locale = 'en' 
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing' 
    page_number = 2
    position = 1



    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)



    def parse(self, response, **kwargs):
        urls = response.css('body > div.body_wrap > div > div.page_content_wrap.scheme_default > div > div.content > div > div > div > div > div.sc_properties_item_info > div.sc_properties_item_options > div.sc_properties_item_button.sc_item_button > a::attr(href)').extract()
        for i in range(len(urls)):
            address = response.css('body > div.body_wrap > div > div.page_content_wrap.scheme_default > div > div.content > div > div > div:nth-child('+str(i+1)+') > div > div.sc_properties_item_info > div.sc_properties_item_header > div.sc_properties_item_row.sc_properties_item_row_address > span > span.sc_properties_item_option_data > span > span *::text').extract()
            city = address[-1]
            property_type = response.css('body > div.body_wrap > div > div.page_content_wrap.scheme_default > div > div.content > div > div > div:nth-child('+str(i+1)+') > div > div.sc_properties_item_info > div.sc_properties_item_header > div.sc_properties_item_type > a::text').get()
            if 'House' in property_type:
                property_type = 'house'
            else:
                property_type = 'apartment'
            rent = int(response.css('body > div.body_wrap > div > div.page_content_wrap.scheme_default > div > div.content > div > div > div:nth-child('+str(i+1)+') > div > div.sc_properties_item_info > div.sc_properties_item_price > span > span.properties_price_data.properties_price1::text').get().replace(' ',''))
            room_count = None
            bathroom_count = None
            try:
                room_count = int(response.css('body > div.body_wrap > div > div.page_content_wrap.scheme_default > div > div.content > div > div > div:nth-child('+str(i+1)+') > div > div.sc_properties_item_info > div.sc_properties_item_options > div:nth-child(2) > span.sc_properties_item_option.sc_properties_item_bedrooms > span.sc_properties_item_option_data::text').get())
            except:
                pass
            try:
                bathroom_count = int(response.css('body > div.body_wrap > div > div.page_content_wrap.scheme_default > div > div.content > div > div > div:nth-child('+str(i+1)+') > div > div.sc_properties_item_info > div.sc_properties_item_options > div:nth-child(2) > span.sc_properties_item_option.sc_properties_item_bathrooms > span.sc_properties_item_option_data::text').get())
            except:
                pass
            
            yield Request(url = urls[i],
            callback=self.populate_item,
            meta={
                'room_count':room_count,
                'bathroom_count':bathroom_count,
                'rent':rent,
                'address':address,
                'city':city,
                'property_type':property_type
            })
        next_page = ("https://renathomerenters.ca/properties_status/for-rent/page/"+ str(renathomerenters_ca_PySpider_canadaSpider.page_number))
        if renathomerenters_ca_PySpider_canadaSpider.page_number <= 2:
            renathomerenters_ca_PySpider_canadaSpider.page_number += 1
            yield response.follow(next_page, callback=self.parse)



    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        room_count = response.meta.get('room_count')
        bathroom_count = response.meta.get('bathroom_count')
        rent = response.meta.get('rent')
        property_type = response.meta.get('property_type')
        city = response.meta.get('city')
        address = response.meta.get('address') 
        address_temp = ''
        for i in range(len(address)):
            address_temp = address_temp + " " + address[i]
        address = address_temp
        
        imagez = response.css('.wp-post-image').extract()
        for i in range(len(imagez)):
            imagez[i] = imagez[i].split('src="')[1].split('"')[0]
        images = []
        images.append(imagez[0])
            
        amenities = None
        try:
            amenities = response.css('#properties_page_tabs_features_content a::text').extract()
        except:
            pass
        balcony = None
        elevator = None
        parking = None
        terrace = None
        washing_machine = None
        dishwasher = None
        pets_allowed = None
        swimming_pool = None
        if amenities is not None:
            if 'Elevator' in amenities:
                elevator = True
            if 'Dishwasher' in amenities:
                dishwasher = True
            if 'Pets' in amenities:
                pets_allowed = True
            if 'In Unit Laundry' in amenities:
                washing_machine = True
            if 'Swimming Pool' in amenities:
                swimming_pool = True
            if 'Balcony' in amenities:
                balcony = True
            if 'Garage Parking' in amenities:
                parking = True
            if 'Roof Deck' in amenities:
                terrace = True
        
        description = None
        try:
            description = response.css('#properties_page_tabs_description_content > p:nth-child(1)::text').get()
        except:
            pass
        
        title = address
        zipcode = None
        zipcode = response.css('article > section.properties_page_section.properties_page_header > div.properties_page_title_wrap > div.properties_page_title_address > span > span:nth-child(4)::text').get()
        zipcode_check = address.split(' ')
        if zipcode in zipcode_check:
            zipcode = None
            
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
        #item_loader.add_value("latitude", latitude) # String
        #item_loader.add_value("longitude", longitude) # String
        #item_loader.add_value("floor", floor) # String
        item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
        #item_loader.add_value("square_meters", square_meters) # Int
        item_loader.add_value("room_count", room_count) # Int
        item_loader.add_value("bathroom_count", bathroom_count) # Int

        #item_loader.add_value("available_date", available_date) # String => date_format

        item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        #item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking) # Boolean
        item_loader.add_value("elevator", elevator) # Boolean
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
        item_loader.add_value("landlord_name", "renathomerenters") # String
        item_loader.add_value("landlord_phone", "(902) 402-8951") # String
        item_loader.add_value("landlord_email", "renat@eastlink.ca") # String

        self.position += 1
        yield item_loader.load_item()
