import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import math
import re

class noblehomes_PySpider_canadaSpider(scrapy.Spider):
    name = 'noblehomes'
    allowed_domains = ['noblehomes.ca']
    start_urls = ['https://www.noblehomes.ca/en/listings']
    country = 'canada'
    locale = 'en-ca'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'development'


    def parse(self, response):
        urls = response.css("#rental_list > div.lists.list.row-fluid > div > div.more-buttons > a::attr(href)").extract()
        properties = response.css("div.list_item > div.text-container > h2::text").extract()
        for i in range(len(urls)):
            property_type = 'apartment' 
            try:    
                if "Apartment Suite" in properties[i] or "Condo" in properties[i] or "Apartment Unit" in properties[i] or "Suite in a Detached " in properties[i] or "Apartment" in properties[i] or "Main & Upstairs of a" in properties[i]:
                    property_type = 'apartment'
                else:
                    property_type = 'house' 
            except:
                pass
            yield Request(url=urls[i],
                        callback=self.parse_property,
                        meta={'property_type': property_type})

    def parse_property(self, response):
        item_loader = ListingLoader(response=response)
        title = response.css(".row-address p::text").get()
        external_id = None
        descriptions = response.css("#description > p::text").get().strip()
        try:
            descriptions = descriptions.replace("\n"," ")
            descriptions = descriptions.replace("\u2022","")
            descriptions = descriptions.replace("\u00a0","")
            descriptions = descriptions.replace("\u2013","")
            descriptions = descriptions.replace("\u2019","")
            descriptions = descriptions.replace("\"","")
        except:
            pass
        description =descriptions.split('Please visit this link for an online virtual presentation or schedule a viewing of the rental unit')[0]
        if "No Pet" in description:
            pet_allowed = False
        else:
            pet_allowed = True
        if "parking" in description:
            parking = True
        else:
            parking = False
        if "balcony" in description:
            balcony = True
        else:
            balcony = False
        if "pool" in description or "Pool" in description:
            swimming_pool = True
        else:
            swimming_pool = False
        city = title.split(' ')[-2]
        address = title
        available_date = response.css("tr:nth-child(1) td::text").get()
        square_meters = response.css("tr:nth-child(2) td::text").get().split(' ')[0]
        try:
            square_meters = square_meters.replace(",","")
        except:
            pass
        square_meters = round(int(square_meters)/10.764,1)
        room_count = response.css("tr:nth-child(3) td::text").get()
        try:
            room_count = int(room_count[0])
        except:
            pass
        bathroom_count = response.css("tr:nth-child(4) td::text").get()
        try:
            bathroom_count = int(bathroom_count[0])
        except:
            pass
        images = response.css("#list_detail > div.basic-info.row-container > div:nth-child(1) > div > div > a::attr(href)").extract()
        for i in range(len(images)):
            images[i] = "https://www.noblehomes.ca/" + images[i]
        external_images_count = len(images)
        rent_x = response.css(".row-price p::text").get().split('$')[-1]
        if ',' in rent_x:
            rent_y = rent_x.split(',')[0]
            rent_z = rent_x.split(',')[-1]
            rent = int(rent_y + rent_z)
        currency = "CAD"
        rent_include = response.css("#rent_includes > div > div > div::text").extract()
        if '√ Washer' in rent_include or '√ Shared Laundry' in rent_include:
            washing_machine = True
        else:
            washing_machine = False
        if '√ Dishwasher' in rent_include:
            dishwasher = True
        else:
            dishwasher = False
        landlord_name = response.css("tr:nth-child(6) td::text").get()
        landlord_email = "Rental@Noblehomes.ca"
        landlord_phone = response.css("tr:nth-child(5) td::text").get()
        property_type = response.meta.get("property_type")
        
        item_loader.add_value('external_link', response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value('title', title)
        item_loader.add_value('description', description)
        item_loader.add_value('city', city)
        item_loader.add_value('property_type', property_type)
        item_loader.add_value('square_meters', int(int(square_meters)*10.764))
        item_loader.add_value('room_count', room_count)
        item_loader.add_value('bathroom_count', bathroom_count)
        item_loader.add_value('available_date', available_date)
        item_loader.add_value('images', images)
        item_loader.add_value('external_images_count', external_images_count)
        item_loader.add_value('rent', rent)
        item_loader.add_value('currency', currency)
        item_loader.add_value('pet_allowed', pet_allowed)
        item_loader.add_value('parking', parking)
        item_loader.add_value('balcony', balcony)
        item_loader.add_value('swimming_pool', swimming_pool)
        item_loader.add_value('washing_machine', washing_machine)
        item_loader.add_value('dishwasher', dishwasher)
        item_loader.add_value('landlord_name', landlord_name)
        item_loader.add_value('landlord_email', landlord_email)
        item_loader.add_value('landlord_phone', landlord_phone)
        yield item_loader.load_item()