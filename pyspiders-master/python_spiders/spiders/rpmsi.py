# -*- coding: utf-8 -*-
# Author: Fill with the developer's Name
import scrapy
from ..loaders import ListingLoader
import dateutil.parser
from python_spiders.helper import remove_white_spaces

class RpmsiSpider(scrapy.Spider):
    name = "rpmsi"
    start_urls = ['http://www.rpmsi.ca/']
    allowed_domains = ["rpmsi.ca"]
    country = 'canada' # Fill in the Country's name
    locale = 'ca' # Fill in the Country's locale, look up the docs if unsure
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
        for url in  response.css(".elementor-widget-container .our-properties-link::attr(href)").re("/property.*"):
            yield scrapy.Request(url=response.urljoin(url), callback=self.populate_item)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        title = response.css(".header-left h1::text").get()
        address = response.xpath("//div[contains(@class, 'header-left')]/p/text()").get().split(",")
        city = address[1]
        zipcode = address[-1]
        address = address[0]
        landlord_email = response.css(".info a::text").getall()[0]
        landlord_phone =  response.css(".info a::text").getall()[1]
        images = [response.urljoin(i) for i in response.css(".slide-image img::attr(src)").getall()]
        longitude =  response.css("#detailMap::attr(data-longitude)").get()
        latitude =  response.css("#detailMap::attr(data-latitude)").get()
        description = remove_white_spaces(" ".join(response.css(".property-details-container p ::text").getall()))
        # fetch_amenities(response.css(".building-features-list ul li::text").getall())
        balcony,dishwasher,washing_machine, parking, elevator, pets_allowed = fetch_amenities(response.css(".icon-list li span::attr(title)").getall())
        i = 1
        for property in response.css("table tr"):
            room_count = property.css("strong::text").get()
            if room_count == "Bachelor":
                room_count = 1
                property_type = 'studio'
            else:
                property_type = 'apartment'
                room_count = int(room_count.replace("Bedroom", "").strip())
            rent = int(property.css("span.price::text").get().replace("$","").replace(",",""))

            available_date = property.css(".desktop-only::text").getall()[1]


            if available_date != "immediate":
                available_date = dateutil.parser.parse(available_date).strftime("%Y-%m-%d")
            else:
                available_date = ""
            item_loader = ListingLoader(response=response)
            item_loader.add_value("external_link", response.url+"#"+str(i)) # String
            item_loader.add_value("external_source", self.external_source) # String

            #item_loader.add_value("external_id", external_id) # String
            item_loader.add_value("position", self.position) # Int
            item_loader.add_value("title", title) # String
            item_loader.add_value("description", description) # String

            # # Property Details
            item_loader.add_value("city", city.strip()) # String
            item_loader.add_value("zipcode", zipcode.strip()) # String
            item_loader.add_value("address", address) # String
            item_loader.add_value("latitude", latitude) # String
            item_loader.add_value("longitude", longitude) # String
            #item_loader.add_value("floor", floor) # String
            item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
            #item_loader.add_value("square_meters", square_meters) # Int
            item_loader.add_value("room_count", room_count) # Int
            #item_loader.add_value("bathroom_count", bathroom_count) # Int
            item_loader.add_value("available_date", available_date) # String => date_format
            item_loader.add_value("pets_allowed", pets_allowed) # Boolean
            #item_loader.add_value("furnished", furnished) # Boolean
            item_loader.add_value("parking", parking) # Boolean
            item_loader.add_value("elevator", elevator) # Boolean
            item_loader.add_value("balcony", balcony) # Boolean
            #item_loader.add_value("terrace", terrace) # Boolean
            #item_loader.add_value("swimming_pool", swimming_pool) # Boolean
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
            item_loader.add_value("landlord_name", title) # String
            item_loader.add_value("landlord_phone", landlord_phone) # String
            item_loader.add_value("landlord_email", landlord_email) # String

            i+=1
            self.position += 1
            yield item_loader.load_item()



def fetch_amenities(l):
    balcony,diswasher,washing_machine, parking, elevator, pets_allowed = '','','','','',''


    for i in l:
        if 'balcon' in i.lower():
            balcony = True
        elif 'dishwasher' in i.lower():
            diswasher = True
        elif 'washer' in i.lower() or 'laundry' in i.lower():
            washing_machine = True
        elif 'pets allowed' in i.lower() or 'dogs' in i.lower() or 'cats' in i.lower():
            pets_allowed = True
        elif 'parking' in i.lower():
            parking = True
        elif 'elevator' in i.lower():
            elevator = True
    return balcony,diswasher,washing_machine, parking, elevator, pets_allowed
