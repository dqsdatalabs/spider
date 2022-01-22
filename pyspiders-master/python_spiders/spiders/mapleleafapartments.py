# -*- coding: utf-8 -*-
# Author: Fill with the developer's Name
import scrapy
from ..loaders import ListingLoader
from python_spiders.helper import remove_white_spaces
import re

class MapleleafapartmentsSpider(scrapy.Spider):
    name = "mapleleafapartments"
    start_urls = ['https://mapleleafapartments.ca/search/?Baths=0&PriceMin=&PriceMax=&orderby=priceasc']
    allowed_domains = ["mapleleafapartments.ca"]
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
        for property in  response.css(".ypnh-property-listing a::attr(href)").getall():
            yield scrapy.Request(url=property, callback = self.populate_item)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)


        title = " ".join(response.css(".ypnh-details-title::text").getall()).strip()
        property_type = make_property_type(title)
        address = response.css("span[itemprop='streetAddress']::text").get()
        city = response.css("span[itemprop='addressLocality']::text").get()
        zipcode = response.css(".ypnh-details-title span::text").getall()[-1].replace("ON","").replace(",","").strip()
        landlord_phone = response.css(".details-phone ::text").get()
        available_date = response.css("p ::text").re("Availability.*")
        rent = response.css("span[itemprop='priceRange']::text").get().split("$")[1]
        square_meters = response.css("span").re("[0-9]+\W*SQFT")
        room_count = int(response.css("span").re("[0-9]+\W*Bedroom")[0].replace(" Bedroom",""))
        pets_allowed = response.css("span::text").re(".*pet.*")
        description = remove_white_spaces(re.sub("[Cc]all.*","",re.sub("\*\*\W*[0-9][0-9][0-9]-.*","",re.sub("Bloor-Yonge Residences",""," ".join(response.css("p.property-desc ::text").getall())))))
        balcony, washer, washing_machine, elevator, parking = fetch_amenities(response.css("ul.ypnh-listing-features li ::text").getall())
        images = response.css(".modal img::attr(src)").re("https.*")
        bathroom_count = response.css("span").re("[0-9]+\W*Bathroom")
        
        description = re.sub("Unit Virtual Tour:.*","",description)
        if bathroom_count:
            bathroom_count = int(bathroom_count[0].replace("Bathroom","").strip())
        if rent:
            rent = int(rent)
        if square_meters:
            square_meters = int(square_meters[0].replace(" SQFT",""))
        if pets_allowed:
            pets_allowed =  pets_allowed[0] in ['pets allowed']

        if available_date:
            available_date = available_date[0].split(":")[1].strip().split("/")[::-1]


        if property_type == "studio":
            room_count = 1

        # buildingfloorplans







        # Your scraping code goes here
        # Dont push any prints or comments in the code section
        # if you want to use an item from the item loader uncomment it
        # else leave it commented
        # Finally make sure NOT to use this format
        #    if x:
        #       item_loader.add_value("furnished", furnished)
        # Use this:
        #   balcony = None
        #   if "balcony" in description:
        #       balcony = True

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
        #item_loader.add_value("latitude", latitude) # String
        #item_loader.add_value("longitude", longitude) # String
        #item_loader.add_value("floor", floor) # String
        item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters) # Int
        item_loader.add_value("room_count", room_count) # Int
        item_loader.add_value("bathroom_count", bathroom_count) # Int

        item_loader.add_value("available_date", available_date) # String => date_format

        item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        #item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking) # Boolean
        item_loader.add_value("elevator", elevator) # Boolean
        item_loader.add_value("balcony", balcony) # Boolean
        #item_loader.add_value("terrace", terrace) # Boolean
        #item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        item_loader.add_value("washing_machine", washing_machine) # Boolean
        # item_loader.add_value("dishwasher", dishwasher) # Boolean

        # # Images
        item_loader.add_value("images", images) # Array
        #item_loader.add_value("external_images_count", len(images)) # Int
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
        item_loader.add_value("landlord_name", 'Maple Leaf') # String
        item_loader.add_value("landlord_phone", landlord_phone) # String
        #item_loader.add_value("landlord_email", landlord_email) # String

        self.position += 1
        if rent:
            yield item_loader.load_item()


def make_property_type(word):
    apartments = ['apartment', 'loft']
    houses = ['house']
    studios = ['studio']

    for studio in studios:
        if  studio in  word.lower() :
            return 'studio'

    for house in houses:
        if  house in  word.lower() :
            return 'house'

    for apart in apartments:
        if apart in word.lower():
            return'apartment'
                  
    
    return word


def fetch_amenities(l):
    balcony, Washer, washing_machine, elevator, parking = '','','', '', ''
    for i in l:
        if 'balcony' in i.lower():
            balcony = True
        elif 'dishwasher' in i.lower():
            diswasher = True
        elif 'Washer' in i or 'laundry' in i.lower():
            washing_machine = True
        elif 'Elevator' in i:
            elevator = True
        elif 'parking' in i.lower():
            parking = True

    return balcony, Washer, washing_machine, elevator, parking