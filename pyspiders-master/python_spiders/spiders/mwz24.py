# -*- coding: utf-8 -*-
# Author: Fill with the developer's Name
import scrapy
from ..loaders import ListingLoader
from python_spiders.helper import remove_white_spaces
import re
class Mwz24Spider(scrapy.Spider):
    name = "mwz24"
    start_urls = ['https://www.mwz24.de/']
    allowed_domains = ["mwz24.de"]
    country = 'germany'
    locale = 'de' 
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
        for property in response.css("a.details::attr(href)").getall():
            yield scrapy.Request(url=response.urljoin(property), callback=self.populate_item)


    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        title = " ".join([i.strip() for  i in response.css(".container h1 ::text").getall()])
        external_id = title.split(":")[1].strip()
        room_count = int(float(response.css(".i-room strong::text").get().replace("Zimmer","").strip()))
        square_meters = int(float(response.css(".i-sq strong::text").get().replace("qm","").strip()))
        address = response.css(".i-location strong::text").get().strip()
        available_date = response.css(".i-cal strong::text").get()
        rent = int(float(response.css(".i-eur strong::text").get().replace("€","").strip()))
        description = remove_white_spaces(response.xpath('//h2[contains(text(), "Beschreibung")]/following-sibling::p/text()').get())
        balcony, terrace,washing_machine, elevator,pets_allowed = search_in_desc(description)
        floor = response.css(".box-body dd span::text").re('[0-9]+\.*\W*[Ee]tage')
        balcony,diswasher,washing_machine,parking= fetch_amenities(response.css(".box-body dd span::text").getall())
        images = response.css(".banner div div a::attr(href)").getall()

        city = re.findall("in \w+",title)[0].replace("in ","")
        if available_date:
            available_date = "-".join(available_date.strip().split(".")[::-1])

        if floor:
            floor = floor[0].replace("Etage","").replace(".","").strip()

        
        # # MetaData
        item_loader.add_value("external_link", response.url) # String
        item_loader.add_value("external_source", self.external_source) # String

        #item_loader.add_value("external_id", external_id) # String
        item_loader.add_value("position", self.position) # Int
        item_loader.add_value("title", title) # String
        item_loader.add_value("description", description) # String
        # # Property Details
        item_loader.add_value("city", city) # String
        #item_loader.add_value("zipcode", zipcode) # String
        item_loader.add_value("address", address) # String
        #item_loader.add_value("latitude", latitude) # String
        #item_loader.add_value("longitude", longitude) # String
        item_loader.add_value("floor", floor) # String
        item_loader.add_value("property_type", 'apartment') # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters) # Int
        item_loader.add_value("room_count", room_count) # Int
        # item_loader.add_value("bathroom_count", bathroom_count) # Int

        item_loader.add_value("available_date", available_date) # String => date_format

        item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        #item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking) # Boolean
        #item_loader.add_value("elevator", elevator) # Boolean
        item_loader.add_value("balcony", balcony) # Boolean
        #item_loader.add_value("terrace", terrace) # Boolean
        #item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        item_loader.add_value("washing_machine", washing_machine) # Boolean
        #item_loader.add_value("dishwasher", dishwasher) # Boolean

        # # Images
        item_loader.add_value("images", images) # Array
        item_loader.add_value("external_images_count", len(images)) # Int
        #item_loader.add_value("floor_plan_images", floor_plan_images) # Array

        # # Monetary Status
        item_loader.add_value("rent", rent) # Int
        #item_loader.add_value("deposit", deposit) # Int
        #item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        #item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", "EUR") # String

        #item_loader.add_value("water_cost", water_cost) # Int
        #item_loader.add_value("heating_cost", heating_cost) # Int

        #item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", 'MWZ 24 Immobilien') # String
        #item_loader.add_value("landlord_phone", landlord_number) # String
        #item_loader.add_value("landlord_email", landlord_email) # String

        self.position += 1
        yield item_loader.load_item()

def search_in_desc(desc):
    balcony, terrace,washing_machine, elevator,pets_allowed = '', '', '','',''
    desc = desc.lower()

    if 'terasse' in desc:
        terrace = True

    if 'balkon' in desc:
        balcony = True
    if 'fahrstuhl' in desc:
        elevator = True
    if 'waschmaschine' in desc:
        washing_machine = True
    if 'haustiere erlaubt' in desc:
        pets_allowed = True
    return balcony, terrace,washing_machine, elevator,pets_allowed


def fetch_amenities(l):
    balcony,diswasher,washing_machine, parking = '','','',''
    for i in l:
        if 'balkon' in i.lower():
            balcony = True
        elif 'dishwasher' in i.lower():
            diswasher = True
        elif 'waschmaschine' in i.lower():
            washing_machine = True
        elif 'tierhaltung NICHT erlaubt' in i.lower():
            pets_allowed = False
        elif 'parkhaus in fußnähe' in i.lower():
            parking = True
    return balcony,diswasher,washing_machine,parking
