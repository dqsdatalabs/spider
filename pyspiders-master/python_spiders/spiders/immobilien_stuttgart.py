# -*- coding: utf-8 -*-
# Author: Fill with the developer's Name
import scrapy
from ..loaders import ListingLoader
from python_spiders.helper import remove_white_spaces, extract_location_from_coordinates, extract_location_from_address
import re
class ImmobilienStuttgartSpider(scrapy.Spider):
    name = "immobilien_stuttgart"
    start_urls = ['https://www.immobilien-stuttgart.com/immobilien/']
    allowed_domains = ["immobilien-stuttgart.com"]
    country = 'germany' # Fill in the Country's name
    locale = 'de' # Fill in the Country's locale, look up the docs if unsure
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
        for property  in response.css(".property"):
            if property.css(".property-status::text").get() == None:
                yield scrapy.Request(url=property.css(".property-title a::attr(href)").get(), callback=self.populate_item)


    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        falses = ['0','No','NO']

        title = response.css(".property-title::text").get()
        address = response.css(".property-subtitle::text").get()
        external_id = response.xpath('//div[contains(text(), "Objekt ID")]/following-sibling::div/text()').get()
        property_type = make_property_type(response.xpath('//div[contains(text(), "Objekttypen")]/following-sibling::div/text()').get())
        address = " ".join(response.xpath('//div[contains(text(), "Adresse")]/following-sibling::div/text()').getall()).strip()
        floor = response.xpath('//div[contains(text(), "Etage")]/following-sibling::div/text()').get()
        room_count = response.xpath('//div[contains(text(), "Zimmer")]/following-sibling::div/text()').get()
        bathroom_count = response.xpath('//div[contains(text(), "Badezimmer")]/following-sibling::div/text()').get()
        furnished = response.xpath('//div[contains(text(), "Ausstattung")]/following-sibling::div/text()').get()
        utilities = response.xpath('//div[contains(text(), "Nebenkosten")]/following-sibling::div/text()').get()
        energy_label = response.xpath('//div[contains(text(), "Energie­effizienz­klasse")]/following-sibling::div/text()').get()
        description = remove_white_spaces(" ".join(response.css('.property-description p::text').getall()))
        rent = response.xpath('//div[contains(text(), "Kaltmiete")]/following-sibling::div/text()').get()
        
        square_meters = response.xpath('//div[contains(text(), "Wohnfläche")]/following-sibling::div/text()').get()
        
        if bathroom_count:
            bathroom_count = int(float(bathroom_count.replace(",",'.')))
        if room_count:
            room_count = int(float(room_count.replace(",",'.')))
        if square_meters:
            square_meters = int(re.findall("[0-9]+",square_meters)[0])
        if rent:
            rent = int(re.findall("[0-9]+\.*[0-9]*",rent )[0].replace(".",""))
        if utilities:
            utilities = int(re.findall("[0-9]+\.*[0-9]*",utilities)[0].replace(".",""))

        deposit = response.xpath('//div[contains(text(), "Kaution")]/following-sibling::div/text()').get()
        if deposit:
            deposit = int(re.findall("[0-9]+\.*[0-9]*",deposit)[0].replace(".",""))

        if furnished:
            furnished = True


        
        balcony,terrace,washing_machine = fetch_amenities(response.css("li.list-group-item::text").getall())

        if not balcony:
            balcony = response.xpath('//div[contains(text(), "Balkone")]/following-sibling::div/text()').get() not in falses

            
        longitude, latitude = extract_location_from_address(response.css(".property-subtitle::text").get())
        zipcode, city, addr = extract_location_from_coordinates(longitude, latitude)
        landlord_name = response.css('span.p-name::text').get()
        landlord_email = response.xpath('//div[contains(text(), "E-Mail Direkt")]/following-sibling::div/a/text()').get()
        landlord_phone = response.xpath('//div[contains(text(), "Tel.")]/following-sibling::div/a/text()').get()
        images = response.css('#immomakler-galleria a::attr(href)').getall()
        if not landlord_email:
            landlord_email = 'info@immobilien-stuttgart.com'
















        # # MetaData
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
        item_loader.add_value("latitude", str(latitude)) # String
        item_loader.add_value("longitude", str(longitude)) # String
        item_loader.add_value("floor", floor) # String
        item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters) # Int
        item_loader.add_value("room_count", room_count) # Int
        item_loader.add_value("bathroom_count", bathroom_count) # Int

        #item_loader.add_value("available_date", available_date) # String => date_format

        #item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        item_loader.add_value("furnished", furnished) # Boolean
        #item_loader.add_value("parking", parking) # Boolean
        #item_loader.add_value("elevator", elevator) # Boolean
        item_loader.add_value("balcony", balcony) # Boolean
        item_loader.add_value("terrace", terrace) # Boolean
        #item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        item_loader.add_value("washing_machine", washing_machine) # Boolean
        # item_loader.add_value("dishwasher", dishwasher) # Boolean

        # # Images
        item_loader.add_value("images", images) # Array
        item_loader.add_value("external_images_count", len(images)) # Int
        #item_loader.add_value("floor_plan_images", floor_plan_images) # Array

        # # Monetary Status
        item_loader.add_value("rent", rent) # Int
        item_loader.add_value("deposit", deposit) # Int
        #item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", "EUR") # String

        #item_loader.add_value("water_cost", water_cost) # Int
        #item_loader.add_value("heating_cost", heating_cost) # Int

        item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name) # String
        item_loader.add_value("landlord_phone", landlord_phone) # String
        item_loader.add_value("landlord_email", landlord_email) # String

        self.position += 1
        if 'einzelhandelsladen' not in property_type.lower() and rent:
            yield item_loader.load_item()





def make_property_type(word):
    apartments = ['etagenwohnung' , 'wohnung']
    # houses = ['house']
    # studios = ['monolocale']

    for apart in apartments:
        if apart in word.lower():
            return'apartment'
                  
    # for house in houses:
    #     if  house in  word.lower() :
    #         return 'house'
    # for studio in studios:
    #     if  studio in  word.lower() :
    #         return 'studio'
    return word
            


def fetch_amenities(l):
    balcony,terrace,washing_machine = '','',''
    for i in l:
        if 'balkon' in i.lower():
            balcony = True
        elif 'terrasse' in i.lower():
            terrace = True
        elif 'wasch' in i.lower():
            washing_machine = True
    return balcony,terrace,washing_machine
