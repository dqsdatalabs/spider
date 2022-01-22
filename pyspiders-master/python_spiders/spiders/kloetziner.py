# -*- coding: utf-8 -*-
# Author: Fill with the developer's Name
import scrapy
from ..loaders import ListingLoader
from ..helper import *
import re

class RevaSpider(scrapy.Spider):
    name = "kloetzner"
    start_urls = ['https://kloetzner-immo.de/mieten/mietangebote']
    allowed_domains = ["kloetzner-immo.de"]
    country = 'germany' # Fill in the Country's name
    locale = 'de' # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing' 
    list_of_properties = []
    position = 1


    def extract_word(self,input_string):
        return(re.findall(r'[A-Za-zßö]*', input_string))

    # 1. SCRAPING level 1
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    # 2. SCRAPING level 2
    def parse(self, response, **kwargs):
        pages = response.xpath('.//a[contains(@href, "seite-")]/@href').extract()[0:2]
        
        for link in response.xpath('.//div[@class="item"]/a/@href').extract():
            link = "https://"+self.allowed_domains[0]+link
            self.list_of_properties.append(link)
        for page in pages:
            if ("https://" not in page):
                page = "https://" + "kloetzner-immo.de" +page
            yield scrapy.Request(page, callback=self.paging)
        
                 
    def paging(self, response):
        for link in response.xpath('.//div[@class="item"]/a/@href').extract() + self.list_of_properties:
            if ("https://" not in link):
                link = "https://" + self.allowed_domains[0]+link
            if ("gewerberaeume" in link or "ladengeschaeft" in link):  # neglect shops and commercial buildings
                continue
            yield scrapy.Request(link, callback=self.populate_item)
        
        
    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        landlord_name = "Dr. Klötzner Immobilien GmbH-Rudolf-Breitscheid-Strasse 1107973 Greiz"
        landlord_phone = " 0 36 61 / 6 32 20"
        landlord_email = "info@kloetzner-immo.de"
        title = response.xpath('.//header/h2/text()').extract_first()
        address = response.xpath('.//header/p/text()').extract_first().strip()
        description = response.xpath('.//div[@class="tabpanel-content"]/p/text()').extract_first()
        zip_code = None
        property_type = None
        properties = response.xpath('.//th/text()').extract()
        values = response.xpath('.//td/text()').extract()
        bathroom_count = 1
        facilities = ' '.join(response.xpath('.//div[@class="tabpanel-content"]/p/text()').extract())
        if ("2 vollwertige " in facilities):
            bathroom_count = 2 
        if ("," in response.xpath('.//header/p/text()').extract_first().strip()):
            zip_separator = response.xpath('.//header/p/text()').extract_first().strip().split(",")[1]
            zip_code = extract_number_only(zip_separator)
        elif("," not in response.xpath('.//header/p/text()').extract_first().strip()):
            zip_code = extract_number_only(response.xpath('.//header/p/text()').extract_first().strip())
        filtered_address = ' '.join(list(filter(None,self.extract_word(address))))
        longitude, latitude = extract_location_from_address(filtered_address)
        Zip, town, location = extract_location_from_coordinates(longitude, latitude)
       
        if ("raum" in response.url or "wohnung" in response.url or "blick" in response.url or "appartement"):
            property_type = "apartment"
        elif ("haus" in response.url):
            property_type = "house"
        
        zipped_data = dict(zip(properties, values))
        rent = int(re.findall(r'\d+(?:\.\d+)?', zipped_data.get("Grundmiete:"))[0].replace(".", ""))
        heating_cost =  int(re.findall(r'\d+(?:\.\d+)?', zipped_data.get("Grundmiete:"))[2].replace(".", ""))-rent
        sqm = int(extract_number_only(zipped_data.get("Wohnfläche:")))
        room_count = int(extract_number_only(zipped_data.get("Räume:")))
        balkon = zipped_data.get("Balkon:")
        images = []
        for img in response.xpath('.//section[@class="gallery"]/figure/a/@href').extract():
            images.append("https://"+self.allowed_domains[0]+img)

        if (balkon == "Ja"):
            balkon = True
        
        
        # # MetaData
        item_loader.add_value("external_link", response.url) # String
        item_loader.add_value("external_source", self.external_source) # String

        #item_loader.add_value("external_id", external_id) # String
        item_loader.add_value("position", self.position) # Int
        item_loader.add_value("title", title) # String
        item_loader.add_value("description", description) # String

        # # Property Details
        item_loader.add_value("city", town) # String
        item_loader.add_value("zipcode", zip_code) # String
        item_loader.add_value("address", filtered_address) # String
        item_loader.add_value("latitude", latitude) # String
        item_loader.add_value("longitude", longitude) # String
        #item_loader.add_value("floor", floor) # String
        item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", sqm) # Int
        item_loader.add_value("room_count", room_count) # Int
        item_loader.add_value("bathroom_count", bathroom_count) # Int

        #item_loader.add_value("available_date", available_date) # String => date_format

        #item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        #item_loader.add_value("furnished", furnished) # Boolean
        #item_loader.add_value("parking", parking) # Boolean
        #item_loader.add_value("elevator", elevator) # Boolean
        item_loader.add_value("balcony", balkon) # Boolean
        #item_loader.add_value("terrace", terrace) # Boolean
        #item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        #item_loader.add_value("washing_machine", washing_machine) # Boolean
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
        item_loader.add_value("heating_cost", heating_cost) # Int

        #item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name) # String
        item_loader.add_value("landlord_phone", landlord_phone) # String
        item_loader.add_value("landlord_email", landlord_email) # String

        self.position += 1
        yield item_loader.load_item()
