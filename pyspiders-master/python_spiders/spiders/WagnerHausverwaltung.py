# -*- coding: utf-8 -*-
# Author: Muhammad Ahmad Hesham
import scrapy
from ..loaders import ListingLoader
from ..helper import extract_location_from_coordinates, extract_location_from_address, extract_number_only

class WagnerhausverwaltungSpider(scrapy.Spider):
    name = "WagnerHausverwaltung"
    start_urls = ['http://www.wagner-hausverwaltung.de/DB/wohnungen.htm']
    allowed_domains = []
    country = 'germany' # Fill in the Country's name
    locale = 'de' # Fill in the Country's locale, look up the docs if unsure
    custom_settings = { 
        "CONCURRENT_REQUESTS" : 4,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": .5,
        "AUTOTHROTTLE_MAX_DELAY": 2,
        "RETRY_TIMES": 2,
        "DOWNLOAD_TIMEOUT": 10,
        "DOWNLOAD_DELAY": 1,
        "RETRY_TIMES": 10
    }
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing' 

    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        for url in self.start_urls:
            headers = {
                "Host":"www.wagner-hausverwaltung.de",
                "Connection":"keep-alive",
                "Pragma":"no-cache",
                "Cache-Control":"no-cache",
                "Upgrade-Insecure-Requests":"1",
                "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36",
                "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
                "Accept-Encoding":"gzip, deflate",
                "Accept-Language":"en-US,en;q=0.9"
            }
            yield scrapy.Request(url, callback=self.parse, headers=headers)

    # 2. SCRAPING level 2
    def parse(self, response, **kwargs):
        headers = {
            "Host":"www.wagner-hausverwaltung.de",
            "Connection":"keep-alive",
            "Pragma":"no-cache",
            "Cache-Control":"no-cache",
            "Upgrade-Insecure-Requests":"1",
            "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36",
            "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
            "Accept-Encoding":"gzip, deflate",
            "Accept-Language":"en-US,en;q=0.9"
        }
        for listing in response.css("a::attr(href)").getall():
            if 'kontakt' in listing:
                continue
            yield scrapy.Request('http://www.wagner-hausverwaltung.de/DB/' + listing , callback=self.populate_item, headers=headers)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        external_id = response.css("div.FRX1_46::text").get()
        address = response.css("div.FRX1_48::text").get() + " " + response.css("div.FRX1_47::text").get() 
        longitude, latitude = extract_location_from_address(address)
        zipcode, city, address = extract_location_from_coordinates(longitude=longitude, latitude=latitude)
        title = response.css("div.FRX1_54::text").get().strip().lower()
        description = title
        parking = balcony = elevator = terrace = washing_machine = None
        if "stellplatz" in title or "garage" in title or "parkhaus" in title or "tiefgarage" in title:
            parking = True
        if 'balkon' in title:
            balcony = True
        if 'aufzug' in title:
            elevator = True
        if 'terrasse' in title:
            terrace = True
        if 'waschmaschine' in title:
            washing_machine = True 
        
        floor = response.css("div.FRX1_49::text").get().strip()[0]
        floor = None if not floor.isnumeric() else floor
        property_type = 'apartment'
        room_count = int(float(response.css("div.FRX1_50::text").get().strip()))
        square_meters = int(float(response.css("div.FRX1_51::text").get().strip().replace(",", '.')))
        rent = get_price(response.css("div.FRX1_52::text").get())
        total_rent = get_price(response.css("div.FRX1_53::text").get().strip().replace(",", '.'))
        utilities = total_rent - rent
        deposit = response.css("div.FRX1_55::text").get()
        if deposit:
            deposit = int(float(deposit.strip().replace(",", '.'))) * rent
        images = ["http://www.wagner-hausverwaltung.de/DB" + x[1:] for x in  response.css("img::attr(src)").getall()]
        landlord_name = 'Wagner'
        landlord_number = '03372 43 32 10'
        landlord_email = 'wagner@wagner-hausverwaltung.de'
        
        
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
        item_loader.add_value("latitude", latitude) # String
        item_loader.add_value("longitude", longitude) # String
        item_loader.add_value("floor", floor) # String
        item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters) # Int
        item_loader.add_value("room_count", room_count) # Int
        #item_loader.add_value("bathroom_count", bathroom_count) # Int

        #item_loader.add_value("available_date", available_date) # String => date_format

        #item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        #item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking) # Boolean
        item_loader.add_value("elevator", elevator) # Boolean
        item_loader.add_value("balcony", balcony) # Boolean
        item_loader.add_value("terrace", terrace) # Boolean
        #item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        item_loader.add_value("washing_machine", washing_machine) # Boolean
        #item_loader.add_value("dishwasher", dishwasher) # Boolean

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

        #item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name) # String
        item_loader.add_value("landlord_phone", landlord_number) # String
        item_loader.add_value("landlord_email", landlord_email) # String

        self.position += 1
        yield item_loader.load_item()


def get_price(val):
    v = int(float(extract_number_only(val, thousand_separator=',', scale_separator='.')))
    v2 = int(float(extract_number_only(val, thousand_separator='.', scale_separator=',')))
    price = min(v, v2)
    if price < 10:
        price = max(v, v2)
    return price