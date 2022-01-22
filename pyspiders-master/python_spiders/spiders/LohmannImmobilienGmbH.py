# -*- coding: utf-8 -*-
# Author: Fill with the developer's Name
import scrapy
import re
from ..loaders import ListingLoader
from dateutil.parser import parse
from datetime import datetime
from ..helper import extract_number_only, format_date, extract_rent_currency, extract_location_from_address, remove_white_spaces


class LohmannimmobiliengmbhSpider(scrapy.Spider):
    name = "LohmannImmobilienGmbH"
    start_urls = ['https://lohmann-immobilien.de/angebote/1']
    allowed_domains = ["lohmann-immobilien.de"]
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
        for page in response.css("div.uk-float-right > ul.uk-pagination > li > a::text").getall():
            for listing in set(response.css("a[title='Exposé anzeigen']::attr(href)").getall()):
                yield scrapy.Request('https://lohmann-immobilien.de' + listing, callback=self.populate_item)
            current_page = int(response.request.url.split("/")[-1])
            if page.isnumeric() and int(page) > current_page:
                yield scrapy.Request('https://lohmann-immobilien.de/angebote/' + page, callback=self.parse) 
                break

    # 3. SCRAPING level 3
    def populate_item(self, response):
        property_type = response.css("span.field-objektart::text").get()
        if property_type.lower() == 'wohnung':
            property_type = 'apartment'
        elif property_type.lower() == 'haus':
            property_type = 'house'
        else:
            return
                    
        marketing = response.css("span.field-vermarktung::text").get()
        if marketing.lower().find('miete') == -1:
            return 
        
        washing_machine = balcony = terrace = elevator = external_id = floor = parking = address = None
        room_count = bathroom_count = available_date = deposit = total_rent = rent = currency = square_meters = None
        room_count = 1
        
        for row in response.css("tr"):
            key = row.css(".label-sesimmotool::text").get()
            val = row.css(".field-sesimmotool::text").get()
            if key is None:
                continue
            key = key.lower()
            if key == "objekt-nr.":
                external_id = val
            elif key == "etage":
                floor = val.strip()[0]
                floor = None if not floor.isnumeric() else floor
            elif key == "plz":
                zipcode = val
            elif key == "ort":
                city = val
            elif key == "kaltmiete":
                rent, currency = extract_rent_currency(val, self.country, LohmannimmobiliengmbhSpider)
            elif "nebenkosten" in key:
                utilities = get_price(val) if get_price(val) !=0 else utilities
            elif key == "kaution":
                deposit = get_price(val)
            elif "wohnfläche" in key:
                square_meters = int(float(extract_number_only(val, thousand_separator='.', scale_separator=',')))
            elif key == "balkon":
                balcony = True
            elif key.find("terrassen") != -1:
                terrace = True
            elif key.find("schlafzimmer") != -1:
                room_count = int(val)
            elif key == "badezimmer":
                bathroom_count = int(val)
            elif key == "verfügbar ab":
                if 'sofort' in val.lower():
                    available_date = datetime.now().strftime("%Y-%m-%d")
                elif 'vermietet' in val.lower():
                    return
                else:
                    try:
                        available_date = parse(val.split(' ')[0]).strftime("%Y-%m-%d")
                    except:
                        available_date = None  
            elif key == "stellplatzart(en)":
                parking = True
            elif key == "warmmiete":
                total_rent, _ = extract_rent_currency(val, self.country, LohmannimmobiliengmbhSpider)
            elif key == "aufzug":
                elevator = True
                
        address = response.css("div[data-tabid='#tab-map']::attr(data-address)").get()
        longitude, latitude = extract_location_from_address(address)
        title = remove_white_spaces(response.css("h1.uk-text-break::text").get())

        description = re.sub(re.compile('<.*?>') , '', response.css("div.uk-placeholder.uk-padding-small > div.uk-text-break").getall()[0])
        
        lower_description = description.lower() + ' ' +  title.lower()
        if "stellplatz" in lower_description or "garage" in lower_description or "parkhaus" in lower_description or "tiefgarage" in lower_description:
            parking = True
        if 'balkon' in lower_description:
            balcony = True
        if 'aufzug' in lower_description:
            elevator = True
        if 'terrasse' in lower_description:
            terrace = True
        if 'waschmaschine' in lower_description:
            washing_machine = True
        
        images = list(set(response.css("li img[alt!='']::attr(src)").getall()) - set(response.css("li img[alt='Grundriss']::attr(src)").getall()))
        images = ['https://lohmann-immobilien.de'+image for image in images]
        floor_plan_images = response.css("li img[alt='Grundriss']::attr(src)").getall()
        floor_plan_images = ['https://lohmann-immobilien.de'+flor_plan_image for flor_plan_image in floor_plan_images]
        landlord_name = (response.css("span.field-kontakt_anrede::text").get() or'') + ' ' +  (response.css("span.field-kontakt_vorname::text").get() or'') + ' ' + (response.css("span.field-kontakt_name::text").get() or'') 
        landlord_email = 'immobilien@lohmann-swertz.de'
        landlord_phone = '+49(0) 2821 / 72 72 - 32'
        item_loader = ListingLoader(response=response)

        # # MetaData
        item_loader.add_value("external_link", response.url) # String
        item_loader.add_value("external_source", self.external_source) # String
        item_loader.add_value("position", self.position) # Int

        item_loader.add_value("external_id", external_id) # String
        item_loader.add_value("title", title) # String
        item_loader.add_value("description", description) # String

        # Property Details
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

        item_loader.add_value("available_date", available_date) # String => date_format

        # item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        # item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking) # Boolean
        item_loader.add_value("elevator", elevator) # Boolean
        item_loader.add_value("balcony", balcony) # Boolean
        item_loader.add_value("terrace", terrace) # Boolean
        # item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        item_loader.add_value("washing_machine", washing_machine) # Boolean
        # item_loader.add_value("dishwasher", dishwasher) # Boolean

        # Images
        item_loader.add_value("images", images) # Array
        item_loader.add_value("external_images_count", len(images)) # Int
        item_loader.add_value("floor_plan_images", floor_plan_images) # Array

        # Monetary Status
        item_loader.add_value("rent", rent) # Int
        item_loader.add_value("deposit", deposit) # Int
        # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", currency) # String

        # item_loader.add_value("water_cost", water_cost) # Int
        # item_loader.add_value("heating_cost", heating_cost) # Int

        # item_loader.add_value("energy_label", energy_label) # String

        # LandLord Details
        item_loader.add_value("landlord_name", landlord_name) # String
        item_loader.add_value("landlord_phone", landlord_phone) # String
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