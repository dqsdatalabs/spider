# -*- coding: utf-8 -*-
# Author: Muhammad Ahmad Hesham
import scrapy
from ..loaders import ListingLoader
from dateutil.parser import parse
from datetime import datetime
from ..helper import extract_number_only, extract_rent_currency, extract_location_from_address, extract_location_from_coordinates, remove_white_spaces


class NibelungenwohnbaugmbhbraunschweigSpider(scrapy.Spider):
    name = "NibelungenWohnbauGmbHBraunschweig"
    start_urls = ['https://www.nibelungen-wohnbau.de/wohnen/wohnungssuche/ergebnisse?start=0']
    allowed_domains = ["nibelungen-wohnbau.de"]
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
        for listing in  response.css('div.o-reo-preview div.o-reo-preview__facts'):
            yield scrapy.Request('https://www.nibelungen-wohnbau.de'+listing.css('a::attr(href)').get(), callback=self.populate_item)
        current_page = response.css('span.o-pagination__page__active::text').get()
        for pages in  response.css('li.o-pagination__page'):
            next_page = pages.css("a::text").get()
            if next_page is not None and next_page > current_page:
                yield scrapy.Request('https://www.nibelungen-wohnbau.de'+pages.css("a::attr(href)").get(), callback=self.parse)
                break

    # 3. SCRAPING level 3
    def populate_item(self, response):
        floor_plan_images = washing_machine = address = property_type = pets_allowed = balcony = terrace = elevator = external_id = floor = parking = None
        heating_cost = energy_label =  utilities = bathroom_count = available_date = deposit = total_rent = rent = currency = square_meters = landlord_email = landlord_name = landlord_phone = None
        room_count = bathroom_count = 1
        property_type = 'apartment'
        address = response.css("div.c-quickview-box__info-slot p span::text").get().strip() + ' ' + response.css("div.c-quickview-box__info-slot p::text").get().strip()
        title =  response.css("h1.o-reo-header__heading::text").get().strip()
        
        for box in response.css('div.o-section.o-section--line-bottom.o-section--lg-no-line-bottom div.col-lg-6'):
            header = box.css('h3::text').extract_first()
            if header and header.strip() == 'Objektbeschreibung':
                description = '\n'.join([x for x in box.css(' ::text').getall() if '@' not in x and ' Tel' not in x]).replace('Objektbeschreibung','').strip()
                
        external_id = response.url.split('/')[-1]
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

        for row in response.css("tr"):
            if len(row.css("td::text").getall()) < 2:
                continue
            key = row.css("td::text").get()
            val = row.css("td::text").getall()[-1]
            if val is not None: val = val.strip()
            key = key.lower()
            if "etage" in key:
                floor = val.strip()[0]
                floor = None if not floor.isnumeric() else floor
            elif "wohnfläche" in key:
                square_meters = int(float(extract_number_only(val, thousand_separator='.', scale_separator=',')))
            elif 'Effizienzklasse'.lower() in key:
                energy_label = val
            elif "bezugsfrei" in key:
                if 'sofort' in val.lower():
                    available_date = datetime.now().strftime("%Y-%m-%d")
                elif 'vermietet' in val.lower():
                    return
                else:
                    try:
                        available_date = parse(val.split(' ')[0]).strftime("%Y-%m-%d")
                    except:
                        available_date = None  
            elif "grundmiete" in key:
                rent, currency = extract_rent_currency(val, self.country, NibelungenwohnbaugmbhbraunschweigSpider)
                rent = get_price(val)
            elif "heizung" in key:
                heating_cost = get_price(val) if get_price(val) !=0 else heating_cost
            elif "betriebskosten" in key:
                utilities = get_price(val) if get_price(val) !=0 else utilities
            elif "kaution" in key:
                deposit = get_price(val)
                if deposit < 10:
                    deposit *= rent
            elif 'objektart' in key:
                property_type = val
                if 'wohnung' in property_type.lower():
                    property_type = 'apartment'
                elif 'familienhaus' in property_type.lower():
                    property_type = 'house'
                elif 'souterrain' in property_type.lower():
                    property_type = 'apartment'
                elif 'apartment' in property_type.lower():
                    property_type = 'apartment'
                elif 'dach­geschoss' in property_type.lower():
                    property_type = 'apartment'
                elif 'zimmer' in property_type.lower():
                    property_type = 'room'
                else: return
            elif "anzahl zimmer" in key:
                room_count = int(float(val.replace(',', '.')))
            if 'objektnummer' in key:
                external_id = val
            elif 'objektanschrift' in key:
                address = remove_white_spaces(val.strip())               
            elif "anzahl badezimmer" in key:
                bathroom_count = int(float(val.split(',')[0]))       
            elif "balkon" in key:
                balcony = True
            elif "warmmiete" in key:
                total_rent, currency = extract_rent_currency(val, self.country, NibelungenwohnbaugmbhbraunschweigSpider)
                total_rent = get_price(val)
            elif "stellplatz" in key:
                parking = False if 'nein' in val.lower() else True
            elif "aufzug" in key:
                elevator = True
            elif key.find("terrasse") != -1:
                terrace = True if 'terrasse' in val.lower() else None
            elif 'wasch' in key:
                washing_machine = True
            elif 'haustiere' in key:
                pets_allowed = False if 'nein' in val.lower() else True

        # s=response.css("body script[type='text/javascript']::text").get()
        # for x in s[s.find('center: {')+len('center: {'):s.find('},')].strip().split(','):
        #     if 'lat' in x:
        #         latitude = float(x.split(':')[1].strip())
        #     elif 'lon' in x:
        #         longitude = float(x.split(':')[1].strip())
        longitude, latitude = extract_location_from_address(address)
        zipcode, city, a = extract_location_from_coordinates(longitude=longitude, latitude=latitude)

        images = ['https://www.nibelungen-wohnbau.de' + x for x in response.css("div.o-image-slider__slide.swiper-slide img::attr(src)").getall()]

        landlord_phone = '+49 800 0531 123'
        landlord_email = 'angebote@nibelungen-wohnbau.de'
        landlord_name = 'Nibelungen-Wohnbau-GmbH Braunschweig'

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

        item_loader.add_value("pets_allowed", pets_allowed) # Boolean
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
        item_loader.add_value("heating_cost", heating_cost) # Int

        item_loader.add_value("energy_label", energy_label) # String

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