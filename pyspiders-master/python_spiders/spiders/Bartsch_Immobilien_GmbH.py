# -*- coding: utf-8 -*-
# Author: Muhammad Alaa
from re import I
import scrapy
from ..loaders import ListingLoader
from ..helper import description_cleaner, extract_number_only, extract_rent_currency, extract_location_from_address, extract_location_from_coordinates
from dateutil.parser import parse
from datetime import datetime

class BartschImmobilienGmbhSpider(scrapy.Spider):
    name = "Bartsch_Immobilien_GmbH"
    start_urls = [
        'https://www.bartsch-immo.de/immobilien/?post_type=immomakler_object&vermarktungsart&nutzungsart&typ&ort',
        'https://www.bartsch-immo.de/immobilien-arten/wohnung/?vermarktungsart=miete'
        ]
    allowed_domains = ["bartsch-immo.de"]
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
        properties = response.css('div.property-container')
        urls = properties.css('a.thumbnail::attr(href)').getall()
        for url in urls:
            yield scrapy.Request(url, callback=self.populate_item)
        next_page = response.css('a.next.page-numbers::attr(href)').get()
        yield scrapy.Request(next_page, callback=self.parse)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        washing_machine = address = property_type = balcony = terrace = elevator = external_id = furnished = parking = None
        bathroom_count = available_date = deposit = heating_cost = rent = currency = square_meters = None
        room_count = 1
        property_type = 'apartment'
        
        keys = response.css('div.dt::text').getall()
        vals = response.css('div.dd')
        for row in zip(keys, vals):
            key = row[0].strip()
            val = row[1].css('::text').get()
            key = key.lower()
            if "objekt id" in key:
                external_id = val
            elif "schlafzimmer" in key:
                room_count = val
            elif "badezimmer" in key:
                bathroom_count = val
            elif "objekttypen" in key:
                if 'grundstück' in val.lower() or 'wohnung' in val.lower() or 'wohngrundstück' in val.lower():
                    property_type = 'apartment'
                elif 'haus' in val.lower() or 'reiheneckhaus' in val.lower():
                    property_type = 'house'
                else:
                    return
            elif "gesamtfläche" in key and square_meters is None:
                square_meters = int(float(extract_number_only(
                    val, thousand_separator='.', scale_separator=',')))
            elif "wohnfläche" in key:
                square_meters = int(float(extract_number_only(
                    val, thousand_separator='.', scale_separator=',')))
            elif "kaltmiete" in key:
                rent, currency = extract_rent_currency(
                    val, self.country, BartschImmobilienGmbhSpider)
                rent = get_price(val)
            elif "heizkosten netto" in key:
                heating_cost = get_price(val)
            elif "kaution" in key:
                deposit = get_price(val)
            elif "verfügbar ab" in key:
                if 'vermietet' in val.lower():
                    return
                elif 'rücksprache' in val.lower() or 'nach vereinbarung' in val.lower():
                    available_date = None
                elif 'sofort' in val.lower():
                    available_date = datetime.now().strftime("%Y/%m/%d")
                else:
                    available_date = parse(val).strftime("%Y/%m/%d")
        address = response.css('h2.property-subtitle::text').get()
        if rent is None:
            return
        if property_type is None:
            return
        longitude, latitude = extract_location_from_address(address)
        zipcode, city, address = extract_location_from_coordinates(longitude=longitude, latitude=latitude)
        if address == zipcode:
            address = zipcode + " " + city
        title = response.css("h1.property-title::text").get().strip()
        if 'Ausstattung' in response.css('div.property-description div.panel-body h3::text').getall():
            furnished = True

        descriptions = response.css('div.property-description div.panel-body p::text').getall()
        description = ''
        for des in descriptions:
            description += des + ' '
        if 'Aufzug' in description:
            elevator = True
        lower_description = description.lower()
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
        landlord_name = 'Bartsch Immobilien GmbH'
        if 'Wolfratshausen' in address:
            landlord_email = 'wor@bartsch-immo.de'
            landlord_number = '08171 424042'
        else:
            landlord_email = 'muc@bartsch-immo.de'
            landlord_number = '089 219098299'
        floor_plan_images = response.css('a.example-image-link img::attr(src)').getall()

        keys = response.css('meta::attr(property)').getall()
        vals = response.css('meta::attr(content)').getall()
        images = response.css("meta[property='og:image:secure_url']::attr(content)").getall()
        images.append(response.css('div.immomakler img::attr(src)').get())
        item_loader = ListingLoader(response=response)

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
        #item_loader.add_value("floor", floor) # String
        item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters) # Int
        item_loader.add_value("room_count", room_count) # Int
        item_loader.add_value("bathroom_count", bathroom_count) # Int

        item_loader.add_value("available_date", available_date) # String => date_format

        #item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        item_loader.add_value("furnished", furnished) # Boolean
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
        item_loader.add_value("floor_plan_images", floor_plan_images) # Array

        # # Monetary Status
        item_loader.add_value("rent", rent) # Int
        item_loader.add_value("deposit", deposit) # Int
        #item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        #item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", currency) # String

        #item_loader.add_value("water_cost", water_cost) # Int
        item_loader.add_value("heating_cost", heating_cost) # Int

        #item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name) # String
        item_loader.add_value("landlord_phone", landlord_number) # String
        item_loader.add_value("landlord_email", landlord_email) # String

        self.position += 1
        yield item_loader.load_item()


def get_price(val):
    v = int(float(extract_number_only(
        val, thousand_separator=',', scale_separator='.')))
    v2 = int(float(extract_number_only(
        val, thousand_separator='.', scale_separator=',')))
    price = min(v, v2)
    if price < 10:
        price = max(v, v2)
    return price
