# -*- coding: utf-8 -*-
# Author: Muhammad Ahmad Hesham
import scrapy
from ..loaders import ListingLoader
import re
from dateutil.parser import parse
from datetime import datetime
from ..helper import extract_number_only, extract_rent_currency, extract_location_from_address, extract_location_from_coordinates


class DhImmobilienserviceSpider(scrapy.Spider):
    name = "DHImmobilienservice"
    start_urls = ['http://dh-immobilien-service.de/index.php/mietangebote?wrapped_page=1']
    allowed_domains = ["dh-immobilien-service.de"]
    country = 'germany' # Fill in the Country's name
    locale = 'de' # Fill in the Country's locale, look up the docs if unsure
    custom_settings = { 
        "CONCURRENT_REQUESTS" : 4,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": .5,
        "AUTOTHROTTLE_MAX_DELAY": 2,
        "DOWNLOAD_TIMEOUT": 25,
        "DOWNLOAD_DELAY": 2,
        "RETRY_TIMES": 20,
        "COOKIES_ENABLED": False,
        "COOKIES_DEBUG": False
    }
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
        for listing in response.css("div.openestate_listing_entry h2 a::attr(href)").getall():
            yield scrapy.Request("http://dh-immobilien-service.de" + listing , callback=self.populate_partial)
        pages = response.css("div#openestate_listing_pagination_bottom  a::text").getall()
        current_page = response.url.split("=")[-1]
        for page in pages:
            if page.isdigit() and int(page) > int(current_page):
                yield scrapy.Request("http://dh-immobilien-service.de/index.php/mietangebote?wrapped_page=" + page, callback=self.parse)
                break

    # 3. SCRAPING level 3
    def populate_partial(self, response):
        washing_machine = address = property_type = pets_allowed = balcony = terrace = elevator = external_id = floor = parking = None
        bathroom_count = available_date = deposit = total_rent = rent = currency = square_meters = None
        room_count = 1
        property_type = 'apartment'
        address = ''
        
        keys = []
        vals = []
        keys.extend(response.css("div#openestate_expose_header li div"))
        keys.extend(response.css("div#openestate_expose_view_content li"))
        vals.extend(response.css("div#openestate_expose_header li"))
        vals.extend(response.css("div#openestate_expose_view_content li b"))
        for index, row in enumerate(zip(keys, vals)):
            key = row[0].css("::text").get()
            val = row[1].css("::text").getall()[-1]
            if key is None:
                address = val + ' ' + address
                continue
            key = key.lower()
            if "objekt-nr" in key:
                external_id = val
            elif "immobilienart" in key:
                property_type = val
                if 'wohnung' in property_type.lower():
                    property_type = 'apartment'
                elif 'haus' in property_type.lower():
                    property_type = 'house'
                elif 'souterrain' in property_type.lower():
                    property_type = 'apartment'
                elif 'apartment' in property_type.lower():
                    property_type = 'apartment'
                elif 'dach­geschoss' in property_type.lower():
                    property_type = 'apartment'
                else: return
            elif 'vermarktungsart' in key:
                if 'miete' not in val.lower(): return
            elif 'adresse' in key:
                address = address + " " + val
            elif "kaltmiete" in key:
                rent, currency = extract_rent_currency(val, self.country, DhImmobilienserviceSpider)
                rent = get_price(val)
            elif "warmmiete" in key:
                total_rent, currency = extract_rent_currency(val, self.country, DhImmobilienserviceSpider)
                total_rent = get_price(val)
            elif "stellplatz" in key:
                parking = True
            elif "kaution" in key:
                deposit = get_price(val)
            elif "wohnfläche" in key:
                square_meters = int(float(extract_number_only(val, thousand_separator='.', scale_separator=',')))
            elif "anzahl badezimmer" in key:
                bathroom_count = int(float(val.split(',')[0]))
            elif "zimmerzahl" in key:
                room_count = int(float(val.replace(',', '.')))
            elif "balkon" in key:
                balcony = True
            elif "etage" in key:
                floor = val.strip()[0]
                floor = None if not floor.isnumeric() else floor
            elif 'weitere räume' in key:
                washing_machine = True if 'wasch' in val.lower() else None
            elif "frei ab" in key:
                if 'vermietet' in val.lower():
                    return
                elif 'rücksprache' in val.lower():
                    available_date = None
                elif 'sofort' in val.lower():
                    available_date = datetime.now().strftime("%Y-%m-%d")
                else:
                    available_date = parse(val).strftime("%Y-%m-%d")
            elif "aufzug" in key:
                elevator = True
            elif key.find("terrasse") != -1:
                terrace = True if 'terrasse' in val.lower() else None
            elif 'kaufpreis' in key:
                return

        utilities = total_rent - rent
        longitude, latitude = extract_location_from_address(address)
        zipcode, city, address = extract_location_from_coordinates(longitude=longitude, latitude=latitude)
        if address == zipcode:
            address = zipcode + " " + city
        title = response.css("div#openestate_expose_header > h2::text").get().strip()
        data = {
            'washing_machine': washing_machine, 
            'address': address, 
            'property_type': property_type, 
            'pets_allowed': pets_allowed,
            'balcony': balcony,
            'terrace': terrace,
            'elevator': elevator,
            'external_id': external_id,
            'floor': floor,
            'parking': parking,
            'bathroom_count': bathroom_count,
            'available_date': available_date,
            'deposit': deposit,
            'total_rent': total_rent,
            'rent': rent,
            'currency': currency,
            'square_meters': square_meters,
            'room_count': room_count,
            'utilities': utilities,
            'longitude': longitude,
            'latitude': latitude,
            'zipcode': zipcode,
            'city': city,
            'title': title
            }
        links = response.css("div#openestate_expose_view_menu li a::attr(href)").getall()
        description_link = None
        for link in links:
            if 'texts' in link.lower():
                description_link = link
        yield scrapy.Request("http://dh-immobilien-service.de" + description_link, callback=self.populate_description, meta={**data})
        
    def populate_description(self, response):
        description =  '\n'.join([x.strip() for x in response.css("div#openestate_expose_view_content p::text").getall() if 'tel:' not in x.lower() and 'www' not in x.lower() and 'unter' not in x.lower() and 'Tel.-Nr' not in x.lower()])
        
        lower_description = description.lower()
        if "stellplatz" in lower_description or "garage" in lower_description or "parkhaus" in lower_description or "tiefgarage" in lower_description:
            response.meta['parking'] = True
        if 'balkon' in lower_description:
            response.meta['balcony'] = True
        if 'aufzug' in lower_description:
            response.meta['elevator'] = True
        if 'terrasse' in lower_description:
            response.meta['terrace'] = True
        if 'waschmaschine' in lower_description:
            response.meta['washing_machine'] = True 
        
        links = response.css("div#openestate_expose_view_menu li a::attr(href)").getall()
        images_link = None
        for link in links:
            if 'gallery' in link.lower():
                images_link = link
        yield scrapy.Request("http://dh-immobilien-service.de" + images_link, callback=self.populate_images, meta={**response.meta, 'description': description})
        
        
    def populate_images(self, response):
        # images = Selector(text = re.findall("<ul>.*?</ul>", response.css("div#openestate_expose_gallery_thumbnails script").get())[0]).css("a::attr(href)").getall()
        images = response.css("div#openestate_expose_header_image a::attr(href)").getall()
        floor_plan_images = None
        links = response.css("div#openestate_expose_view_menu li a::attr(href)").getall()
        contact_link = None
        for link in links:
            if 'contact' in link.lower():
                contact_link = link
        yield scrapy.Request("http://dh-immobilien-service.de" + contact_link, callback=self.populate_landlord, meta={**response.meta, 'images': images, 'floor_plan_images': floor_plan_images})
    
    def populate_landlord(self, response):
        keys = response.css("div#openestate_expose_view_content li div")
        vals = response.css("div#openestate_expose_view_content li")
        landlord_phone = landlord_name = landlord_email = None
        for item in zip(keys, vals):
            key = item[0].css("::text").getall()[-1]
            val = item[1].css("::text").getall()[-1]
            if 'name' in key.lower():
                landlord_name = val
            elif 'telefon' in key.lower():
                landlord_phone = val
        landlord_email = 'info@dh-immobilien-service.de'

        item_loader = ListingLoader(response=response)

        # # MetaData
        item_loader.add_value("external_link", response.url) # String
        item_loader.add_value("external_source", self.external_source) # String
        item_loader.add_value("position", self.position) # Int

        item_loader.add_value("external_id", response.meta['external_id']) # String
        item_loader.add_value("title", response.meta['title']) # String
        item_loader.add_value("description", response.meta['description']) # String

        # Property Details
        item_loader.add_value("city", response.meta['city']) # String
        item_loader.add_value("zipcode", response.meta['zipcode']) # String
        item_loader.add_value("address", response.meta['address']) # String
        item_loader.add_value("latitude", str(response.meta['latitude'])) # String
        item_loader.add_value("longitude", str(response.meta['longitude'])) # String
        item_loader.add_value("floor", response.meta['floor']) # String
        item_loader.add_value("property_type", response.meta['property_type']) # String => ["apartment", response.meta['"house", response.meta['"room", response.meta['"student_apartment", response.meta['"studio"]
        item_loader.add_value("square_meters", response.meta['square_meters']) # Int
        item_loader.add_value("room_count", response.meta['room_count']) # Int
        item_loader.add_value("bathroom_count", response.meta['bathroom_count']) # Int

        item_loader.add_value("available_date", response.meta['available_date']) # String => date_format

        item_loader.add_value("pets_allowed", response.meta['pets_allowed']) # Boolean
        # item_loader.add_value("furnished", response.meta['furnished']) # Boolean
        item_loader.add_value("parking", response.meta['parking']) # Boolean
        item_loader.add_value("elevator", response.meta['elevator']) # Boolean
        item_loader.add_value("balcony", response.meta['balcony']) # Boolean
        item_loader.add_value("terrace", response.meta['terrace']) # Boolean
        # item_loader.add_value("swimming_pool", response.meta['swimming_pool']) # Boolean
        item_loader.add_value("washing_machine", response.meta['washing_machine']) # Boolean
        # item_loader.add_value("dishwasher", response.meta['dishwasher']) # Boolean

        # Images
        item_loader.add_value("images", response.meta['images']) # Array
        item_loader.add_value("external_images_count", len(response.meta['images'])) # Int
        item_loader.add_value("floor_plan_images", response.meta['floor_plan_images']) # Array

        # Monetary Status
        item_loader.add_value("rent", response.meta['rent']) # Int
        item_loader.add_value("deposit", response.meta['deposit']) # Int
        # item_loader.add_value("prepaid_rent", response.meta['prepaid_rent']) # Int
        item_loader.add_value("utilities", response.meta['utilities']) # Int
        item_loader.add_value("currency", response.meta['currency']) # String

        # item_loader.add_value("water_cost", response.meta['water_cost']) # Int
        # item_loader.add_value("heating_cost", response.meta['heating_cost']) # Int

        # item_loader.add_value("energy_label", response.meta['energy_label']) # String

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