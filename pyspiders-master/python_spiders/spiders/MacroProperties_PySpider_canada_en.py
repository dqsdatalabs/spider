# -*- coding: utf-8 -*-
# Author: Muhammad Alaa
import scrapy
from ..loaders import ListingLoader
from ..helper import format_date, sq_feet_to_meters, extract_number_only, format_date
from datetime import datetime
import json
import re




class MacropropertiesPyspiderCanadaEnSpider(scrapy.Spider):
    name = "MacroProperties_PySpider_canada_en"
    start_urls = ['https://api.theliftsystem.com/v2/search?locale=en&client_id=52&auth_token=sswpREkUtyeYjeoahA2i&city_id=304&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=0&max_rate=2500&min_sqft=0&max_sqft=10000&show_custom_fields=true&show_promotions=true&region=&keyword=false&property_types=apartments,+houses&ownership_types=&exclude_ownership_types=&custom_field_key=&custom_field_values=&order=min_rate+ASC&limit=66&neighbourhood=&amenities=&promotions=&city_ids=2555,897,1117,1389,2005,2293,2295,3133,2489,304&pet_friendly=&offset=0&count=false']
    allowed_domains = ["macroproperties.com", "theliftsystem.com"]
    country = 'canada'
    locale = 'en'
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
        listings = json.loads(response.body)
        for listing in listings:
            yield scrapy.Request(listing['permalink'], callback=self.populate_item, meta={**listing})
    # 3. SCRAPING level 3
    def populate_item(self, response):
        data = response.meta
        amenities = response.css("div.amenity-holder::text").getall()
        sqaure_feet = response.css(
            "div.suite-sqft.cell > span.faded::text").getall()
        dates = response.css("div.suite-availability.cell")
        available_dates = []
        for div in dates:
            spans = div.css("span.faded")
            if spans:
                available_dates.append(spans.css("::text").get())
            else:
                date = div.css("a.open-suite-modal::text").get()
                if "Waitlist" in date:
                    available_dates.append(date)
                else:
                    date_index = date.lower().find("available") + len("available") + 1
                    available_dates.append(date[date_index:])

        for index, unit in enumerate(response.css("script[type='application/ld+json']::text").getall()):
            unit = json.loads(unit)
            if unit['price'] == "" or int(unit['price']) == 0:
                continue

            external_id = str(data['id'])
            title = unit['name']
            descriptions = response.css(
                "#content > div > div > div > p::text").getall()
            description = ''
            for desc in descriptions:
                description += desc
            city = data['address']['city']
            zipcode = data['address']['postal_code']
            address = data['address']['address']
            latitude = data['geocode']['latitude']
            longitude = data['geocode']['longitude']
            square_meters = None
            if len(sqaure_feet) > index:
                square_meters = sqaure_feet[index] if sqaure_feet[index].isnumeric() else None
            room_count = 1
            if unit['name'][0].isnumeric():
                room_count = int(unit['name'][0])

            property_type = 'apartment' if room_count > 1 else 'studio'
            pets_allowed = None if data['pet_friendly'] == 'n/a' else data['pet_friendly']
            bathroom_count = int(response.css(
                "div.suite-bath.cell > span.value::text").getall()[index][0])
            available_date = re.sub(re.compile('<.*?>'), '', available_dates[index])
            if not available_date[0].isnumeric():
                available_date = datetime.now().strftime("%Y-%m-%d") if available_date.lower() == 'inquire now' else None
            
            parking = None
            balcony = None
            dishwasher = None
            furnished = None
            washing_machine = None 
            if "Furnished" in unit['name']:
                furnished = True
            for amenity in amenities:
                if amenity.lower().find('balcony') != -1 or amenity.lower().find('balconies') != -1:
                    balcony = True
                if amenity.lower().find('laundry') != -1:
                    washing_machine = True
                if amenity.lower().find('dishwasher') != -1:
                    dishwasher = True
                if amenity.lower().find('parking') != -1:
                    parking = True
            images = response.css(
                "div.gallery-image > div.cover::attr(data-src2x)").getall()
            rent = int(unit['price'])
            landlord_name = data['client']['name']
            landlord_number = response.css("a.phone::text").getall()[-1].strip()
            landlord_email = data['client']['email']

            item_loader = ListingLoader(response=response)

            # # MetaData
            item_loader.add_value(
                "external_link", response.url + f'#{index+1}')  # String
            item_loader.add_value(
                "external_source", self.external_source)  # String
            item_loader.add_value("position", self.position)  # Int

            item_loader.add_value("external_id", external_id)  # String
            item_loader.add_value("title", title)  # String
            item_loader.add_value("description", description)  # String

            # # Property Details
            item_loader.add_value("city", city)  # String
            item_loader.add_value("zipcode", zipcode)  # String
            item_loader.add_value("address", address)  # String
            item_loader.add_value("latitude", latitude)  # String
            item_loader.add_value("longitude", longitude)  # String
            item_loader.add_value("property_type", property_type)
            item_loader.add_value("square_meters", square_meters)  # Int
            item_loader.add_value("room_count", room_count)  # Int
            item_loader.add_value("bathroom_count", bathroom_count)  # Int
            item_loader.add_value("available_date", available_date)

            item_loader.add_value("pets_allowed", pets_allowed)  # Boolean
            item_loader.add_value("furnished", furnished) # Boolean

            item_loader.add_value("parking", parking)  # Boolean
            item_loader.add_value("balcony", balcony)  # Boolean
            item_loader.add_value("washing_machine", washing_machine) # Boolean
            item_loader.add_value("dishwasher", dishwasher)  # Boolean

            # # Images
            item_loader.add_value("images", images)  # Array
            item_loader.add_value("external_images_count", len(images))  # Int
            # # Monetary Status
            item_loader.add_value("rent", rent)  # Int
            item_loader.add_value("currency", "CAD")  # String

            # # LandLord Details
            item_loader.add_value("landlord_name", landlord_name)  # String
            item_loader.add_value("landlord_phone", landlord_number)  # String
            item_loader.add_value("landlord_email", landlord_email)  # String

            self.position += 1
            yield item_loader.load_item()
