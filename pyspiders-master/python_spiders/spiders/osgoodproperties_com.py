# -*- coding: utf-8 -*-
# Author: Adham Mansour
from math import ceil

import requests
import scrapy
from bs4 import BeautifulSoup
from scrapy import Request

from ..helper import remove_unicode_char, extract_number_only, extract_location_from_address
from ..loaders import ListingLoader


class OsgoodepropertiesComSpider(scrapy.Spider):
    name = 'osgoodeproperties_com'
    allowed_domains = ['osgoodeproperties.com']
    start_urls = ['https://www.osgoodeproperties.com/searchlisting.aspx?']  # https not http
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
    def parse(self, response, ):
        rentals = response.css('.property-details')
        for rental in rentals:
            zipcode = rental.css('.propertyZipCode::text').extract_first()
            city = rental.css('.propertyCity::text').extract_first()
            address = rental.css('.propertyAddress::text').extract_first()
            bathroom_count = (rental.css('.propertyMaxBath::text').extract_first())
            if bathroom_count:
                bathroom_count = bathroom_count[0]
            else:
                bathroom_count = (rental.css('.propertyMinBath::text').extract_first())
                if bathroom_count:
                    bathroom_count = bathroom_count[0]
            room_count = (rental.css('.propertyMaxBed::text').extract_first())
            if room_count:
                room_count = room_count[0]
            else:
                room_count = (rental.css('.propertyMinBed::text').extract_first())
                if room_count:
                    room_count = room_count[0]
            square_meters = rental.css('.prop-area::text').extract_first()
            if square_meters:
                square_meters = square_meters.split('-')
                square_meters = [extract_number_only(i) for i in square_meters]
                if len(square_meters) == 2:
                    square_meters = (int(square_meters[0]) + int(square_meters[1])) /2
                else:
                    square_meters = square_meters[0]

            minrent = rental.css('.propertyMinRent::text').extract_first()
            maxrent = rental.css('.propertyMaxRent::text').extract_first()
            if minrent or maxrent:
                if minrent and maxrent:
                    rent = ceil((int(float(minrent))))
                elif maxrent:
                    rent = int(float(maxrent))
                elif minrent:
                    rent = (int(float(minrent)))
                else:
                    rent = 0
            else:
                rent = 0


            title = rental.css('.propertyUrl::text').extract_first()
            external_link = rental.css('.propertyUrl::attr(href)').extract_first()
            if rent != 0:
                yield Request(url=external_link ,
                              callback=self.populate_item,
                              meta={
                                  'zipcode' : zipcode,
                                  'city' : city,
                                  'address' : address,
                                  'bathroom_count' : bathroom_count,
                                  'room_count' : room_count,
                                  'rent' : rent,
                                  'square_meters' : square_meters,
                                  'title' : title
                              })

    # 3. SCRAPING level 3
    def populate_item(self, response):
        description = remove_unicode_char((((' '.join(response.css('#InnerContentDiv p::text').extract()).replace('\n','')).replace('\t','')).replace('\r','')))
        longitude, latitude = extract_location_from_address(response.meta['address'])
        property_type = 'apartment'

        url = (response.url).replace('index.aspx','photogallery.aspx')
        images = []
        url_get = requests.get(url)
        soup = BeautifulSoup(url_get.content, 'lxml')
        col = soup.find('div',class_=['owl-carousel'])
        ind_images = col.find_all('div',class_=['item'])
        for image in ind_images:
            image_meta = image.find('a')
            image_meta = ''.join(image_meta['href'])
            image_meta = image_meta.replace(' ','%20')
            image_meta = (image_meta.split('?'))[0]
            images.append(image_meta)
        images = list(dict.fromkeys(images))

        url = (response.url).replace('index.aspx','amenities.aspx')
        url_get = requests.get(url)
        soup = BeautifulSoup(url_get.content, 'lxml')
        col = soup.find_all('p',class_=['amenity_title'])
        amenities=[]
        for i in col:
            amenities.append(i.getText())
        amenities = ' '.join(amenities)
        amenities = amenities.lower()

        pets_allowed = None
        if 'pet' in amenities:
            pets_allowed = True

        furnished = None
        if 'furnish' in description.lower() or 'furnish' in amenities:
            furnished = True


        parking = None
        if 'parking' in amenities:
            parking = True

        elevator = None
        if 'elevator' in amenities:
            elevator = True

        balcony = None
        if 'balcony' in amenities:
            balcony = True

        terrace = None
        if 'terrace' in description.lower()  or 'terrace' in amenities:
            terrace = True


        swimming_pool = None
        if 'pool' in amenities:
            swimming_pool = True


        washing_machine = None
        if 'laundry' in amenities:
            washing_machine = True


        dishwasher = None
        if 'dishwasher' in description.lower() or 'dishwasher' in amenities:
            dishwasher = True

        url = (response.url).replace('index.aspx','floorplans.aspx')
        url_get = requests.get(url)
        soup = BeautifulSoup(url_get.content, 'lxml')
        borders = soup.find_all('tr',class_=['responsive-border'])
        for counter in range(1,len(borders)+1):
            rental = soup.find('tr',attrs={"data-selenium-id" : f"tRow{str(counter)}_1"})
            floor_plan_image = rental.find('img')
            floor_plan_images = floor_plan_image['data-src']
            room_count = rental.find('td',attrs={"data-label" : "Beds"})
            room_count = room_count.getText()
            room_count = room_count.replace('Bed/Bath','')
            roombath_count = room_count.split('/')
            if 'Studio' in roombath_count[0]:
                room_count = 1
                property_type = 'studio'
                bathroom_count = extract_number_only(roombath_count[1])
            else:
                room_count = extract_number_only(roombath_count[0])
                bathroom_count = extract_number_only(roombath_count[1])
            square_meters = rental.find('td',attrs={"data-label" : "SQ. FT."})
            square_meters = square_meters.getText()
            square_meters = square_meters.replace('Square Foot', '')
            square_meters = int(extract_number_only(extract_number_only(square_meters)))

            rent = rental.find('td',attrs={"data-label" : "Rent"})
            rent = rent.getText()
            rent = rent.replace('Rent', '')
            rent = rent.split('-')
            rent = int((extract_number_only(extract_number_only((rent[0])))))
            # # MetaData
            item_loader = ListingLoader(response=response)
            item_loader.add_value("external_link", (response.url).replace('index.aspx','floorplans.aspx')+'#'+str(counter))  # String
            item_loader.add_value("external_source", self.external_source)  # String

            # item_loader.add_value("external_id", external_id) # String
            item_loader.add_value("position", self.position)  # Int
            item_loader.add_value("title", response.meta['title']) # String
            item_loader.add_value("description", description) # String

            # # Property Details
            item_loader.add_value("city", response.meta['city']) # String
            item_loader.add_value("zipcode", response.meta['zipcode']) # String
            item_loader.add_value("address", response.meta['address']) # String
            item_loader.add_value("latitude", str(latitude)) # String
            item_loader.add_value("longitude", str(longitude)) # String
            # item_loader.add_value("floor", floor) # String
            item_loader.add_value("property_type", property_type) # String
            item_loader.add_value("square_meters", square_meters) # Int
            item_loader.add_value("room_count", room_count) # Int
            item_loader.add_value("bathroom_count", bathroom_count) # Int

            # item_loader.add_value("available_date", available_date) # String => date_format also "Available", "Available Now" ARE allowed

            item_loader.add_value("pets_allowed", pets_allowed) # Boolean
            item_loader.add_value("furnished", furnished) # Boolean
            item_loader.add_value("parking", parking) # Boolean
            item_loader.add_value("elevator", elevator) # Boolean
            item_loader.add_value("balcony", balcony) # Boolean
            item_loader.add_value("terrace", terrace) # Boolean
            item_loader.add_value("swimming_pool", swimming_pool) # Boolean
            item_loader.add_value("washing_machine", washing_machine) # Boolean
            item_loader.add_value("dishwasher", dishwasher) # Boolean

            # # Images
            item_loader.add_value("images", images) # Array
            item_loader.add_value("external_images_count", len(images)) # Int
            item_loader.add_value("floor_plan_images", floor_plan_images) # Array

            # # Monetary Status
            item_loader.add_value("rent", rent) # Int
            # item_loader.add_value("deposit", deposit) # Int
            # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
            # item_loader.add_value("utilities", utilities) # Int
            item_loader.add_value("currency", "CAD") # String

            # item_loader.add_value("water_cost", water_cost) # Int
            # item_loader.add_value("heating_cost", heating_cost) # Int

            # item_loader.add_value("energy_label", energy_label) # String

            # # LandLord Details
            item_loader.add_value("landlord_name", 'osgoodeproperties') # String
            item_loader.add_value("landlord_phone", '(866) 794-1222') # String
            # item_loader.add_value("landlord_email", landlord_email) # String

            self.position += 1
            yield item_loader.load_item()
