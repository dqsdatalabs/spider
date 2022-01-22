# -*- coding: utf-8 -*-
# Author: Abanoub Moris
import scrapy
from scrapy.http.request import Request
from ..loaders import ListingLoader
from ..helper import *
import re
import json


class Stuttgart_bookooSpider(scrapy.Spider):

    name = "stuttgart_bookoo"

    country = 'germany'  # Fill in the Country's name
    locale = 'de'  # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'

    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        for page in range(1, 7):
            url = f'https://stuttgart.bookoo.com/homes-rent-by-owner/page{page}'
            yield Request(url,
                          callback=self.parseApartment,
                          dont_filter=True)

        for page in range(1, 7):
            url = f'http://stuttgart.bookoo.com/homes-rent-by-agent/page{page}'
            yield Request(url,
                          callback=self.parseApartment, 
                          dont_filter=True)
    # 2. SCRAPING level 2

    def parse(self, response):
        pass

    def parseApartment(self, response):

        apartments = response.css('#middleColumn .itemListGallery a')
        for apartment in apartments:
            url = 'https://stuttgart.bookoo.com' + \
                apartment.css('::attr(href)').get()
            external_id = apartment.css("span::attr('data-itemid')").get()

            day=False
            renttxt = apartment.css(".price::text").get()
            
            if renttxt:
                if 'night' in renttxt:
                    day=True
                try:
                    rent = int(re.search(
                        r'\d+', renttxt.replace(',00', ''))[0])
                    if int(rent)<15:
                        rent = int(re.search(
                        r'\d+', renttxt.replace(',00', '').replace('.',''))[0])
                    if day:
                        rent*=30
                except:
                    continue
                
            else:
                continue

            datausage = {
                'rent': rent,
                'external_id': external_id,
            }

            yield Request(url, meta=datausage, callback=self.populate_item, dont_filter=True)

    # 3. SCRAPING level 3

    def populate_item(self, response):

        rent = response.meta['rent']
        external_id = response.meta['external_id']
        title = response.css('.top h1::text').get()
        property_type = 'apartment' if 'apartment' in title.lower() else 'house'

        landlord_name = response.css('.subTitle a .aliasName::text').get()
        if not landlord_name:
            landlord_name = 'stuttgart bookoo'

        landlord_phone = response.css('.messaging a::attr(href)').get()
        if landlord_phone:
            landlord_phone = landlord_phone.replace('tel:', '')

        '''

        square_meters = response.css(".listobject-information tr:contains(area) span::text").get()
        if square_meters:
            square_meters = re.search(
                r'\d+', square_meters)[0]



        td = response.css(".objectdetails-details tr:contains(bathroom) td *::text").getall()
        bathroom_count = ''
        room_count = ''
        for i,t in enumerate(td):
            if 'bathroom' in t:
                bathroom_count = td[i+1]
            
        td = response.css(".objectdetails-details tr:contains(bedrooms) td *::text").getall()
        for i,t in enumerate(td):
            if 'bedrooms' in t:
                room_count = td[i+1]

        td = response.css(".objectdetails-details tr:contains(tilities) td *::text").getall()
        utilities=''
        for i,t in enumerate(td):
            if 'tilities' in t:
                utilities = td[i+1]
        '''

        '''latlng = "".join(response.css('.objectdetails-googlemaps script::text').getall())
        location = extract_coordinates_reqex(latlng)
        latitude = str(location[0])
        longitude = str(location[1])
        zipcode, city, address = extract_location_from_coordinates(longitude,latitude)'''

        description = remove_white_spaces(
            "".join(response.css("#description::text").getall())).replace('?', '').lower()
        landlord_email = re.search(r'Mail:.+com', description)
        if landlord_email:
            landlord_email = landlord_email.replace('E-Mail: ', '')
        description = re.sub(r'please.+', '', description)
        description = re.sub(r'contact.+', '', description)
        description = re.sub(r'if you have any questions.+', '', description)

        square_meters = re.search(r'\d+ sqm|\d+ m2', description)
        if square_meters:
            square_meters = square_meters[0].replace(' sqm', '')

        room_count = re.search(r'\d+ bedroom', description)

        bathroom_count = re.search(r'\d+ bathroom', description)
        if bathroom_count:
            bathroom_count = bathroom_count[0].replace(' bathroom', '')
        if room_count:
            room_count = room_count[0].replace(' bedroom', '')

        else:
            room_count = re.search(r'bedroom \d+', description)
            if room_count:
                room_count = room_count[0].replace('bedroom ', '')

            bathroom_count = re.search(r'bathroom \d+', description)
            if bathroom_count:
                bathroom_count = bathroom_count[0].replace('bathroom ', '')

        available_date = re.search(r'available: \d+.\d+.\d+', description)
        if available_date:
            available_date = available_date[0].replace('available: ', '')

        images = response.css('#itemDetails #desktop .image::attr(guid)').getall()
        images = [
            f'https://s3item-bookooinc.netdna-ssl.com/s640_{x}.jpg' for x in images]

        for i,x in enumerate(images):
            if 'Access Denied' in requests.get(x).text:
                images[i] = images[i].replace('jpg','jpeg') 
                if 'Access Denied' in requests.get(images[i]).text:
                    images[i] = images[i].replace('jpeg','png')
        
        images = [x for x in images if 'png' not in x]

        if not room_count or room_count=='':
            room_count='1'

        

        if int(rent) > 0 and int(rent) < 20000:
            item_loader = ListingLoader(response=response)

            # # MetaData
            item_loader.add_value("external_link", response.url)  # String
            item_loader.add_value(
                "external_source", self.external_source)  # String

            item_loader.add_value("external_id", external_id)  # String
            item_loader.add_value("position", self.position)  # Int
            item_loader.add_value("title", title)  # String
            item_loader.add_value("description", description)  # String

            # # Property Details
            item_loader.add_value("city", 'stuttgart')  # String
            #item_loader.add_value("zipcode", zipcode)  # String
            #item_loader.add_value("address", address)  # String
            #item_loader.add_value("latitude", latitude)  # String
            #item_loader.add_value("longitude", longitude)  # String
            # item_loader.add_value("floor", floor)  # String
            item_loader.add_value("property_type", property_type)  # String
            item_loader.add_value("square_meters", square_meters)  # Int
            item_loader.add_value("room_count", room_count)  # Int
            item_loader.add_value("bathroom_count", bathroom_count)  # Int

            # String => date_format also "Available", "Available Now" ARE allowed
            #item_loader.add_value("available_date", available_date)

            self.get_features_from_description(
                description+" ".join(response.css(".objectdetails-details tr td *::text").getall()), response, item_loader)

            # # Images
            item_loader.add_value("images", images)  # Array
            item_loader.add_value("external_images_count", len(images))  # Int
            # item_loader.add_value("floor_plan_images", floor_plan_images) # Array

            # # Monetary Status
            item_loader.add_value("rent", rent)  # Int
            # item_loader.add_value("deposit", deposit) # Int
            # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
            #item_loader.add_value("utilities", utilities)  # Int
            item_loader.add_value("currency", "EUR")  # String

            # item_loader.add_value("water_cost", water_cost) # Int
            # item_loader.add_value("heating_cost", heating_cost) # Int

            # item_loader.add_value("energy_label", energy_label)  # String

            # # LandLord Details
            item_loader.add_value(
                "landlord_name", 'Mr. Marc Schindler')  # String
            item_loader.add_value(
                "landlord_phone", '01727313100')  # String
            item_loader.add_value(
                "landlord_email", 'schindler1910@aol.com')  # String

            self.position += 1
            yield item_loader.load_item()

    def get_features_from_description(self, description, response, item_loader):
        description = description.lower()
        pets_allowed = 'pets' in description
        furnished = 'furnish' in description
        parking = 'parking' in description or 'garage' in description
        elevator = 'elevator' in description
        balcony = 'balcon' in description
        terrace = 'terrace' in description
        swimming_pool = 'pool' in description
        washing_machine = 'laundry' in description
        dishwasher = 'dishwasher' in description

        item_loader.add_value("pets_allowed", pets_allowed)  # Boolean
        item_loader.add_value("furnished", furnished)  # Boolean
        item_loader.add_value("parking", parking)  # Boolean
        item_loader.add_value("elevator", elevator)  # Boolean
        item_loader.add_value("balcony", balcony)  # Boolean
        item_loader.add_value("terrace", terrace)  # Boolean
        item_loader.add_value("swimming_pool", swimming_pool)  # Boolean
        item_loader.add_value("washing_machine", washing_machine)  # Boolean
        item_loader.add_value("dishwasher", dishwasher)  # Boolean
        return pets_allowed, furnished, parking, elevator, balcony, terrace, swimming_pool, washing_machine, dishwasher

    def get_property_type(self, property_type, description):

        if property_type and ('appartamento' in property_type.lower() or 'appartamento' in description.lower()):
            property_type = 'apartment'
        elif property_type and 'ufficio' in property_type.lower():
            property_type = ""
        else:
            if not property_type:
                property_type = ''
            else:
                property_type = 'house'
        return property_type
