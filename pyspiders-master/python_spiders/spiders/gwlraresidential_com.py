# -*- coding: utf-8 -*-
# Author: Adham Mansour
import scrapy
from scrapy import Request

from ..helper import remove_unicode_char, extract_number_only ,extract_location_from_address, extract_location_from_coordinates
from ..loaders import ListingLoader
import requests
from bs4 import BeautifulSoup


class GwlraresidentialComSpider(scrapy.Spider):
    name = 'gwlraresidential_com'
    allowed_domains = ['gwlraresidential.com']
    start_urls = ['https://www.gwlraresidential.com/searchlisting']  # https not http
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
        rentals = response.css('.prop-content')
        for rental in rentals:
            external_link = rental.css('.propertyUrl::attr(href)').extract_first()
            title = rental.css('.propertyUrl::text').extract_first()
            min_rent = rental.css('.propertyMinRent::text').extract_first()
            max_rent = rental.css('.propertyMaxRent::text').extract_first()
            if min_rent is not None or max_rent is not None:
                if min_rent is None:
                    rent = int(max_rent)
                else:
                    rent = int(float((int(float(min_rent))+int(float(max_rent)))/2))
                apartment_type = 'apartment'
                min_room_count = rental.css('.propertyMinBed::text').extract_first()
                max_room_count = rental.css('.propertyMaxBed::text').extract_first()
                if not min_room_count.isnumeric():
                    if min_room_count.lower() == 'studio':
                        apartment_type = 'studio'
                    room_count = max_room_count
                else:
                    room_count = int(float((int(float(min_room_count))+int(float(max_room_count)))/2))
                min_bathroom_count = rental.css('.propertyMinBath::text').extract_first()
                max_bathroom_count = rental.css('.propertyMaxBath::text').extract_first()
                if min_bathroom_count is None or max_bathroom_count is None:
                    if min_bathroom_count is None:
                        bathroom_count = int(float(max_bathroom_count))
                    elif max_bathroom_count is None:
                        bathroom_count = int(float(min_bathroom_count))
                else:
                    bathroom_count = int(float((int(float(min_bathroom_count)) + int(float(max_bathroom_count))) / 2))
                address = ' '.join((rental.css('.prop-address span::text').extract())[:-2])
                longitude, latitude = extract_location_from_address(address)
                zipcode = rental.css('.propertyZipCode::text').extract_first()
                city = rental.css('.propertyCity::text').extract_first()
                landlord_phone = rental.css('.prop-call-us::text').extract_first()
                # address = address, city, rental.css('.propertyState::text').extract_first(), zipcode
                yield Request(url=external_link,
                              callback=self.populate_item,
                              meta= {
                                  'external_link' : external_link,
                                  'rent' : rent,
                                  'property_type' : apartment_type,
                                  'room_count': room_count,
                                  'bathroom_count' : bathroom_count,
                                  'longitude' : str(longitude),
                                  'latitude' : str(latitude),
                                  'zipcode' : zipcode,
                                  'city' : city,
                                  'landlord_phone' : landlord_phone,
                                  'title' :title,
                                  'address' : address
                              }
                              )
            if not(response.css('#paginationTop li:nth-last-child(1) a ::text').extract_first()).isnumeric():
                next_page_no = response.css('#paginationTop li:nth-last-child(1) a ::attr(onclick)').extract_first()
                next_page_no = extract_number_only(next_page_no)
                yield Request(callback=self.parse,
                              url='https://www.gwlraresidential.com/searchlisting.aspx?ftst=&cmbBeds=-1&cmbBeds1=-1&cmbBeds2=-1&cmbBeds3=-1&cmbBeds4=-1&cmbBeds5=-1&cmbBaths=-1&cmbBaths2=-1&cmbBaths3=-1&cmbBaths4=-1&cmbBaths5=-1&cmb_PetPolicy=-1&LocationGeoId=0&zoom=10&autoCompleteCorpPropSearchlen=3&renewpg=1&PgNo='+next_page_no+'&LatLng=(56.130366,-106.346771)&')

    # 3. SCRAPING level 3
    def populate_item(self, response):
        if 'www.gwlraresidential.com' in response.url:
            item_loader = ListingLoader(response=response)
            description =remove_unicode_char((((' '.join(response.css('#InnerContentDiv ::text').extract()).replace('\n','')).replace('\t','')).replace('\r','')))
            description_lower = description.lower()
            images = []
            images_url = (response.url).replace('index.aspx','photogallery.aspx')
            url_get = requests.get(images_url)
            soup = BeautifulSoup(url_get.content, 'lxml')
            col = soup.find_all('div', class_="photoGalleryColumn")
            for photo in col:
                col_all = photo.find_all('a')
                for link in col_all:
                    images.append(link.get('href'))
            images = [(image.split('?'))[0] for image in images]
            images = list(dict.fromkeys(images))
            if response.css('.rightcontent .btn-fancy') is not None:
                amenities = []
                amenities_url = (response.url).replace('index.aspx','amenities.aspx')
                url_get = requests.get(amenities_url)
                soup = BeautifulSoup(url_get.content, 'lxml')
                col = soup.find_all('ul', class_="amenities-list")
                for item in col:
                    col_all = item.find_all('li')
                    for link in col_all:
                        amenities.append((link.getText()).lower())
                amenities = ' '.join(amenities)

            pets_allowed = None
            if ('pet' in description_lower) or ('pet' in amenities):
                pets_allowed = True


            furnished = None
            if ('furnish' in description_lower) or ('furnish' in amenities):
                furnished = True

            parking = None
            if ('parking' in description_lower) or ('parking' in amenities):
                parking = True

            elevator = None
            if ('elevator' in description_lower) or ('elevator' in amenities):
                elevator = True

            balcony = None
            if ('balcony' in description_lower) or ('balcony' in amenities):
                balcony = True

            terrace = None
            if ('terrace' in description_lower) or ('terrace' in amenities):
                terrace = True

            swimming_pool = None
            if ('pool' in description_lower) or ('pool' in amenities):
                swimming_pool = True

            washing_machine = None
            if ('laundry' in description_lower) or ('laundry' in amenities):
                washing_machine = True

            dishwasher = None
            if ('dishwasher' in description_lower) or ('dishwasher' in amenities):
                dishwasher = True


            # # MetaData
            item_loader.add_value("external_link", response.url)  # String
            item_loader.add_value("external_source", self.external_source)  # String

            # item_loader.add_value("external_id", external_id) # String
            item_loader.add_value("position", self.position)  # Int
            item_loader.add_value("title", response.meta['title']) # String
            item_loader.add_value("description", description) # String

            # # Property Details
            item_loader.add_value("city", response.meta['city']) # String
            item_loader.add_value("zipcode", response.meta['zipcode']) # String
            item_loader.add_value("address", response.meta['address']) # String
            item_loader.add_value("latitude", response.meta['latitude']) # String
            item_loader.add_value("longitude", response.meta['longitude']) # String
            # item_loader.add_value("floor", floor) # String
            item_loader.add_value("property_type", response.meta['property_type']) # String
            # item_loader.add_value("square_meters", square_meters) # Int
            item_loader.add_value("room_count", response.meta['room_count']) # Int
            item_loader.add_value("bathroom_count", response.meta['bathroom_count']) # Int

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
            # item_loader.add_value("floor_plan_images", floor_plan_images) # Array

            # # Monetary Status
            item_loader.add_value("rent", response.meta['rent']) # Int
            # item_loader.add_value("deposit", deposit) # Int
            # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
            # item_loader.add_value("utilities", utilities) # Int
            item_loader.add_value("currency", "CAD") # String

            # item_loader.add_value("water_cost", water_cost) # Int
            # item_loader.add_value("heating_cost", heating_cost) # Int

            # item_loader.add_value("energy_label", energy_label) # String

            # # LandLord Details
            item_loader.add_value("landlord_name", 'gwlra residential') # String
            item_loader.add_value("landlord_phone", response.meta['landlord_phone']) # String
            # item_loader.add_value("landlord_email", landlord_email) # String

            self.position += 1
            yield item_loader.load_item()
