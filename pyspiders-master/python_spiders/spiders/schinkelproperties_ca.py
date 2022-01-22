import requests
import scrapy
from scrapy import Request

from ..helper import extract_number_only, remove_unicode_char
from ..items import ListingItem


class SchinkelPropertiesSpider(scrapy.Spider):
    name = 'schinkelproperties_com'
    start_urls = ['https://schinkelproperties.com/apartments/']
    country = 'Canada'
    locale = 'en'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'

    def parse(self, response):

        for start_url in self.start_urls:
            yield Request(url=start_url,
                          callback=self.parse_area)

    def parse_area(self, response):
        rentals = response.css('.property-block')
        for rental in rentals:
            if rental.css('.property-block-status .available'):
                area_url = rental.css("::attr(href)").extract()
                area_url = area_url[0]
                yield Request(url=area_url,
                              callback=self.parse_area_pages)

        #
        # for area_url in available:
        #     yield Request(url=area_url,
        #                   callback=self.parse_area_pages)

    def parse_area_pages(self, response):
        contact_list = {'Judi Reimer': 'judi@schinkelproperties.com',
                        'Leah Morin': 'leah@schinkelproperties.com',
                        'Sabrina Lewis': 'sabrina@schinkelproperties.com',
                        'Audrey Harder': 'audrey@schinkelproperties.com',
                        'Agnes Gusowski': 'agnes@schinkelproperties.com',
                        'Kim Holowczak': 'kimberly@schinkelproperties.com',
                        'Ashley Meilleur': 'ashley@schinkelproperties.com',
                        'Krista Oar':  'krista@schinkelproperties.com',
                        'Lindsay Budey':  'lindsay@schinkelproperties.com',
                        'Crystal Funk':  'crystal@schinkelproperties.com',
                        'Dani Schulz':  'dani@schinkelproperties.com'




                        }
        # types = ["apartment", "house", "room", "student_apartment", "studio"]
        item = ListingItem()
        # Write your code here and remove `pass` in the following line
        bathroom_count = response.css('.vcenter~ .vcenter+ .vcenter p::text').extract()
        bathroom_count = int(bathroom_count[0])
        square_meters = response.css('.vcenter:nth-child(1) p::text').extract_first()
        square_meters = square_meters.split('-')
        square_meters_arr = []
        square_meters_int = 0
        for i in square_meters:
            square_meters_arr.append(extract_number_only(i))
        for i in square_meters_arr:
            square_meters_int += int(i)
        square_meters_int = square_meters_int / len(square_meters_arr)
        square_meters = int(square_meters_int / 10.7639)
        room_count = response.css('.vcenter:nth-child(2) p::text').extract()
        room_count = int(room_count[0])
        rent = response.css('.single-apartment-container h3::text').extract()
        rent = rent[0].split('/')[0]
        rent = rent[1:]
        rent = rent.split('-')
        rent_arr = []
        rent_int = 0
        for i in rent:
            rent_arr.append(extract_number_only(i))
        for i in rent_arr:
            rent_int += int(i)
        rent_int = rent_int / len(rent_arr)
        rent = int(rent_int)
        images = response.css('.single-apartment-gallery .fancybox::attr(href)').extract()

        address_block = response.css('.single-apartment-infoblock~ .single-apartment-infoblock+ .single-apartment-infoblock p::text').extract()
        if address_block: #getting address from address block
            address_arr_filtered = []
            address = ''
            for i in address_block:
                remove_unicode_char(i)
                address_arr_filtered.append(remove_unicode_char(i))
            for i in address_arr_filtered:
                if i:
                    address += i + " "
            responseGeocode = requests.get(
                f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/find?f=json&text={address}&maxLocations=1")
            responseGeocodeData = responseGeocode.json()

            longitude = responseGeocodeData['locations'][0]['feature']['geometry']['x']
            latitude = responseGeocodeData['locations'][0]['feature']['geometry']['y']

            responseGeocode = requests.get(
                f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
            responseGeocodeData = responseGeocode.json()
            zipcode = responseGeocodeData['address']['Postal']
            city = responseGeocodeData['address']['City']
            address = responseGeocodeData['address']['Match_addr']

            longitude = str(longitude)
            latitude = str(latitude)
        else:
            title_address = ((response.css("h1::text").extract_first()).split(":"))[0]
            responseGeocode = requests.get(
                f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/find?f=json&text={title_address}&maxLocations=1")
            responseGeocodeData = responseGeocode.json()

            longitude = responseGeocodeData['locations'][0]['feature']['geometry']['x']
            latitude = responseGeocodeData['locations'][0]['feature']['geometry']['y']

            responseGeocode = requests.get(
                f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
            responseGeocodeData = responseGeocode.json()
            zipcode = responseGeocodeData['address']['Postal']
            city = responseGeocodeData['address']['City']
            address = responseGeocodeData['address']['Match_addr']

            longitude = str(longitude)
            latitude = str(latitude)



        landlord = response.css('.single-apartment-infoblock:nth-last-child(1)')
        landlord_name = landlord.css('p::text').get()
        landlord_phone = response.css('.telephone-link::text').extract()
        landlord_phone = landlord_phone[0]

        description = response.css('.single-apartment-description p::text').extract()

        item['external_link'] = response.request.url
        item['external_source'] = self.external_source

        paragraph = ''
        if len(description) != 1:
            for sentence in description:
                paragraph += sentence
            item['description'] = paragraph
        else:
            description = description[0]
            item['description'] = description


        features_arr = response.css('.single-apartment-container+ .single-apartment-infoblock p::text').extract()
        features_arr_filtered = []
        features= ''
        for i in features_arr:
            remove_unicode_char(i)
            features_arr_filtered.append(remove_unicode_char(i))
        for i in features_arr_filtered:
            if i:
                features += i + " "
        features = features.lower()

        details_arr = response.css('.single-apartment-infoblock+ .single-apartment-infoblock p::text').extract()
        details_arr_filtered = []
        details= ''
        for i in details_arr:
            remove_unicode_char(i)
            details_arr_filtered.append(remove_unicode_char(i))
        for i in details_arr_filtered:
            if i:
                details += i + " "
        details = details.lower()
        print('features',features)
        print('type', type(features))
        print('details',details)
        print('type', type(details))
        if ("park" in details) or ('park' in features):
            item['parking'] = True
        else:
            item['parking'] = False

        if ("balcony" in details) or ('balcony' in features):
            item['balcony'] = True
        else:
            item['balcony'] = False

        if ("park" in details) or ('park' in features):
            item['parking'] = True
        else:
            item['parking'] = False


        if ('elevator' in features):
            item['elevator'] = True
        else:
            item['elevator'] = False

        if (" washer" in details) or (' washer' in features):
            item['washing_machine'] = True
        else:
            item['washing_machine'] = False

        if ("dishwasher" in details) or ('dishwasher' in features):
            item['dishwasher'] = True
        else:
            item['dishwasher'] = False

        if "pet" in features:
            item['pets_allowed'] = True
            item['deposit'] = int(rent/2)
        else:
            item['pets_allowed'] = False
            item['deposit'] = 0


        item['address'] = address
        item['zipcode'] = zipcode
        item['city'] = city
        item['longitude'] = longitude
        item['latitude'] = latitude

        item['title'] = response.css('h1::text').extract_first()
        item['property_type'] = 'apartment'
        item['square_meters'] = int(int(square_meters*10.764))
        item['room_count'] = room_count
        item['bathroom_count'] = bathroom_count
        item['images'] = images
        item['external_images_count'] = len(images)
        item['rent'] = rent
        item['currency'] = 'CAD'


        # item['landlord_email'] = landlord_email

        if landlord_name:
            landlord_name = landlord_name[1:]
            item['landlord_name'] = landlord_name
            landlord_email = contact_list.get(landlord_name)
            item['landlord_email'] = landlord_email
        else:
            item['landlord_name'] = "Schinkel Properties"

        item['landlord_phone'] = landlord_phone


        yield item
