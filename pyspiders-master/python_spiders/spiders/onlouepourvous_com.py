from math import ceil

import requests
import scrapy
from scrapy import Request

from ..helper import extract_number_only, sq_feet_to_meters, remove_unicode_char, remove_white_spaces
from ..loaders import ListingLoader


class OnlouepourvousComSpider(scrapy.Spider):
    name = 'onlouepourvous_com'
    allowed_domains = ['onlouepourvous.com']
    start_urls = [
        'https://onlouepourvous.com/advanced-search/?adv_search_nonce_field=1bf2052b58&_wp_http_referer=%2Fadvanced-search%2F%3Ffilter_search_action%255B%255D%3D%26filter_search_type%255B%255D%3D%26advanced_city%3D%26advanced_area%3D%26advanced_rooms%3D1%26advanced_bath%3D%26price_low%3D%26price_max%3D%26lang%3Den&lang=en&filter_search_action%5B%5D=&filter_search_type%5B%5D=&advanced_city=&advanced_area=&advanced_rooms=&advanced_bath=&price_low=&price_max=&submit=SEARCH']
    country = 'canada'
    locale = 'en'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'development'
    all_rentals = []
    current_page = 1

    def parse(self, response):
        for start_url in self.start_urls:
            yield Request(url=start_url,
                          callback=self.parse_area)

    def parse_area(self, response):
        rentals_count = response.css('.title_prop::text').extract_first()
        rentals_count = rentals_count.split('(')
        rentals_count = rentals_count[-1]
        rentals_count = rentals_count[:-1]
        pages = ceil(int(rentals_count) / 6)
        if self.current_page != pages:
            rental_box = response.css('.property_listing')
            for rentals in rental_box:
                sold_state = rentals.css(
                    'div > div.listing-unit-img-wrapper > div.tag-wrapper > div > div::text').extract_first()
                if sold_state is None:
                    rental_link = rentals.css('h4 a::attr(href)').extract_first()
                    self.all_rentals.append(rental_link)
            self.current_page += 1
            next_page_url = 'https://onlouepourvous.com/advanced-search/page/' + str(
                self.current_page) + '/?adv_search_nonce_field=1bf2052b58&_wp_http_referer=%2Fadvanced-search%2F%3Ffilter_search_action%255B%255D%3D%26filter_search_type%255B%255D%3D%26advanced_city%3D%26advanced_area%3D%26advanced_rooms%3D1%26advanced_bath%3D%26price_low%3D%26price_max%3D%26lang%3Den&lang=en&filter_search_action%5B0%5D&filter_search_type%5B0%5D&advanced_city&advanced_area&advanced_rooms&advanced_bath&price_low&price_max&submit=SEARCH'
            yield Request(url=next_page_url,
                          callback=self.parse_area,
                          dont_filter=True)
        else:
            rental_box = response.css('.property_listing')
            for rentals in rental_box:
                sold_state = rentals.css(
                    'div > div.listing-unit-img-wrapper > div.tag-wrapper > div > div::text').extract_first()
                if sold_state is None:
                    rental_link = rentals.css('h4 a::attr(href)').extract_first()
                    self.all_rentals.append(rental_link)
            for rental in self.all_rentals:
                yield Request(url=rental,
                              callback=self.parse_area_pages)

    def parse_area_pages(self, response):
        item_loader = ListingLoader(response=response)
        property_details_values = response.css('#collapseOne .col-md-4::text').extract()
        property_details_values = [i for i in property_details_values if i != " "]
        property_details_keys = response.css('#collapseOne .col-md-4 strong::text').extract()
        property_details = zip(property_details_keys, property_details_values)
        property_details = dict(property_details)
        if 'Price:' in property_details.keys():
            external_link = response.url
            external_id = response.css('#propertyid_display::text').extract_first()
            external_source = self.external_source
            title = response.css('.entry-prop::text').extract_first()
            description = remove_unicode_char(remove_white_spaces((" ".join(response.css('#property_description p ::text').extract())).replace('\n', '')))
            property_address_values = response.css('#collapseTwo .col-md-4::text').getall()
            property_address_keys = response.css('#collapseTwo .col-md-4 strong::text').extract()
            property_address = zip(property_address_keys, property_address_values)
            property_address = dict(property_address)

            address = property_address.get('Address:')
            zipcode = property_address.get('Zip:')
            responseGeocode = requests.get(
                f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/find?f=json&text={address}&maxLocations=1")
            responseGeocodeData = responseGeocode.json()

            try:
                longitude = responseGeocodeData['locations'][0]['feature']['geometry']['x']
                latitude = responseGeocodeData['locations'][0]['feature']['geometry']['y']

                responseGeocode = requests.get(
                    f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
                responseGeocodeData = responseGeocode.json()
                # zipcode = responseGeocodeData['address']['Postal']
                city = responseGeocodeData['address']['City']
                if city is None:
                    city = ''
                address = responseGeocodeData['address']['Match_addr']
                longitude = str(longitude)
                latitude = str(latitude)
            except:
                longitude = ''
                latitude = ''
                city = ''
                address = property_address.get('Address:')

            if 'villa' in title.lower():
                property_type = 'house'
            else:
                property_type = 'apartment'
                square_meters = 1
                room_count = 1
                bathroom_count = 1
            try:
                room_count = int(extract_number_only(property_details['Bedrooms:']))
                square_meters = sq_feet_to_meters(int(extract_number_only(property_details['Property Size:'])))
                bathroom_count = int(extract_number_only(property_details['Bathrooms:']))
            except:
                    pass
            if room_count == 0:
                room_count = 1
            available_date = None
            images = response.css('#carousel-listing .item a::attr(href)').extract()
            floor_plan_images = None
            external_images_count = len(images)
            rent = int(extract_number_only(property_details['Price:']))
            currency = "CAD"
            deposit = None
            prepaid_rent = None
            utilities = None
            water_cost = None
            heating_cost = None
            energy_label = None

            if 'pet' in description.lower():
                pets_allowed = True
            else:
                pets_allowed = False

            if 'furnish' in description.lower():
                furnished = True
            else:
                furnished = False

            floor = None
            count = 0
            desc_arr = description.split(" ")
            ordinal = lambda n: "%d%s" % (n, "tsnrhtdd"[(n // 10 % 10 != 1) * (n % 10 < 4) * n % 10::4])
            ordinal_no = [ordinal(n) for n in range(1, 101)]
            for i in desc_arr:
                if 'floor' in i:
                    if desc_arr[count - 1] in ordinal_no:
                        floor = desc_arr[count - 1], i
                        break
                    else:
                        pass
                else:
                    count += 1
            if floor is None:
                floor = '1st floor'

            if 'parking' in description.lower():
                parking = True
            else:
                parking = False

            elevator = None

            if 'balcony' in description.lower():
                balcony = True
            else:
                balcony = False

            if 'terrace' in description.lower() or 'terrace' in title.lower():
                terrace = True
            else:
                terrace = False

            swimming_pool = None

            if ' washer' in description.lower():
                washing_machine = True
            else:
                washing_machine = False

            if 'dishwasher' in description.lower():
                dishwasher = True
            else:
                dishwasher = False
            landlord_name = 'on loue pour vous'
            landlord_phone = '438-324-0618'
            landlord_email = 'info@onlouepourvous.com'

            # --------------------------------#
            # item loaders
            item_loader.add_value('external_link', external_link)
            item_loader.add_value('external_id', external_id)
            item_loader.add_value('external_source', external_source)
            item_loader.add_value('title', title)
            item_loader.add_value('description', description)
            item_loader.add_value('city', city)
            item_loader.add_value('zipcode', zipcode)
            item_loader.add_value('address', address)
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)
            item_loader.add_value('property_type', property_type)
            item_loader.add_value('square_meters', int(int(square_meters)*10.764))
            item_loader.add_value('room_count', room_count)
            item_loader.add_value('bathroom_count', bathroom_count)
            item_loader.add_value('available_date', available_date)
            item_loader.add_value("images", images)
            item_loader.add_value("floor_plan_images", floor_plan_images)
            item_loader.add_value("external_images_count", external_images_count)
            item_loader.add_value("rent", rent)
            item_loader.add_value("currency", currency)
            item_loader.add_value("deposit", deposit)
            item_loader.add_value("prepaid_rent", prepaid_rent)
            item_loader.add_value("utilities", utilities)
            item_loader.add_value("water_cost", water_cost)
            item_loader.add_value("heating_cost", heating_cost)
            item_loader.add_value("energy_label", energy_label)
            item_loader.add_value("pets_allowed", pets_allowed)
            item_loader.add_value("furnished", furnished)
            item_loader.add_value("floor", floor)
            item_loader.add_value("parking", parking)
            item_loader.add_value("elevator", elevator)
            item_loader.add_value("balcony", balcony)
            item_loader.add_value("terrace", terrace)
            item_loader.add_value("swimming_pool", swimming_pool)
            item_loader.add_value("washing_machine", washing_machine)
            item_loader.add_value("dishwasher", dishwasher)
            item_loader.add_value("landlord_name", landlord_name)
            item_loader.add_value("landlord_email", landlord_email)
            item_loader.add_value("landlord_phone", landlord_phone)

            yield item_loader.load_item()
