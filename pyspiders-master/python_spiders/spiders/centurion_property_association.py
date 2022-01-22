import scrapy
from scrapy import Request
from ..items import ListingItem
import json
import dateparser
import datetime
import requests

class CenturionPropertyAssoctiation(scrapy.Spider):
    name = 'centurion_property'
    allowed_domains = ['cpliving.com']
    start_urls = ['https://www.cpliving.com/apartments/mayfair-on-jasper']
    # start_urls = ['https://www.cpliving.com/apartments/137-141-woodside-avenue-apartments']
    execution_type = 'development'
    country = 'canada'
    locale ='en'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)

    def start_requests(self):
        yield Request(url='https://api.theliftsystem.com/v2/search?local_url_only=true&client_id=21&auth_token=sswpREkUtyeYjeoahA2i&city_ids=14,161,347,408,415,709,845,902,1136,1154,1287,1373,1425,1607,1837,1863,2081,2084,2310,2356,2566,3044,3133,3227,3284,3339,3377,8425,28850,28870,32939&geocode=&min_bed=-1&max_bed=5&min_bath=0&max_bath=10&min_rate=0&max_rate=10000&region=&keyword=&order=min_rate+ASC,+max_rate+ASC,+min_bed+ASC,+max_bed+ASC&limit=100&offset=0&count=false&show_custom_fields=true&show_amenities=true&show_all_properties=true&only_available_suites=false',
                    callback=self.parse,
                    body='',
                    method='GET')


    def parse(self, response):
        parsed_response = json.loads(response.body)
        for item in parsed_response:
            url = item['permalink']
            yield Request(url=url, callback=self.parse_ad)



    # def parse(self, response):
    #     for start_url in self.start_urls:
    #         yield Request(url=start_url,
    #                       callback=self.parse_ad)
    #


    def parse_ad(self, response):
        items = ListingItem()

        external_link = str(response.request.url)
        title = response.css(".property-title::text").extract()
        title = title[0].strip()
        address = response.css(".address::text").extract()
        address = address[0].strip()
        rent = None
        room_count = None
        bathroom_count = None
        elevator = False
        pets_allowed = False
        parking = False
        balcony = False
        elevator = False
        terrace = False
        swimming_pool = False
        dishwasher = False
        washing_machine = False

        description = response.css(".half p:nth-child(1)::text").extract()
        if not description:
            description = response.css("p span:nth-child(1) span span > span span span span::text").extract()
        if not description:
            description = response.css("p:nth-child(3) span , p:nth-child(2) span::text").extract()
        if not description:
            description = response.css(".half p:nth-child(2)::text").extract()
        description = description[0]

        amenities = response.css(".amenity::text").extract()
        if 'Elevators' in amenities:
            elevator = True
        if 'Dishwasher' in amenities:
            dishwasher = True
        if 'Laundry facilities' in amenities:
            washing_machine = True
        if 'Balconies' in amenities:
            balcony = True
        if 'parking' in amenities:
            parking = True

        city = None
        longitude = None
        latitude = None
        zipcode = None

        try:
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
        except:
            pass

        landlord_phone = response.css(".cms-content a::text").extract()
        landlord_phone = landlord_phone[0]
        if any(char.isdigit() for char in landlord_phone):
            landlord_phone = ''.join(x for x in landlord_phone if x.isdigit())
        else:
            landlord_phone = " "

        images = response.css(".gallery-image::attr(href)").extract()

        try:
            columns = response.css(".suite-rate .big::text").extract()
            count = len(columns)
            for column in range(count):
                rent = response.css(".suite-rate .big::text")[column].extract()
                if any(char.isdigit() for char in rent):
                    rent = int(''.join(x for x in rent if x.isdigit()))
                room_count = response.css(".bedroom::text")[column].extract()
                if any(char.isdigit() for char in room_count):
                    room_count = ''.join(x for x in room_count if x.isdigit())
                    if len(room_count) > 1:
                        square_meters = int(room_count[1:])
                        square_meters = int(square_meters / 10.7639)
                    else:
                        square_meters = 0
                    room_count = int(room_count[0])
                else:
                    room_count = 1
                bathroom_count = response.css(".bathroom span::text")[column].extract()
                if any(char.isdigit() for char in bathroom_count):
                    bathroom_count = ''.join(x for x in bathroom_count if x.isdigit())
                    bathroom_count = int(bathroom_count[0])
                else:
                    bathroom_count = 1

                    available_date = " "
                try:
                    available_date = response.xpath('//*[@id="content"]/div/div[5]/div/div[1]/section/div[2]/section[1]/div/div/div/div[3]/span[2]').css("::text")[column].extract()
                    available_date = available_date.strip()
                    available_date = dateparser.parse(available_date)
                    available_date = available_date.strftime("%Y-%m-%d")
                except:
                    available_date = " "





                items['external_source'] = self.external_source
                items['external_link'] = external_link
                items['title'] = title
                items['description'] = description
                items['address'] = address
                if city:
                    items['city'] = city
                if zipcode:
                    items['zipcode'] = zipcode
                if latitude:
                    items['latitude'] = latitude
                if longitude:
                    items['longitude'] = longitude
                items['property_type'] = 'apartment'
                items['rent'] = rent
                items['currency'] = 'CAD'
                if square_meters != 0:
                    items['square_meters'] = int(int(square_meters*10.764))
                if available_date != " ":
                    items['available_date'] = available_date
                items['room_count'] = room_count
                items['bathroom_count'] = bathroom_count
                items['address'] = address
                items['property_type'] = 'apartment'
                items['elevator'] = elevator
                items['dishwasher'] = dishwasher
                items['washing_machine'] = washing_machine
                items['balcony'] = balcony
                items['parking'] = parking
                items['landlord_name'] = 'Centurion Property Associates'
                items['landlord_phone'] = landlord_phone

                items['images'] = images

                yield items
        except:
            return
