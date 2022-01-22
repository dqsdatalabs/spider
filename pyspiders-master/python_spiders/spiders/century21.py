# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
import scrapy
import re
import json
import js2xml
from ..loaders import ListingLoader
import math
from datetime import date
from ..helper import remove_unicode_char, extract_rent_currency, format_date


def extract_city_zipcode(_address):
    if ", " in _address:
        zip_city = _address.split(", ")[1]
    else:
        zip_city = _address
    zipcode = zip_city.split(" ")[0]
    city = re.sub(r"\d+", "", zip_city).strip()
    return zipcode, city


class Century21Spider(scrapy.Spider):

    name = 'century21viaplus_be'
    allowed_domains = ['century21viaplus.be', 'c21advies.omnicasaweb.com']
    start_urls = ['https://www.century21viaplus.be/']
    execution_type = 'testing'
    country = 'belgium'
    locale = 'nl'
    thousand_separator = ','
    scale_separator = '.'
    
    def start_requests(self):
        start_urls = [
            {'url': 'https://api.prd.cloud.century21.be/api/v2/properties?facets=elevator%2Ccondition%2CfloorNumber%2Cgarden%2ChabitableSurfaceArea%2ClistingType%2CnumberOfBedrooms%2Cparking%2Cprice%2CsubType%2CsurfaceAreaGarden%2CswimmingPool%2Cterrace%2CtotalSurfaceArea%2Ctype&filter=eyJib29sIjp7ImZpbHRlciI6eyJib29sIjp7Im11c3QiOlt7Im1hdGNoIjp7Imxpc3RpbmdUeXBlIjoiRk9SX1JFTlQifX0seyJib29sIjp7InNob3VsZCI6eyJtYXRjaCI6eyJ0eXBlIjoiSE9VU0UifX19fSx7InJhbmdlIjp7ImNyZWF0aW9uRGF0ZSI6eyJsdGUiOiIyMDIwLTEwLTEzVDE0OjQyOjQ3LjQ3NCJ9fX1dfX19fQ%3D%3D&pageSize=300&sort=-creationDate',
                'property_type': 'house'},
            {'url': 'https://api.prd.cloud.century21.be/api/v2/properties?facets=elevator%2Ccondition%2CfloorNumber%2Cgarden%2ChabitableSurfaceArea%2ClistingType%2CnumberOfBedrooms%2Cparking%2Cprice%2CsubType%2CsurfaceAreaGarden%2CswimmingPool%2Cterrace%2CtotalSurfaceArea%2Ctype&filter=eyJib29sIjp7ImZpbHRlciI6eyJib29sIjp7Im11c3QiOlt7Im1hdGNoIjp7Imxpc3RpbmdUeXBlIjoiRk9SX1JFTlQifX0seyJib29sIjp7InNob3VsZCI6eyJtYXRjaCI6eyJ0eXBlIjoiQVBBUlRNRU5UIn19fX0seyJyYW5nZSI6eyJjcmVhdGlvbkRhdGUiOnsibHRlIjoiMjAyMC0xMC0xM1QxNDo1NDoyNy42MSJ9fX1dfX19fQ%3D%3D&pageSize=1000&sort=-creationDate',
                'property_type': 'apartment'},
            {'url': 'https://api.prd.cloud.century21.be/api/v2/properties?facets=elevator,condition,floorNumber,garden,habitableSurfaceArea,listingType,numberOfBedrooms,parking,price,subType,surfaceAreaGarden,swimmingPool,terrace,totalSurfaceArea,type&filter=eyJib29sIjp7ImZpbHRlciI6eyJib29sIjp7Im11c3QiOlt7Im1hdGNoIjp7Imxpc3RpbmdUeXBlIjoiRk9SX1JFTlQifX0seyJyYW5nZSI6eyJjcmVhdGlvbkRhdGUiOnsibHRlIjoiMjAyMC0wOS0zMFQwODo1NDoyOC43NjMifX19XX19fX0=&pageSize=800&sort=-creationDate',
                'property_type': 'null'},
        ]
        for url in start_urls:
            yield scrapy.Request(
                url=url.get('url'),
                callback=self.parse,
                meta={'property_type': url.get('property_type')},
                dont_filter=True
            )
    
    def parse(self, response,  **kwargs):
        datas = json.loads(response.text)
        for data in datas['data']:
            link_id = data['id']
            if 'reference' in data:
                external_id = data['reference']
            elif 'reference' not in data:
                external_id = ''
            if 'fr' in data['title']:
                title = data['title']['fr']
            elif 'nl' in data['title']:
                 title = data['title']['nl']
            if 'fr' in data['description']: 
                description = data['description']['fr']
            elif 'nl' in data['description']:
                description = data['description']['nl']
            if 'price' in data:
                rent = str(data['price']['amount']) + '€'
            if 'surface' in data:    
                if 'habitableSurfaceArea' in data['surface']:
                    if int(data['surface']['habitableSurfaceArea']['value']) > 0:
                        square_meters = str(math.ceil(data['surface']['habitableSurfaceArea']['value']))
                    else:
                        square_meters = ''
                else:
                    square_meters = ''
            if 'rooms' in data:    
                if 'numberOfBedrooms' in data['rooms']: 
                    if int(data['rooms']['numberOfBedrooms']) > 0:
                        room_count = str(data['rooms']['numberOfBedrooms']) 
                    else:
                        room_count = ''
                else:
                    room_count = ''
            elif 'rooms' not in data:
                room_count = ''
            if 'rooms' in data:
                if 'numberOfBathrooms' in data['rooms']:
                    if int(data['rooms']['numberOfBathrooms']) > 0:
                        bathroom_count = str(data['rooms']['numberOfBathrooms'])
                    else:
                        bathroom_count = ''
                else:
                    bathroom_count = ''
            elif 'rooms' not in data:
                bathroom_count = ''
            city = data['address']['city']
            zipcode = data['address']['postalCode']
            if 'street' in data['address']:
                street_v = data['address']['street'] 
            address = street_v + ' ' + city + ' ' + zipcode 
            try:
                latitude = data['location']['latitude']
                longitude = data['location']['longitude']
            except:
                latitude = longitude = '' 
            if 'floorNumber' in data:
                floor = data['floorNumber']
            else:
                floor = ''
            if 'parking' in data['amenities']: 
                parking_text = data['amenities']['parking']
                if 'true' in str(parking_text):
                    parking = True
                else:
                    parking = ''
            else:
                parking = ''
            if 'terrace' in data['amenities']: 
                terrace_text = data['amenities']['terrace']
                if 'true' in str(terrace_text):
                    terrace = True
                else:
                    terrace = ''
            else:
                terrace = ''
            if 'swimmingPool' in data['amenities']: 
                swimmingPool_text = data['amenities']['swimmingPool']
                if 'true' in str(swimmingPool_text):
                    swimming_pool = True
                else:
                    swimming_pool = ''
            else:
                swimming_pool = ''
            if 'elevator' in data['amenities']: 
                elevator_text = data['amenities']['elevator']
                if 'true' in str(elevator_text):
                    elevator = True
                else:
                    elevator = ''
            else:
                elevator = ''

            property_type = response.meta.get('property_type')
            if property_type == 'null':
                p_type = data['type']

                if ("student" in p_type.lower() or "étudiant" in p_type.lower() or  "studenten" in p_type.lower()) and ("apartment" in p_type.lower() or "appartement" in p_type.lower()):
                    property_type = "student_apartment"
                elif "appartement" in p_type.lower() or "apartment" in p_type.lower():
                    property_type ="apartment"
                    property_url = 'appartement'
                elif "woning" in p_type.lower() or "maison" in p_type.lower() or "huis" in p_type.lower() or "house" in p_type.lower():
                    property_type = "house"
                    property_url = 'huis'
                elif "chambre" in p_type.lower() or "kamer" in p_type.lower() or "room" in p_type.lower():
                    property_type = "room"
                elif "studio" in p_type.lower():
                    property_type = "studio"
            elif 'house' in property_type:
                property_url = 'huis'
            else:
                property_url = 'appartement'
            city_id = city.lower()
            if room_count != '' and square_meters != '':
                external_link = 'https://www.century21.be/nl/pand/te-huur/{}/{}/{}'.format(property_url, city_id, link_id)
                yield scrapy.Request(
                    url=external_link,
                    callback=self.get_details,
                    meta={
                        'external_id' : external_id,
                        'bathroom_count' : bathroom_count,
                        'property_type': property_type, 
                        'title': title, 
                        'description': description, 
                        'rent': rent, 
                        'address': address,
                        'city': city,
                        'zipcode': zipcode,
                        'room_count': room_count,
                        'floor': floor,
                        'latitude': latitude,
                        'longitude': longitude,
                        'terrace': terrace,
                        'parking': parking,
                        'swimming_pool': swimming_pool,
                        'square_meters': square_meters,
                        'elevator': elevator
                    },
                    dont_filter=True
                )

    def get_details(self, response):
        external_link = response.url
        image_links = response.xpath('//div[@id="___gatsby"]//div[@class="carousel slide"]//div[contains(@class, "carousel-item")]//img')
        images = []
        for image_link in image_links:
            image_url = image_link.xpath('./@src').extract_first()
            if image_url not in images: 
                images.append(image_url)
        item_loader = ListingLoader(response=response)
        if response.meta.get('property_type') == 'null':
            item_loader.add_value('property_type', response.meta.get('property_type'))
        else:
            item_loader.add_value('property_type', response.meta.get('property_type'))
        item_loader.add_value('title', response.meta.get('title'))
        item_loader.add_value('external_id', response.meta.get('external_id'))
        item_loader.add_value('bathroom_count', response.meta.get('bathroom_count'))
        item_loader.add_value('external_link', external_link)
        item_loader.add_value('address', response.meta.get('address'))
        item_loader.add_value('description', response.meta.get('description'))
        item_loader.add_value('rent_string', response.meta.get('rent'))
        item_loader.add_value('images', images)
        item_loader.add_value('square_meters', response.meta.get('square_meters'))
        item_loader.add_value('zipcode', response.meta.get('zipcode'))
        item_loader.add_value('city', response.meta.get('city'))
        item_loader.add_value('room_count', response.meta.get('room_count'))
        item_loader.add_value('floor', response.meta.get('floor'))
        item_loader.add_value('landlord_name', 'CENTURY 21 Benelux N.V.')
        item_loader.add_value('landlord_email', 'info@century21.be')
        item_loader.add_value('landlord_phone', '+322 721 21 21')
        item_loader.add_value('external_source', 'Century21ViaPlus_PySpider_belgium_nl')
        if response.meta.get('parking'):
            item_loader.add_value('parking', True)
        if response.meta.get('elevator'):
            item_loader.add_value('elevator', True)
        if response.meta.get('terrace'):
            item_loader.add_value('terrace', True)
        if response.meta.get('swimming_pool'):
            item_loader.add_value('swimming_pool', True)
        yield item_loader.load_item()
