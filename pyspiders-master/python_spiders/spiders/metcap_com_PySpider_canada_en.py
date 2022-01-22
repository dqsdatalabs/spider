import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import json
import requests
import math

class metcap_com_PySpider_canadaSpider(scrapy.Spider):
    name = 'metcap_com'
    allowed_domains = ['metcap.com']
    start_urls = [
        'https://www.metcap.com/province-search-results?lang=en&provinces=&cities=&beds=&price_from=&price_to='
        ]
    country = 'canada'
    locale = 'en'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'


    def parse(self, response):  #page_follower
        urls = response.css("h2 a::attr(href)").extract()
        emem = response.css(".province-results__item").extract()
        for i in range(len(urls)):
            urls[i] = "https://www.metcap.com" + urls[i]

        for i in range(len(urls)):
            latitude = emem[i].split('{lat: ')[1].split(',')[0]
            longitude = emem[i].split('lon: ')[1].split('}')[0]
            yield Request(url=urls[i],
            callback = self.parse_property,
            meta={
                'latitude':latitude,
                'longitude':longitude
            })
       

    def parse_property(self, response):
        units = 0
        try:
            units = response.css("body > div.listing.listing--building > div > div.listing-content > table > tbody > tr").extract()
        except:
            pass
        counter = 1
        cc = 1 
        if len(units) > 0:
        
            for i in range(len(units)):
                
                item_loader = ListingLoader(response=response)
                room_count = response.css('body > div.listing.listing--building > div > div.listing-content > table > tbody > tr:nth-child('+str(cc)+') > td:nth-child(3)::text').get()
                bathroom_count = response.css('body > div.listing.listing--building > div > div.listing-content > table > tbody > tr:nth-child('+str(cc)+') > td:nth-child(4)::text').get()
                square_meters = response.css('body > div.listing.listing--building > div > div.listing-content > table > tbody > tr:nth-child('+str(cc)+') > td:nth-child(5)::text').get()
                rent = response.css('body > div.listing.listing--building > div > div.listing-content > table > tbody > tr:nth-child('+str(cc)+') > td:nth-child(6)::text').get()
                available_date = response.css('body > div.listing.listing--building > div > div.listing-content > table > tbody > tr:nth-child('+str(cc)+') > td:nth-child(2)::text').get()
                cc = cc +1 
                rent = int(rent.replace('$','').replace(',','').split('.')[0])
                if 'N/A' in square_meters:
                    square_meters = None
                else:
                    square_meters = int(math.ceil(int(square_meters)/10.764))
                if '.5' in bathroom_count:
                    bathroom_count = int(math.ceil(float(bathroom_count)))
                else:
                    bathroom_count = int(bathroom_count)
                room_count = int(room_count)
                if room_count == 0:
                    room_count = 1

                title = response.css('body > div.listing.listing--building > div > div.listing-content > h1::text').extract()
                latitude = response.meta.get("latitude")
                longitude = response.meta.get("longitude")
                
                responseGeocode = requests.get(f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
                responseGeocodeData = responseGeocode.json()
                zipcode = responseGeocodeData['address']['Postal']
                city = responseGeocodeData['address']['City']
                address = responseGeocodeData['address']['Match_addr']
                description = response.xpath("//meta[@name='description']/@content").get()
                
                info = response.css('body > div.listing.listing--building > div > div.listing-sidebar').get()
                balcony = None
                washing_machine = None
                dishwasher = None
                parking = None
                pets_allowed = None
                elevator = None
                swimming_pool = None
                if 'Parking' in info:
                    parking = True
                if 'Laundry' in info:
                    washing_machine = True
                if 'dishwasher' in description or 'Dishwasher' in info:
                    dishwasher = True
                if 'Elevator' in info:
                    elevator = True
                if 'Pet Friendly' in info:
                    pets_allowed = info.split('Pet Friendly</h2>')[1].split('<li>')[1].split('<')[0]
                if "Yes" in pets_allowed:
                    pets_allowed = True
                else:
                    pets_allowed = False
                if 'balconies' in description:
                    balcony = True
                if 'pool' in description:
                    swimming_pool = True

                images = response.css('.js-gallery-item').extract()
                for i in range(len(images)):
                    images[i] = images[i].split('url(')[1].split('"')[0]
                external_images_count = len(images)
                property_type = 'apartment'

                responseGeocode = requests.get(f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
                responseGeocodeData = responseGeocode.json()

                landlord_email = response.css("body > div.listing.listing--building > div > div.listing-sidebar > div > p > span.d-block.mb-30::text").get()
                landlord_phone = response.css("body > div.listing.listing--building > div > div.listing-sidebar > div > p > span:nth-child(2)::text").get()
                landlord_name = response.css("body > div.listing.listing--building > div > div.listing-sidebar > div > p > span:nth-child(1) > span::text").get()

                if rent > 0:
                    item_loader.add_value('external_link', response.url+f"#{counter}")        
                    item_loader.add_value('external_source', self.external_source)
                    item_loader.add_value('title',title)
                    item_loader.add_value('description',description)
                    item_loader.add_value('city',city)
                    item_loader.add_value('address',address)
                    item_loader.add_value('latitude',latitude)
                    item_loader.add_value('longitude',longitude)
                    item_loader.add_value('zipcode',zipcode)
                    item_loader.add_value('property_type',property_type)
                    item_loader.add_value('room_count',room_count)
                    item_loader.add_value('bathroom_count',bathroom_count)
                    item_loader.add_value('square_meters',square_meters)
                    item_loader.add_value('available_date',available_date)
                    item_loader.add_value('rent',rent)
                    item_loader.add_value('currency','CAD')
                    item_loader.add_value('images',images)
                    item_loader.add_value('external_images_count',external_images_count)
                    item_loader.add_value('pets_allowed',pets_allowed)
                    item_loader.add_value('balcony',balcony)
                    item_loader.add_value('swimming_pool',swimming_pool)
                    item_loader.add_value('elevator',elevator)
                    item_loader.add_value('parking',parking)
                    item_loader.add_value('washing_machine',washing_machine)
                    item_loader.add_value('dishwasher',dishwasher)
                    item_loader.add_value('landlord_name',landlord_name)
                    item_loader.add_value('landlord_phone',landlord_phone)
                    item_loader.add_value('landlord_email',landlord_email)
                    counter = counter+1
                    yield item_loader.load_item()
