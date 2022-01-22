import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import json
import math
import requests

class andreafeldman_com_PySpider_canadaSpider(scrapy.Spider):
    name = 'andreafeldman_com'
    allowed_domains = ['andreafeldman.com']
    start_urls = [
        'https://andreafeldman.com/listings/search'
        ]
    country = 'canada'
    locale = 'en'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'

    def start_requests(self):
        yield Request(url='https://andreafeldman.com/WebService.svc/SearchListingsAdapter?fwdId=56205f01df2f0242dc46d399&model=%7B%22IsCommunity%22%3Atrue%2C%22Latitude%22%3A43.70095%2C%22Longitude%22%3A-79.39745%2C%22BoundsNorth%22%3A90%2C%22BoundsSouth%22%3A-90%2C%22BoundsEast%22%3A180%2C%22BoundsWest%22%3A-180%2C%22Pivot%22%3A%224%22%2C%22MinPrice%22%3A%22Any%22%2C%22MaxPrice%22%3A%22Any%22%2C%22Beds%22%3A%220%22%2C%22Baths%22%3A%220%22%2C%22BuildingType%22%3A0%2C%22ShowIDX%22%3Afalse%2C%22Proximity%22%3Atrue%2C%22Source%22%3A0%2C%22Query%22%3A%22%22%7D',
                    callback=self.parse,
                    body='',
                    method='GET')

    def parse(self, response):  
        parsed_response = json.loads(response.body)
        for item in parsed_response['results']:
            listing_price = None
            try:
                listing_price = item['formattedListPrice']
            except:
                pass
            if listing_price is not None:
                aptnum = ''
                try:
                    aptnum = item['addressDetails']['aptNum']
                    aptnum = aptnum+"-"
                except:
                    pass
                if "#" in aptnum:
                    aptnum = aptnum.replace("#","")
                streetnum = item['addressDetails']['streetNumber']
                streetname = item['addressDetails']['streetName']
                streettype = item['addressDetails']['streetType']
                streetdirection = ''
                try:
                    streetdirection = item['addressDetails']['streetDirection']
                    streetdirection = streetdirection+"-"
                except:
                    pass
                c = item['addressDetails']['city']
                z = item['addressDetails']['zip']
                mlnum = str(item['mlNum'])
                ids = str(item['listingId'])
                url = 'https://andreafeldman.com/listing/'+aptnum+streetnum+'-'+streetname+'-'+streettype+'-'+streetdirection+c+'-'+z+'-'+mlnum+'?id='+ids
                square_meters = None
                try:
                    square_meters = item['sqft']
                except:
                    pass
                bedsPlus = 0
                try:
                    bedsPlus = int(item['bedsPlus'])
                except:
                    pass
                beds = 0
                try:
                    beds = int(item['beds'])
                except:
                    pass
                room_count = bedsPlus + beds
                yield Request(url=url,callback=self.parse_property,
                meta={
                    "external_id":str(item['listingId']),
                    "title":item['address'],
                    "property_type":item['propertyTypeId'],
                    "address":item['address'],
                    "city":item['addressDetails']['city'],
                    "latitude":item['latitude'],
                    "longitude":item['longitude'],
                    "description":item['description'],
                    "bathroom_count":int(item['baths']),
                    "room_count":room_count,
                    "rent":int(item['listPrice']),
                    "available_date":item['status'],
                    "square_meters":square_meters,
                    "parking":int(item['parkingSpaces']),
                    "images":item['images']
                })

    def parse_property(self, response):
        item_loader = ListingLoader(response=response)
        title = response.meta.get("title")
        description= response.meta.get("description")
        property_type = response.meta.get("property_type")
        external_id = response.meta.get("external_id")
        bathroom_count = response.meta.get("bathroom_count")
        room_count = response.meta.get("room_count")
        if room_count == 0:
            room_count = 1
        rent = response.meta.get("rent")
        address = response.meta.get("address")
        city = response.meta.get("city")
        latitude = response.meta.get("latitude")
        longitude = response.meta.get("longitude")
        images = response.meta.get("images")
        parking = response.meta.get("parking")
        square_meters = response.meta.get("square_meters")
        try:
            square_meters = int(square_meters.split("-")[1])
            square_meters = int(math.ceil(float(square_meters/10.764)))
        except:
            pass
        available_date = response.meta.get("available_date")

        external_images_count = len(images)
        if "Condo" in property_type or "Residential" in property_type:
            property_type = 'apartment'
        else:
            property_type = 'house'
        if parking > 0:
            parking = True
        else:
            parking = False

        pets_allowed = None
        furnished = None
        balcony = None
        terrace = None
        swimming_pool = None
        washing_machine = None
        dishwasher = None

        if "Pet" in description:
            pets_allowed = True
        if "Furnished" in description or "Furniture" in description:
            furnished = True
        if "Balcony" in description or "Balconies" in description:
            balcony = True
        if "Terrace" in description:
            terrace = True
        if "Pool" in description or "Pools" in description:
            swimming_pool = True
        if "Laundry" in description:
            washing_machine = True
        if "Dishwasher" in description:
            dishwasher = True
        responseGeocode = requests.get(f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
        responseGeocodeData = responseGeocode.json()
        zipcode = responseGeocodeData['address']['Postal']

        item_loader.add_value('external_link', response.url)
        item_loader.add_value('external_id',external_id)        
        item_loader.add_value('external_source', self.external_source)
        item_loader.add_value('title',title)
        item_loader.add_value('description',description)
        item_loader.add_value('city',city)
        item_loader.add_value('zipcode',zipcode)
        item_loader.add_value('address',address)
        item_loader.add_value('latitude',str(latitude))
        item_loader.add_value('longitude',str(longitude))
        item_loader.add_value('property_type',property_type)
        item_loader.add_value('square_meters',int(int(square_meters)*10.764))
        item_loader.add_value('room_count',room_count)
        item_loader.add_value('bathroom_count',bathroom_count)
        item_loader.add_value('available_date',available_date)
        item_loader.add_value('images',images)
        item_loader.add_value('external_images_count',external_images_count)
        item_loader.add_value('rent',rent)
        item_loader.add_value('currency','CAD')
        item_loader.add_value('pets_allowed',pets_allowed)
        item_loader.add_value('furnished',furnished)
        item_loader.add_value('parking',parking)
        item_loader.add_value('balcony',balcony)
        item_loader.add_value('terrace',terrace)
        item_loader.add_value('swimming_pool',swimming_pool)
        item_loader.add_value('washing_machine',washing_machine)
        item_loader.add_value('dishwasher',dishwasher)
        item_loader.add_value('landlord_name','andreafeldman')
        item_loader.add_value('landlord_email','andrea@andreafeldman.com')
        item_loader.add_value('landlord_phone','416-558-1565')
        yield item_loader.load_item()