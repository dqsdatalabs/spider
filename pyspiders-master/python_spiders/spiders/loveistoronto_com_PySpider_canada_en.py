import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import json
import math

class loveistoronto_com_PySpider_canadaSpider(scrapy.Spider):
    name = 'loveistoronto_com'
    allowed_domains = ['loveistoronto.com']
    start_urls = [
        'https://loveistoronto.com/Listings/Rent'
        ]
    country = 'canada'
    locale = 'en'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'

    def start_requests(self):
        yield Request(url='https://loveistoronto.com/WebService.svc/SearchListingsAdapter?fwdId=5b3f60abcc44f41394e0cea9&model=%7B%22IsCommunity%22%3Atrue%2C%22Latitude%22%3A43.65771%2C%22Longitude%22%3A-79.38618%2C%22BoundsNorth%22%3A43.664056178691226%2C%22BoundsSouth%22%3A43.641874848242516%2C%22BoundsEast%22%3A-79.35634367480313%2C%22BoundsWest%22%3A-79.40766349449191%2C%22Pivot%22%3A%224%22%2C%22MinPrice%22%3A%22Any%22%2C%22MaxPrice%22%3A%22Any%22%2C%22Beds%22%3A%220%22%2C%22Baths%22%3A%220%22%2C%22BuildingType%22%3A0%2C%22ShowIDX%22%3Afalse%2C%22Proximity%22%3Afalse%2C%22Source%22%3A0%2C%22Query%22%3A%22%22%7D',
                      callback=self.parse,
                      body='',
                      method='GET')

    def parse(self, response):  
        parsed_response = json.loads(response.body)
        for item in parsed_response['results']:
            item_loader = ListingLoader(response=response)
            square_meters = None            
            room_count = None
            bed = 0
            bed_plus = 0
            bathroom_count = None
            rent = None
            mlNum = None
            external_link = None
            description = None

            property_type = item['propertyTypeId']
            if property_type == "Condo":
                property_type = 'apartment'       

            try:
                square_meters = item['sqft']
                square_meters = square_meters.split("-")[1]
                square_meters = math.ceil(int(square_meters)/10.764)
            except:
                pass

            try:
                bed = int(item['beds'])
            except:
                pass
            try:
                bed_plus = int(item['bedsPlus'])
            except:
                pass
            try:
                room_count = bed+bed_plus
            except:
                pass
            if room_count == 0:
                room_count = 1
            try:
                bathroom_count = int(item['baths'])
            except:
                pass

            images = item['images']
            external_images_count = len(images)
            try:
                rent = int(item['listPrice'])
            except:
                pass
            try:
                external_link = "https://loveistoronto.com/Listing/"+item['mlNum']+'?id='+str(item['listingId'])
            except:
                pass
                
            if external_link is not None:
                
                pets_allowed = None
                furnished = None
                elevator = None
                balcony = None
                terrace = None
                swimming_pool = None
                washing_machine = None
                dishwasher = None


                description = item['description']
                parking = int(item['parkingSpaces'])
                if parking > 0:
                    parking = True
                else:
                    parking = False
                if "No Pets" in description or "No Pet" in description:
                    pets_allowed = False
                if "Furniture" in description or "Furnished" in description:
                    furnished = True
                if "Elevators" in description:
                    elevator = True
                if "Balcony" in description or "Balconies" in description:
                    balcony = True
                if "Terrace" in description:
                    terrace = True
                if "Pool" in description:
                    swimming_pool = True
                if "Laundry" in description or "Washer" in description:
                    washing_machine = True
                if "Dishwasher" in description:
                    dishwasher = True
                title = item['addressDetails']['formattedStreetAddress']
                if 'Parking' not in title:

                    item_loader.add_value('external_link',external_link)
                    item_loader.add_value('external_id',str(item['listingId']))
                    item_loader.add_value('external_source',self.external_source)
                    item_loader.add_value('title', title)
                    item_loader.add_value('description',description)
                    item_loader.add_value('city',item['addressDetails']['city'])
                    item_loader.add_value('zipcode',item['addressDetails']['zip'])
                    item_loader.add_value('address',item['addressDetails']['formattedStreetAddress'])
                    item_loader.add_value('latitude',str(item['latitude']))
                    item_loader.add_value('longitude',str(item['longitude']))
                    item_loader.add_value('property_type',property_type)
                    item_loader.add_value('square_meters',int(int(square_meters)*10.764))
                    item_loader.add_value('room_count',room_count)
                    item_loader.add_value('bathroom_count',bathroom_count)
                    item_loader.add_value('available_date',item['status'])
                    item_loader.add_value('images',images)
                    item_loader.add_value('external_images_count',external_images_count)
                    item_loader.add_value('rent',rent)
                    item_loader.add_value('currency','CAD')
                    item_loader.add_value('parking',parking)
                    item_loader.add_value('pets_allowed',pets_allowed)
                    item_loader.add_value('furnished',furnished)
                    item_loader.add_value('elevator',elevator)
                    item_loader.add_value('balcony',balcony)
                    item_loader.add_value('terrace',terrace)
                    item_loader.add_value('swimming_pool',swimming_pool)
                    item_loader.add_value('washing_machine',washing_machine)
                    item_loader.add_value('dishwasher',dishwasher)
                    item_loader.add_value("landlord_name","Alex Patlavski Broker")
                    item_loader.add_value("landlord_email","alex@loveistoronto.com ")
                    item_loader.add_value("landlord_phone","647-504-7107")
                    yield item_loader.load_item()
