import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import json
import math


class BrianpersaudCaSpider(scrapy.Spider):
    name = 'brianpersaud_ca'
    allowed_domains = ['brianpersaud.ca']
    start_urls = ['https://search.brianpersaud.ca/WebService.svc/SearchListingsAdapter?fwdId=5c2a55600fcff60f8c44ecab&model=%7B%22IsCommunity%22%3Atrue%2C%22Latitude%22%3A43.65771%2C%22Longitude%22%3A-79.38618%2C%22BoundsNorth%22%3A90%2C%22BoundsSouth%22%3A-90%2C%22BoundsEast%22%3A180%2C%22BoundsWest%22%3A-180%2C%22Pivot%22%3A%224%22%2C%22MinPrice%22%3A%22Any%22%2C%22MaxPrice%22%3A%22Any%22%2C%22Beds%22%3A%220%22%2C%22Baths%22%3A%220%22%2C%22BuildingType%22%3A0%2C%22ShowIDX%22%3Afalse%2C%22Proximity%22%3Atrue%2C%22Source%22%3A0%2C%22Query%22%3A%22%22%7D']
    country = 'canada'
    locale = 'en'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'development'

    def start_requests(self):
        for url in self.start_urls:
            yield Request(url=url,
                          callback=self.parse,
                          body='',
                          method='GET')

    def parse(self, response):
        parsed_response = json.loads(response.body)
        for item in parsed_response['results']:
            item_loader = ListingLoader(response=response)

            try:
                link = f'https://search.brianpersaud.ca/Listing/{item["mlNum"]}?id={item["listingId"]}'

                space = int(item['sqft'].split('-')[-1])
                space = int(space)/10.7639

                property_type = item['propertyTypeId']
                if "apartment" in property_type or "Condo" in property_type:
                    property_type = "apartment"
                elif "house" in property_type:
                    property_type = "house"

                id = None or str(item['listingId'])
                description = None or item['description']

                rooms = int(float(item['beds'])) or None
                try:
                    if rooms is None:
                        rooms = 1
                except:
                    pass
                baths = int(float(item['baths'])) or None
                space = None or int(space)

                address = item['address'] or None
                city = item['addressDetails']['city'] or None
                zipcode = item['addressDetails']['zip'] or None

                lat = None or item['latitude']
                lng = None or item['longitude']

                images = item['images'] or None

                rent = None or int(item['listPrice'])

                furnished = None
                if 'furnished' in description.lower():
                    furnished = True
                parking = None
                if 'parking' in description.lower():
                    parking = True
                elevator = None
                if 'elevator' in description.lower():
                    elevator = True
                washing_machine = None
                if 'laundry' in description.lower():
                    washing_machine = True
                dishwasher = None
                if 'dishwasher' in description.lower():
                    dishwasher = True
                swimming_pool = None
                if 'swimming' in description.lower():
                    swimming_pool = True
                pets_allowed = None
                if 'no pets' in description.lower():
                    pets_allowed = False
                balcony = None
                if 'balcony' in description.lower():
                    balcony = True
                terrace = None
                if 'terrace' in description.lower():
                    terrace = True




                item_loader.add_value('external_id', id)
                item_loader.add_value('external_source', self.external_source)
                item_loader.add_value('external_link', link)
                item_loader.add_value('title', address)
                item_loader.add_value('description', description)

                item_loader.add_value('property_type', property_type)
                item_loader.add_value('square_meters', int(int(space)*10.764))
                item_loader.add_value('room_count', rooms)
                item_loader.add_value("bathroom_count", baths)

                item_loader.add_value('address', address)
                item_loader.add_value('city', city)
                item_loader.add_value('zipcode', zipcode)

                item_loader.add_value("latitude", str(lat))
                item_loader.add_value("longitude", str(lng))

                item_loader.add_value("images", images)
                item_loader.add_value("external_images_count",
                                      len(images))

                item_loader.add_value("pets_allowed", pets_allowed) # Boolean
                item_loader.add_value("furnished", furnished)  # Boolean
                item_loader.add_value("parking", parking)  # Boolean
                item_loader.add_value("elevator", elevator) # Boolean
                item_loader.add_value("balcony", balcony)  # Boolean
                item_loader.add_value("terrace", terrace) # Boolean
                item_loader.add_value("swimming_pool", swimming_pool)  # Boolean
                item_loader.add_value("washing_machine", washing_machine)  # Boolean
                item_loader.add_value("dishwasher", dishwasher) # Boolean

                # Monetary Status
                item_loader.add_value("rent", rent)
                item_loader.add_value("currency", "CAD")

                item_loader.add_value("landlord_phone", '416.222.8600')
                item_loader.add_value(
                    "landlord_email", 'brian@brianpersaud.ca')
                item_loader.add_value(
                    "landlord_name", 'RE/MAX REALTRON REAL EXPERTS')

                yield item_loader.load_item()
            except:
                pass
