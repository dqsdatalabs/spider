import scrapy
from scrapy import Request

from ..helper import extract_number_only
from ..loaders import ListingLoader
import json


class TammysaddlerComSpider(scrapy.Spider):
    name = 'tammysaddler_com'
    allowed_domains = ['tammysaddler.com']
    start_urls = [
        'https://tammysaddler.com/WebService.svc/SearchListingsAdapter?fwdId=5afd6fa9a7da921228ff1d18&model=%7B%22IsCommunity%22%3Atrue%2C%22Latitude%22%3A43.65771%2C%22Longitude%22%3A-79.38618%2C%22BoundsNorth%22%3A90%2C%22BoundsSouth%22%3A-90%2C%22BoundsEast%22%3A180%2C%22BoundsWest%22%3A-180%2C%22Pivot%22%3A%224%22%2C%22MinPrice%22%3A%22Any%22%2C%22MaxPrice%22%3A%22Any%22%2C%22Beds%22%3A%220%22%2C%22Baths%22%3A%220%22%2C%22BuildingType%22%3A0%2C%22ShowIDX%22%3Afalse%2C%22Proximity%22%3Atrue%2C%22Source%22%3A0%2C%22Query%22%3A%22%22%7D']
    country = 'canada'
    locale = 'en'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'

    def start_requests(self):
        for url in self.start_urls:
            yield Request(url=url,
                          callback=self.parse,
                          body='',
                          method='GET')

    def parse(self, response):
        parsed_response = json.loads(response.body)
        base_url = 'https://tammysaddler.com/Listing/'

        for item in parsed_response['results']:
            if (not item['isSold']) and ('formattedListPrice' in item.keys()):
                print(item['listingId'])
                item_loader = ListingLoader(response=response)

                property_type = item['propertyTypeName']
                if "apartment" in property_type:
                    property_type = "apartment"
                elif "house" in property_type:
                    property_type = "house"
                else:
                    property_type = "apartment"

                space = None
                mlnum = ''
                beds = None
                bath = None
                description = ''
                address = ''
                zipcode = ''
                itemurl = ''
                formattedListPrice = extract_number_only(item['formattedListPrice'])

                try:
                    space = item['sqft']
                    space = space.split("-")
                    space = space[1]
                    space = int(int(space) / 10.7639)
                    mlnum = str(item['mlNum'])
                    itemurl = base_url + mlnum + "?id=" + str(item['listingId'])
                    beds = int(item['beds'])
                    if beds == 0:
                        beds = 1
                    bath = int(item['baths'])
                    description = item['description']
                    address = item['address']
                    zipcode = item['addressDetails']['zip']
                except:
                    pass
                if formattedListPrice and(itemurl != ''):
                    item_loader.add_value('external_id', mlnum)
                    item_loader.add_value('external_source', self.external_source)
                    item_loader.add_value('external_link', itemurl)
                    item_loader.add_value('description', description)

                    item_loader.add_value('property_type', property_type)
                    item_loader.add_value('square_meters', int(int(space)*10.764))
                    item_loader.add_value('room_count', beds)
                    item_loader.add_value('bathroom_count', bath)

                    item_loader.add_value('address', address)
                    item_loader.add_value('city', item['addressDetails']['city'])
                    item_loader.add_value('zipcode', zipcode)

                    item_loader.add_value("latitude", str(item['latitude']))
                    item_loader.add_value("longitude", str(item['longitude']))

                    images_arr=item['images']
                    item_loader.add_value("images", images_arr)
                    item_loader.add_value("external_images_count",
                                          len(images_arr))

                    item_loader.add_value("rent",
                                          formattedListPrice)
                    item_loader.add_value("currency", "CAD")
                    if "balcony" in description.lower():
                        item_loader.add_value("balcony", True )
                    else:
                        item_loader.add_value("balcony", '' )


                    if "terrace" in description.lower():
                        item_loader.add_value("terrace", True )
                    else:
                        item_loader.add_value("terrace", '' )

                    if "swimming" in description.lower():
                        item_loader.add_value("swimming_pool", True )
                    else:
                        item_loader.add_value("swimming_pool", '' )

                    if "dishwasher" in description.lower():
                        item_loader.add_value("dishwasher", True )
                    else:
                        item_loader.add_value("dishwasher", '' )

                    item_loader.add_value('parking', bool(int(item['parkingSpaces'])))

                    item_loader.add_value("landlord_name", 'tammy saddler')
                    item_loader.add_value("landlord_email", 'tammy@tammysaddler.com')
                    item_loader.add_value("landlord_phone", '416.712-7754')

                    yield item_loader.load_item()
