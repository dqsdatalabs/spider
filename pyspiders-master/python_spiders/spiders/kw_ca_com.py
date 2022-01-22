import scrapy
from scrapy import Request, FormRequest
from scrapy.utils.response import open_in_browser
from scrapy.loader import ItemLoader
from scrapy.selector import Selector
from scrapy.utils.url import add_http_if_no_scheme

from ..loaders import ListingLoader
from ..helper import *
from ..user_agents import random_user_agent

import requests
import re
import time
from urllib.parse import urlparse, urlunparse, parse_qs
import json


class kwSpider(scrapy.Spider):

    name = 'kw'
    execution_type = 'testing'
    country = 'canada'
    locale = 'en'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['kw.com']
    start_urls = [
        'https://kw.com/search/location/ChIJ2WrMN9MDDUsRpY9Doiq3aJk,canada,Canada?for_rent=true&for_sale=false&viewport=70%2C-50%2C42%2C-142']

    position = 1

    def parse(self, response):

        secret_key = response.css('#__NEXT_DATA__::text').get()
        jsonresponse = json.loads(secret_key)
        shared_secret = jsonresponse['runtimeConfig']['graphQLAPISecret']

        body = {
            "operationName": "searchListingsQuery",
            "variables": {
                "query": "{\"@type\":\"query.listing\",\"listing\":{\"selector\":{\"filter\":{\"$or\":[{\"facet\":{\"$elemMatch\":{\"LISTING_CATEGORY_ID\":{\"$eq\":1}}}}]}},\"flags\":{},\"sorting\":[{\"LISTING_UPDATE_DATE\":\"DESC\"}],\"bounding\":{\"bounding\":{\"geo\":{\"@type\":\"geo.area\",\"type\":\"Feature\",\"geometry\":{\"type\":\"Polygon\",\"coordinates\":[[[-131.15625,75.76769216128254],[-131.15625,25.673605065840622],[-60.843750000000014,25.673605065840622],[-60.843750000000014,75.76769216128254],[-131.15625,75.76769216128254]]]}}}}}}",
                "first": 1000,
                "queryId": "0.5187423635769524"
            },
            "query": "query searchListingsQuery($query: SearchQueryJSON!, $marketCenter: Int, $first: Int, $after: String, $filter: FilterEnum) {\n  SearchListingsQuery(query: $query, marketCenter: $marketCenter) {\n    result {\n      ...SearchListingResultFragment\n      __typename\n    }\n    __typename\n  }\n}\n\nfragment SearchListingResultFragment on SearchListingResultType {\n  listings(first: $first, after: $after, filter: $filter) {\n    edges {\n      node {\n        ...ListingPropertyCardFragment\n        __typename\n      }\n      __typename\n    }\n    pageInfo {\n      ...PageInfoFragment\n      __typename\n    }\n    totalCount\n    __typename\n  }\n  __typename\n}\n\nfragment ListingPropertyCardFragment on ListingType {\n  id\n  listingCategory\n  listingDate\n  listingStatus\n  listingUpdateDate\n  propertyType\n  propertySubType\n  match\n  virtualTour\n  neighborhood {\n    id\n    __typename\n  }\n  locator {\n    address {\n      state\n      city\n      display\n      zipcode\n      country\n      letterbox {\n        unit {\n          label\n          __typename\n        }\n        number {\n          low\n          __typename\n        }\n        __typename\n      }\n      __typename\n    }\n    geo {\n      ...BaseGeoPointFragment\n      __typename\n    }\n    __typename\n  }\n  flags {\n    ...ListingFlagsFragment\n    __typename\n  }\n  image\n  categories {\n    images {\n      image\n      __typename\n    }\n    __typename\n  }\n  featuredImages {\n    id\n    image\n    category\n    __typename\n  }\n  features {\n    ...ListingFeaturesFragment\n    __typename\n  }\n  pricing {\n    ...ListingPriceDataTypeFragment\n    __typename\n  }\n  openhouses {\n    ...ListingOpenHouseFragment\n    __typename\n  }\n  listingAgentData {\n    courtesyOfBrokerage\n    __typename\n  }\n  mlsNumber\n  mlsId\n  changeLog {\n    statusChange\n    statusChangeDate\n    openHouseChange\n    openHouseChangeDate\n    __typename\n  }\n  listingSizeData {\n    ...ListingSizeDataFragment\n    __typename\n  }\n  __typename\n}\n\nfragment BaseGeoPointFragment on BaseGeoPointType {\n  point {\n    coordinates\n    type\n    __typename\n  }\n  __typename\n}\n\nfragment ListingOpenHouseFragment on ListingOpenHouseType {\n  startDateTime\n  endDateTime\n  __typename\n}\n\nfragment ListingFeaturesFragment on ListingFeatures {\n  bedrooms\n  bathrooms\n  homeSize {\n    ...DimensionTypeFragment\n    __typename\n  }\n  lotSize {\n    ...DimensionTypeFragment\n    __typename\n  }\n  hasParking\n  __typename\n}\n\nfragment DimensionTypeFragment on DimensionType {\n  value\n  dimension\n  __typename\n}\n\nfragment ListingPriceDataTypeFragment on ListingPriceDataType {\n  rent {\n    ...ListingPriceFragment\n    __typename\n  }\n  sale {\n    ...ListingPriceFragment\n    __typename\n  }\n  estimate {\n    ...ListingPriceEstimateFragment\n    __typename\n  }\n  __typename\n}\n\nfragment ListingPriceFragment on ListingPriceType {\n  price {\n    ...PriceFragment\n    __typename\n  }\n  pricePerSize {\n    ...PriceFragment\n    __typename\n  }\n  variance {\n    ...ListingPriceVarianceFragment\n    __typename\n  }\n  __typename\n}\n\nfragment PriceFragment on PriceType {\n  amount\n  currency\n  __typename\n}\n\nfragment ListingPriceVarianceFragment on ListingPriceVarianceType {\n  priceDifference {\n    ...PriceFragment\n    __typename\n  }\n  variance\n  days\n  __typename\n}\n\nfragment ListingPriceEstimateFragment on ListingPriceEstimateType {\n  price {\n    ...PriceFragment\n    __typename\n  }\n  variance {\n    ...ListingPriceVarianceFragment\n    __typename\n  }\n  range {\n    ...ListingPriceEstimateRangeFragment\n    __typename\n  }\n  yearForecast {\n    ...ListingPriceEstimateForecastFragment\n    __typename\n  }\n  __typename\n}\n\nfragment ListingPriceEstimateRangeFragment on ListingPriceEstimateRangeType {\n  low {\n    ...PriceFragment\n    __typename\n  }\n  high {\n    ...PriceFragment\n    __typename\n  }\n  __typename\n}\n\nfragment ListingPriceEstimateForecastFragment on ListingPriceEstimateForecastType {\n  price {\n    ...PriceFragment\n    __typename\n  }\n  variance\n  __typename\n}\n\nfragment ListingFlagsFragment on ListingFlagsType {\n  isExclusive\n  isOpenHouse\n  isMatch\n  isAgentRecommended\n  isPhotoDisplayAllowed\n  isAddressDisplayAllowed\n  __typename\n}\n\nfragment ListingSizeDataFragment on ListingSizeDataType {\n  livingArea\n  livingAreaUnits\n  lotSizeArea\n  lotSizeAreaUnits\n  __typename\n}\n\nfragment PageInfoFragment on PageInfo {\n  endCursor\n  hasNextPage\n  __typename\n}\n"
        }

        headers = {
            "x-shared-secret": "MjFydHQ0dndjM3ZAI0ZHQCQkI0BHIyM=",
            "Content-Type": "application/json; charset=UTF-8",
            "Accept": "*/*",
            "Accept-Encoding": "gzip, deflate, br",
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:48.0) Gecko/20100101 Firefox/48.0'


        }

        yield Request(url='https://api-endpoint.cons-prod-us-central1.kw.com/graphql',
                      body=json.dumps(body), headers=headers,
                      method='POST',
                      callback=self.parseApartment
                      )

    def parseApartment(self, response):

        apartments = json.loads(response.text)[
            'data']['SearchListingsQuery']['result']['listings']['edges']

        for apartment in apartments:

            external_link = 'https://kw.com/property/'+apartment['node']['id']
            external_id = apartment['node']['id']

            title = apartment['node']['locator']['address']['display']
            city = apartment['node']['locator']['address']['city']
            state = apartment['node']['locator']['address']['state']
            zipcode = apartment['node']['locator']['address']['zipcode']
            address = str(city)+", "+str(state)+" "+str(zipcode)

            coordinates = apartment['node']['locator']['geo']['point']['coordinates']
            latitude = str(coordinates[1])
            longitude = str(coordinates[0])

            property_type = apartment['node']['propertySubType']
            if property_type == 'CONDO' or property_type == 'APARTMENT':
                property_type = 'apartment'
            else:
                property_type = 'house'

            images = []

            pics = apartment['node']['categories']
            for pic in pics:
                for picc in pic['images']:
                    image = picc['image']
                    images.append(
                        'https://cflare.smarteragent.com/rest/Resizer?url={img}'.format(img=image))

            square_meters = sq_feet_to_meters(
                apartment['node']['features']['homeSize']['value'])
            room_count = int(apartment['node']['features']['bedrooms'])
            if room_count == "":
                room_count = 0
            bathroom_count = int(apartment['node']['features']['bathrooms'])

            rent = apartment['node']['pricing']['sale']['price']['amount']
            currency = 'CAD'

            parking = apartment['node']['features']['hasParking']

            dataUsage = {
                "position": self.position,
                "property_type": property_type,
                "title": title,
                "external_id": external_id,
                "external_link": external_link,
                "city": city,
                "address": address,
                "zipcode": zipcode,
                "longitude": longitude,
                "latitude": latitude,
                "square_meters": square_meters,
                "room_count": room_count,
                "images": images,
                "bathroom_count": bathroom_count,
                # "terrace": terrace,
                "parking": parking,
                "rent": rent,
                "currency": currency,
            }
            self.position += 1
            headers = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:48.0) Gecko/20100101 Firefox/48.0'}

            try:
                yield Request(external_link,
                    callback=self.parse_description,headers=headers,
                    dont_filter=True, meta=dataUsage)
            except:
                continue
            

            self.position += 1

    def parse_description(self, response):

        details = json.loads(response.css('#__NEXT_DATA__::text').get())
        description = details['props']['pageProps']['propertyData']['description']
 
        if description == None:
            description = ""
        room_count = response.meta['room_count']
        if not room_count or room_count=='0' or len(str(room_count))==0 or room_count==0:
            room_count='1'
        if int(response.meta['rent'])>0 or int(response.meta['rent'])<16000:
            item_loader = ListingLoader(response=response)

            item_loader.add_value("external_link", response.url)
            item_loader.add_value("external_source", self.external_source)
            item_loader.add_value("external_id", response.meta["external_id"])
            item_loader.add_value("title", response.meta['title'])

            item_loader.add_value("description", description)
            item_loader.add_value("city", response.meta['city'])
            item_loader.add_value("zipcode", response.meta['zipcode'])
            item_loader.add_value("address", response.meta['address'])
            item_loader.add_value("latitude", response.meta['latitude'])
            item_loader.add_value("longitude", response.meta['longitude'])

            item_loader.add_value("property_type", response.meta['property_type'])
            item_loader.add_value("square_meters", int(int(response.meta['square_meters'])*10.764))
            item_loader.add_value("room_count", room_count)
            item_loader.add_value(
                "bathroom_count", response.meta['bathroom_count'])

            item_loader.add_value("images", response.meta['images'])
            item_loader.add_value("external_images_count",
                                len(response.meta['images']))

            item_loader.add_value("rent", response.meta['rent'])
            item_loader.add_value("currency", response.meta['currency'])

            item_loader.add_value("dishwasher", 'dishwasher' in description)
            item_loader.add_value("furnished", 'furnished' in description)
            #item_loader.add_value("floor", floor)
            item_loader.add_value("parking", response.meta['parking'])

            item_loader.add_value("elevator", 'elevator' in description)
            item_loader.add_value('washing_machine', 'LAUNDRY' in description or 'aundry' in description)
            item_loader.add_value("balcony", 'balcony' in description)
            item_loader.add_value('swimming_pool', 'Pool' in description)
            item_loader.add_value("terrace", 'errace' in description)
            item_loader.add_value("landlord_name", 'Keller Williams Realty International')
            #item_loader.add_value("landlord_email", response.landlord_email)
            item_loader.add_value("landlord_phone", '512-327-3070')
            item_loader.add_value("position", response.meta['position'])

            yield item_loader.load_item()
