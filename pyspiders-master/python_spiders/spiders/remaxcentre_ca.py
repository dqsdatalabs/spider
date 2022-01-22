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


class remaxcentreSpider(scrapy.Spider):

    name = 'remaxcentre'
    execution_type = 'testing'
    country = 'canada'
    locale = 'en'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['remaxcentre.ca']
    start_urls = ['https://realtor.remarketer.ca/api/ES_Query?addressline=&propertysubtypesearch=&typesearch=&propertystylesearch=&salelease=Lease&minprice=&maxprice=&minbed=&minbath=&active=1&limit=900']

    position = 1

    def parse(self, response):

        jsonresponse = json.loads(response.text)

        for prop in jsonresponse:

            position = self.position

            property_type = prop['_source']['ClassSearch']
            if property_type == 'Residential':
                property_type = 'house'
            elif property_type == 'Condo':
                property_type = 'apartment'

            if property_type == 'Commercial':
                continue

            ex_id = prop["_id"].split('-')[0]
            external_link = 'https://remaxcentre.ca/details/{}'.format(ex_id)

            description = prop['_source']['RemarksForClients']

            address = prop['_source']['Address']

            square_ft = str(prop['_source']['BuildingAreaTotal'])
            if '-' in square_ft:
                firstNumber = float(square_ft.split('-')[0])
                secondNumber = float(square_ft.split('-')[1])
                if int(firstNumber) == 0:
                    square_meters = float(secondNumber)/10.7639
                else:
                    square_meters = ((firstNumber+secondNumber)/2)/10.7639
            elif '<' in square_ft:
                firstNumber = float(square_ft.replace('<', ''))
                square_meters = firstNumber/10.7639
            elif '+' in square_ft:
                firstNumber = float(square_ft.replace('+', ''))
                square_meters = firstNumber/10.7639
            elif square_ft == '0' and prop['_source']['ApproxSquareFootage'] != "":
                firstNumber = float(
                    prop['_source']['ApproxSquareFootage'].split(' to ')[0])
                secondNumber = float(
                    prop['_source']['ApproxSquareFootage'].split(' to ')[1])
                if int(firstNumber) == 0:
                    square_meters = float(secondNumber)/10.7639
                else:
                    square_meters = ((firstNumber+secondNumber)/2)/10.7639
            elif prop['_source']['ApproxSquareFootage'] == "":
                firstNumber = float(prop['_source']['LotDepth'])
                secondNumber = float(prop['_source']['LotFront'])
                square_meters = ((firstNumber*secondNumber))/10.7639
            else:
                square_meters = float(square_ft)/10.7639

            square_meters = int(square_meters)

            rent = prop['_source']['ListPrice']

            room_count = int(prop['_source']['Bedrooms']) + \
                int(prop['_source']['BedroomsPlus'])
            if room_count == 0 or not room_count:
                room_count = 1

            furnished = True if len(
                prop['_source']['Furnished']) > 0 and prop['_source']['Furnished'] != 'N' else False

            elevator = True if len(
                prop['_source']['Elevator']) > 0 and prop['_source']['Elevator'] != 'N' else False

            parking = True if float(
                prop['_source']['GarageSpaces']) > 0 else False

            currency = "CAD"

            latitude = prop['_source']['Latitude']
            longitude = prop['_source']['Longitude']

            if latitude != "0":
                responseGeocode = requests.get(
                    f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
                responseGeocodeData = responseGeocode.json()
                zipcode = responseGeocodeData['address']['Postal']
                city = responseGeocodeData['address']['City']
            else:
                responseGeocode = requests.get(
                    f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/find?f=json&text={address}&maxLocations=1")
                responseGeocodeData = responseGeocode.json()

                responseGeocode = requests.get(
                    f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")

                responseGeocodeData = responseGeocode.json()
                zipcode = responseGeocodeData['address']['Postal']
                city = responseGeocodeData['address']['City']

            dataUsage = {
                "position": position,
                "description": description,
                "property_type": property_type,
                "external_id": ex_id,
                "external_link": external_link,
                "city": city,
                "address": address,
                "zipcode": zipcode,
                "furnished": furnished,
                "longitude": longitude,
                "latitude": latitude,
                "square_meters": square_meters,
                "room_count": room_count,
                # "bathroom_count": bathroom_count,
                "elevator": elevator,
                # "terrace": terrace,
                "parking": parking,
                "rent": rent,
                "currency": currency,
            }
            self.position += 1

            # if position==10:
            #    break

            yield Request(external_link,
                          callback=self.parseApartment,
                          dont_filter=True, meta=dataUsage)

    def parseApartment(self, response):

        title = response.css('.pxp-sp-top-title::text').get()
        bathroom_count = int(
            float(response.css('.pxp-sp-top-feat div:nth-child(2)::text').get()))
        landlord_phone = response.css('.pxp-sp-agent-info-phone::text').get()

        images = response.css(
            '.pxp-single-property-gallery-container a::attr(href)').getall()
        del images[-1]

        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_id", response.meta["external_id"])
        item_loader.add_value("title", title)
        item_loader.add_value("description", response.meta['description'])
        item_loader.add_value("city", response.meta['city'])
        item_loader.add_value("zipcode", response.meta['zipcode'])
        item_loader.add_value("address", response.meta['address'])
        item_loader.add_value("latitude", response.meta['latitude'])
        item_loader.add_value("longitude", response.meta['longitude'])
        item_loader.add_value("property_type", response.meta['property_type'])
        item_loader.add_value("square_meters", int(
            int(response.meta['square_meters'])*10.764))
        item_loader.add_value("room_count", response.meta['room_count'])
        item_loader.add_value("bathroom_count", bathroom_count)
        item_loader.add_value("images", images)
        item_loader.add_value("external_images_count", len(images))
        item_loader.add_value("rent", response.meta['rent'])
        item_loader.add_value("currency", response.meta['currency'])
        #item_loader.add_value("energy_label", energy_label)
        item_loader.add_value("furnished", response.meta['furnished'])
        #item_loader.add_value("floor", floor)
        item_loader.add_value("parking", response.meta['parking'])
        item_loader.add_value("elevator", response.meta['elevator'])
        #item_loader.add_value("balcony", response.meta['balcony'])
        #item_loader.add_value("terrace", response.meta['terrace'])
        item_loader.add_value("landlord_name", 'Remax centre')
        #item_loader.add_value("landlord_email", response.landlord_email)
        item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("position", response.meta['position'])

        yield item_loader.load_item()
