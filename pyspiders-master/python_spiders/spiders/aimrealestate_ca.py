from operator import le
from requests.api import get, post
from requests.models import Response
import scrapy
from scrapy import Request, FormRequest
from scrapy.http.request import form
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
import math


class Aimrealestate_Spider(scrapy.Spider):

    name = 'aimrealestate'
    execution_type = 'testing'

    country = 'canada'
    locale = 'en'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"

    position = 1

    def start_requests(self):
        url = 'https://www.aimrealestate.ca/Listing/ListingUnits'
        data = {
            "ConcreteTypeAssemblyQualifiedName":
                "Gotham.Service.Model.ResidentialListingFilter, Gotham.Service, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null"
        }
        yield FormRequest(
            url, method='POST',
            formdata=data, callback=self.parseDetails
        )

    def parseDetails(self, response):

        apartments = apartments = json.loads(response.text)['units']

        for apartment in apartments:
            title = apartment['BuildingName']
            external_id = apartment['ApartmentId']
            external_link = 'https://www.aimrealestate.ca' + \
                apartment['BuildingUrl']
            latitude = apartment['Address']['Latitude']
            longitude = apartment['Address']['Longitude']
            address = apartment['Address']['Full']
            city = apartment['Address']['City']
            zipcode = apartment['Address']['ZipCode']
            property_type = 'apartment'
            landlord_phone = apartment['Building']['ListingEmployeesContacts'][0]['FormattedPhoneNumber']

            datausge = {
                'external_id': external_id,
                'title': title,
                'property_type': property_type,
                'latitude': latitude,
                'longitude': longitude,
                'city': city,
                'zipcode': zipcode,
                'landlord_phone': landlord_phone,
                'address': address
            }
            yield Request(external_link, callback=self.parseApartment, meta=datausge)

    def parseApartment(self, response):

        title = response.meta['title']
        external_id = response.meta['external_id']
        property_type = response.meta['property_type']
        latitude = response.meta['latitude']
        longitude = response.meta['longitude']
        landlord_phone = response.meta['landlord_phone']
        address = response.meta['address']
        zipcode = response.meta['zipcode']
        city = response.meta['city']

        datausge = {
            'external_id': external_id,
            'title': title,
            'property_type': property_type,
            'latitude': latitude,
            'longitude': longitude,
            'city': city,
            'zipcode': zipcode,
            'landlord_phone': landlord_phone,
            'address': address
        }

        currentUl = response.url
        unitsUrls = response.css(
            '.wpb-tabs-menu.wpb-mobileHide a::attr(href)').getall()
        unitsUrls = [currentUl + x for x in unitsUrls]
        unitsIDs = response.css(
            '.wpb-tabs-menu.wpb-mobileHide a::attr(data-unitid)').getall()

        for idx, id in enumerate(unitsIDs):
            dataurl = f'https://www.aimrealestate.ca/Listing/ApartmentView?id={id}&lang=en-CA'

            datausge['uniturl'] = unitsUrls[idx]
            yield Request(dataurl, callback=self.parseData, meta=datausge)

    def parseData(self, response):

        external_link = response.meta['uniturl']

        title = response.meta['title']
        external_id = response.meta['external_id']
        property_type = response.meta['property_type']
        latitude = response.meta['latitude']
        longitude = response.meta['longitude']
        landlord_phone = response.meta['landlord_phone']
        address = response.meta['address']
        zipcode = response.meta['zipcode']
        city = response.meta['city']

        external_id = response.css(
            '.content-item:nth-child(1) .unit-floor-name h3::text').getall()[0]

        rent = 0
        rex = re.search(r'\d+', (response.css('.content-item:nth-child(1) .unit-floor-name h3::text').getall()
                        [1]).replace('$', '').replace(',', ''))
        if rex:
            rent = int(rex[0])

        description = remove_white_spaces(
            "".join(response.css('.unit-notes p::text').getall()))

        balcony = True if 'alcon' in description.lower() else False
        dishwasher = True if 'dishwasher' in description.lower() else False
        parking = True if 'parking' in description.lower() else False
        washing_machine = True if 'aundry' in description else False

        room_count = 1
        bathroom_count = 1
        floor = ''
        square_meters = 0

        room_count = response.css(
            '.main-items:nth-child(1) h4::text').getall()[2]
        if room_count == '0':
            room_count = '1'
        bathroom_count = response.css(
            '.main-items:nth-child(1) h4::text').getall()[3]
        try:
            floor = response.css(
                '.main-items:nth-child(1) h4::text').getall()[1]
        except:
            floor = ''
        try:
            square_meters = int(response.css(
                '.main-items:nth-child(1) h4::text').getall()[0])
        except:
            square_meters = 0

        images = response.css('.content-slider a::attr(href)').getall()
        external_images_count = len(images)
        description = description.replace(
            'Call or Text Jeff at 780-446-0978 for more information or to set up a showing.', '')
        description = description.replace(
            'Please call or text directly at 587-982-4459 to book an appointment to view your NEW HOME', '')
        if rent > 0:

            item_loader = ListingLoader(response=response)

            item_loader.add_value("external_link", external_link)
            item_loader.add_value("external_source", self.external_source)
            item_loader.add_value("external_id", external_id)
            item_loader.add_value("title", title)
            item_loader.add_value("description", description)
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode)
            item_loader.add_value("address", address)
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)
            item_loader.add_value('washing_machine', washing_machine)
            item_loader.add_value(
                'square_meters', int(int(square_meters)*10.764))
            item_loader.add_value('floor', floor)

            item_loader.add_value('balcony', balcony)

            item_loader.add_value(
                "property_type", property_type)
            item_loader.add_value("room_count", room_count)
            item_loader.add_value(
                "bathroom_count", bathroom_count)
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count",
                                  external_images_count)
            item_loader.add_value("rent", rent)
            item_loader.add_value('available_date', 'Available Now!')
            item_loader.add_value("currency", "CAD")
            item_loader.add_value('parking', parking)
            item_loader.add_value('dishwasher', dishwasher)

            item_loader.add_value("landlord_name", 'aimrealestate')
            #item_loader.add_value("landlord_email", 'rentals@oshanter.com')
            item_loader.add_value("landlord_phone", landlord_phone)

            item_loader.add_value("position", self.position)
            self.position += 1
            yield item_loader.load_item()
