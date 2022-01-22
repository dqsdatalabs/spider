from typing import Any

import scrapy
from scrapy import Request
from ..items import ListingItem
from ..loaders import ListingLoader
import requests

class AlphaImmobiliareSpider(scrapy.Spider):
    name = 'alpha_immo'
    allowed_domains = ['alphaimmobiliare.com']
    start_urls = ['http://www.alphaimmobiliare.com/immobili-residenziali-in-affitto-alpha-immobiliare']
    execution_type = 'testing'
    country = 'italy'
    locale ='it'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)

    def parse(self, response):
        for start_url in self.start_urls:
            yield Request(url=start_url,
                          callback=self.parse_area)

    def parse_area(self, response):
        area_urls = response.css('.annuncio_title::attr(href)').extract()
        area_urls = ['http://www.alphaimmobiliare.com/' + x for x in area_urls]
        for area_url in area_urls:
            yield Request(url=area_url,
                          callback=self.parse_pages)

    def parse_pages(self, response):
        items = ListingItem()

        external_link = str(response.request.url)

        description = response.css(".first-paragraph::text")[0].extract()
        title = response.css("h1::text")[0].extract()
        if "ATTICO" in title:
            property_type = "house"
        else:
            property_type = "apartment"
        rent = response.css("tr:nth-child(1) strong::text")[0].extract()
        if any(char.isdigit() for char in rent):
            rent = int(''.join(x for x in rent if x.isdigit()))
        else:
            return

        external_id = response.css("tr:nth-child(2) strong::text")[0].extract()
        city = response.css("tr:nth-child(3) strong::text")[0].extract()
        area = response.css("tr:nth-child(4) strong::text")[0].extract()
        address = city + ", " + area
        square_meters = response.css("tr:nth-child(6) strong::text")[0].extract()
        square_meters = int(square_meters)
        room_count = response.css(".col-xs-12:nth-child(1) tr:nth-child(1) td+ td::text")[0].extract()
        if any(char.isdigit() for char in room_count):
            room_count = int(''.join(x for x in room_count if x.isdigit()))
        else:
            room_count = 1
        bathroom_count = response.css(".col-xs-12:nth-child(1) tr:nth-child(2) td+ td::text")[0].extract()
        if any(char.isdigit() for char in bathroom_count):
            bathroom_count = int(''.join(x for x in bathroom_count if x.isdigit()))
        else:
            bathroom_count = 1
        terrace = False
        check_terrace = response.css(".col-xs-12:nth-child(1) td:nth-child(1)::text").extract()
        try:
            if "Terrazzi" in check_terrace:
                terrace = True
        except:
            terrace = False

        amenities = response.css(".clear-both-mobile td:nth-child(1)::text").extract()
        elevator = False
        dishwasher = False
        washing_machine = False
        pets_allowed = False
        try:
            if "Ascensore" in amenities:
                elevator = True
        except:
            elevator = False
        try:
            if "Lavatrice" in amenities:
                washing_machine = True
        except:
            washing_machine = False
        try:
            if "Lavastoviglie" in amenities:
                dishwasher = True
        except:
            dishwasher = False
        try:
            if "Animali ammessi" in amenities:
                pets_allowed = True
        except:
            pets_allowed = False
        floor = " "
        try:
            if "Livelli" in amenities:
                floor = response.css(".clear-both-mobile tr:nth-child(1) td+ td::text")[0].extract()
        except:
            pass
        latitude = response.css('#latitude_hidden::text')[0].extract()
        longitude = response.css('#longitude_hidden::text')[0].extract()
        responseGeocode = requests.get(
            f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
        responseGeocodeData = responseGeocode.json()

        address = responseGeocodeData['address']['Match_addr']
        city = responseGeocodeData['address']['City']
        zipcode = responseGeocodeData['address']['Postal']

        images = response.css('.rsTmb::attr(src)').extract()
        for image in images:
            if "800x800" in image:
                images = {x.replace('/800x800', '') for x in images}
            else:
                pass

        items['external_source'] = self.external_source
        items['external_link'] = external_link
        items['external_id'] = external_id
        items['address'] = address
        items['title'] = title
        items['city'] = city
        items['zipcode'] = zipcode
        items['latitude'] = latitude
        items['longitude'] = longitude
        items['description'] = description
        items['property_type'] = property_type
        items['square_meters'] = square_meters
        items['room_count'] = room_count
        items['bathroom_count'] = bathroom_count
        items['rent'] = rent
        if floor != " ":
            items['floor'] = floor
        items['elevator'] = elevator
        items['dishwasher'] = dishwasher
        items['washing_machine'] = washing_machine
        items['pets_allowed'] = pets_allowed
        items['terrace'] = terrace
        items['energy_label'] = "G"
        items['currency'] = "EUR"
        items['landlord_name'] = "Alpha Immobiliare"
        items['landlord_phone'] = "390658331153"
        items['landlord_email'] = "info@alphaimmobiliare.com"
        items['images'] = images

        yield items
