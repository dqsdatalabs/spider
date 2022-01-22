from typing import Any

import scrapy
from scrapy import Request
from ..items import ListingItem
from ..loaders import ListingLoader
import requests

class PrimaImmobiliareSpider(scrapy.Spider):
    name = 'prima_immo'
    allowed_domains = ['primaimmobiliare.it']
    start_urls = ['https://www.primaimmobiliare.it/advanced-search/?status%5B%5D=affitto&location%5B%5D=&keyword=']
    # start_urls = ['https://www.primaimmobiliare.it/listing/affitto-appartamento-centro-storico-perugia-a000970/']
    execution_type = 'development'
    country = 'italy'
    locale ='it'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)

    def parse(self, response):
        for start_url in self.start_urls:
            yield Request(url=start_url,
                          callback=self.parse_area)

    def parse_area(self, response):
        area_urls = response.css('.item-title a::attr(href)').extract()
        for area_url in area_urls:
            yield Request(url=area_url,
                          callback=self.parse_pages)

    def parse_pages(self, response):
        property_type = response.css("#main-wrap > section > div.property-top-wrap > div > div.container.hidden-on-mobile > div > div.col-md-12 > div > div > ul:nth-child(1) > li.property-overview-item > strong::text").extract()
        property_type = property_type[0]
        if "Ufficio" in property_type or "Negozio" in property_type:
            return

        items = ListingItem()

        external_link = str(response.request.url)

        description = response.css("#property-description-wrap p::text")[0].extract()
        title = response.css("h1::text")[0].extract()
        rent = response.css(".property-title-price-wrap .item-price::text").extract()
        rent = rent[0]
        if any(char.isdigit() for char in rent):
            rent = int(''.join(x for x in rent if x.isdigit()))
        else:
            return

        square_feet = None
        room_count = None
        bathroom_count = None
        try:
            room_count = response.css(".icon-hotel-double-bed-1+ strong::text").extract()
            room_count = int(room_count[0])
        except:
            room_count = 1

        try:
            square_feet = response.css(".icon-real-estate-dimensions-plan-1+ strong::text").extract()
            square_feet = square_feet[0]
        except:
            pass

        try:
            bathroom_count = response.css(".icon-bathroom-shower-1+ strong::text").extract()
            bathroom_count = int(bathroom_count[0])
        except:
            bathroom_count = 1

        furnish = None
        elevator = None
        balcony = None
        terrace = None
        try:
            furnish = response.css("#schede li::text").extract()
            if " Arredato a Nuovo" in furnish:
                furnished = True
            else:
                furnished = False
        except:
            pass

        try:
            elevator = response.css("#property-features-wrap li a::text").extract()
            if ["Ascensore"] in elevator:
                elevator = True
            else:
                elevator = False
        except:
            pass

        try:
            balcony = response.css("#schede li:nth-last-child(2)::text")[0].extract()
            if "No" in balcony:
                balcony = False
            else:
                balcony = True
        except:
            pass

        try:
            terrace = response.css("#schede li:nth-last-child(1)::text")[0].extract()
            if "No" in terrace:
                terrace = False
            else:
                terrace = True
        except:
            pass

        try:
            energy_label = response.xpath("//*[@id='schede']/div/div[2]/ul/li[1]/text()").extract_first()
            energy_label = energy_label.strip()
        except:
            pass




        landlord_name = response.css(".agent-name::text")[0].extract()

        images = response.css('#main-wrap > section > div.property-top-wrap > div > div.container.hidden-on-mobile > div > a > img::attr(src)').extract()
        images = set(images)
        for image in images:
            if "758x564" in image:
                images = {x.replace('-758x564', '') for x in images}
            else:
                pass

        address = response.css(".page-title-wrap .item-address::text")[0].extract()
        responseGeocode = requests.get(f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/find?f=json&text={address}&maxLocations=1")
        responseGeocodeData = responseGeocode.json()

        longitude = responseGeocodeData['locations'][0]['feature']['geometry']['x']
        latitude = responseGeocodeData['locations'][0]['feature']['geometry']['y']

        responseGeocode = requests.get(f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
        responseGeocodeData = responseGeocode.json()
        zipcode = responseGeocodeData['address']['Postal']
        city = responseGeocodeData['address']['City']
        address = responseGeocodeData['address']['Match_addr']

        longitude = str(longitude)
        latitude = str(latitude)

        items['external_source'] = self.external_source
        items['external_link'] = external_link
        items['address'] = address
        items['title'] = title
        items['city'] = city
        items['zipcode'] = zipcode
        items['latitude'] = latitude
        items['longitude'] = longitude
        items['description'] = description
        items['property_type'] = "apartment"
        items['square_meters'] = square_feet
        items['room_count'] = room_count
        items['bathroom_count'] = bathroom_count
        items['rent'] = rent
        items['furnished'] = furnished
        items['elevator'] = elevator
        items['balcony'] = balcony
        items['terrace'] = terrace
        items['energy_label'] = energy_label
        items['currency'] = "EUR"
        items['landlord_name'] = landlord_name
        items['landlord_phone'] = "0755731381"
        items['images'] = images

        yield items
