import scrapy
from scrapy import Request
from ..items import ListingItem
from ..loaders import ListingLoader
import requests
import json
from scrapy.http.request.json_request import JsonRequest

class MarinelliBalzeranoSpider(scrapy.Spider):
    name = 'marinelli_balzerano'
    allowed_domains = ['immobili.marinellibalzerano.com']
    start_urls = ['https://immobili.marinellibalzerano.com/api/AdsSearch/PostMiniFichasAdsMaps']
    execution_type = 'development'
    country = 'italy'
    locale ='it'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)

    def start_requests(self):
        yield JsonRequest(url='https://immobili.marinellibalzerano.com/api/AdsSearch/PostMiniFichasAdsMaps',
                    callback=self.parse,
                    data ={"currentPage": 3, "itemsPerPage": 20},
                    method='POST')

    def parse(self, response):
        parsed_response = json.loads(response.body)
        for item in parsed_response['ads']:
            is_rent = item["IsRent"]
            if is_rent:
                url = str(item['id'])
                url = 'https://immobili.marinellibalzerano.com/ad/' + url
                yield Request(url=url,
                              callback=self.parse_property,
                              meta={"item": item}
                              )


    def parse_property(self, response):
        item = response.meta["item"]
        rent = int(item["prices"]["byOperation"]["RENT"]["price"])
        title = response.css("#titulo::text")[0].extract()
        if "Ufficio" in title or "Locale" in title or "Capannone" in title or "Garage" in title or "Cantina" in title:
            return

        items = ListingItem()

        external_link = str(response.request.url)
        external_id = response.css(".property-ref::text")[0].extract()
        external_id = (external_id[4:]).strip()
        description = response.css(".contitle::text")[0].extract()
        square_meters = int(item["property"]["housing"]["propertyArea"])
        room_count = int(item["property"]["housing"]["roomNumber"])
        bathroom_count = int(item["property"]["housing"]["bathNumber"])

        floor = None
        parking = None
        terrace = None

        try:
            parking = item["property"]["housing"]["parkingSpace"]["hasParkingSpace"]
        except:
            pass
        try:
            terrace = item["property"]["housing"]["hasTerrace"]
        except:
            pass
        try:
            floor = item["property"]["address"]["floorNumber"]
        except:
            pass

        amenities = response.css('#caracteristicas ul:nth-child(5) li::text').extract()
        energy_label = amenities[-1][-1]

        elevator = None
        if 'con ascensore' in amenities:
            elevator = True

        longitude = item["property"]["address"]["coordinates"]["longitude"]
        latitude = item["property"]["address"]["coordinates"]["latitude"]

        responseGeocode = requests.get(
            f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
        responseGeocodeData = responseGeocode.json()

        address = responseGeocodeData['address']['Match_addr']
        city = responseGeocodeData['address']['City']
        zipcode = responseGeocodeData['address']['Postal']


        image_list = item["multimedias"]["pictures"]
        images = []
        for x in range(len(image_list)):
            image_id = item["multimedias"]["pictures"]
            for i in image_id:
                path = i['masterPath']
                name = i['masterName']
                image = "https://img3.idealista.it/blur/HOME_WI_1500/0/" + path + name
                images.append(image)



        items['external_source'] = self.external_source
        items['external_link'] = external_link
        items['external_id'] = external_id
        items['title'] = title
        items['description'] = description
        items['address'] = address
        items['city'] = city
        items['zipcode'] = zipcode
        items['longitude'] = str(longitude)
        items['latitude'] = str(latitude)
        items['property_type'] = "apartment"
        items['square_meters'] = square_meters
        items['room_count'] = room_count
        items['bathroom_count'] = bathroom_count
        items['rent'] = rent
        items['parking'] = parking
        items['terrace'] = terrace
        items['floor'] = floor
        items['elevator'] = elevator
        items['energy_label'] = energy_label
        items['currency'] = "EUR"
        items['landlord_name'] = "Marinelli Balzerano"
        items['landlord_phone'] = "+39081400196"
        items['landlord_email'] = "info@marinellibalzerano.com"
        items['images'] = images
        yield items