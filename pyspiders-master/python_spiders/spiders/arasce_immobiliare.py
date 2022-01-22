import scrapy
from scrapy import Request
from ..items import ListingItem
from ..loaders import ListingLoader
import requests

class ArasceImmobiliareSpider(scrapy.Spider):
    name = 'arasce_immo'
    allowed_domains = ['arasce.it']
    start_urls = ['https://www.arasce.it//elenco_immobili_f.asp?idcau=2']
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
        area_urls = response.css('#pager .button::attr(href)').extract()
        area_urls = ['https://www.arasce.it//' + x for x in area_urls]
        for area_url in area_urls:
            yield Request(url=area_url,
                          callback=self.parse_pages)

    def parse_pages(self, response):
        title = response.css(".scheda_immobile .titling-search::text")[0].extract()
        if "Appartamento" not in title:
            return

        items = ListingItem()

        external_link = str(response.request.url)
        description = response.css(".eight p::text")[0].extract()

        rent = None
        try:
            rent = response.css("#dato_prezzo::text")[0].extract()
            if any(char.isdigit() for char in rent):
                rent = int(''.join(x for x in rent if x.isdigit()))
            else:
                return
        except:
            pass

        square_meters = response.css(".padded+ .one-third-liquid tr:nth-child(2) td::text")[0].extract()
        if any(char.isdigit() for char in square_meters):
            square_meters = int(''.join(x for x in square_meters if x.isdigit()))
        else:
            square_meters = " "

        room_count = response.css(".padded+ .one-third-liquid tr:nth-child(8) td::text")[0].extract()
        room_count = int(room_count)

        external_id = response.css(".add-top h5:nth-child(1)::text")[0].extract()
        external_id = external_id[5:]

        elevator = None
        balcony = None
        terrace = None

        try:
            elevator = response.css(".one-third-liquid:nth-child(3) tr:nth-child(7) td::text")[0].extract()
            elevator = elevator.strip()
            if "NO" in elevator:
                elevator = False
            else:
                elevator = True
        except:
            pass

        try:
            balcony = response.css("tr:nth-child(9) td::text")[0].extract()
            if "NO" in balcony:
                balcony = False
            else:
                balcony = True
        except:
            pass

        try:
            terrace = response.css("tr:nth-child(10) td::text")[0].extract()
            if "NO" in terrace:
                terrace = False
            else:
                terrace = True
        except:
            pass
        energy_label = response.css(".one-half-liquid tr:nth-child(1) td::text")[0].extract()
        floor = response.css(".one-third-liquid:nth-child(3) tr:nth-child(5) td::text")[0].extract()
        images1 = response.css('body > div:nth-child(3) > div > section > div > div > a::attr(href)').extract()
        images2 = response.css('#fancybox-manual-c::attr(href)').extract()

        images1 = images1[:len(images1) - 2]
        images = images2 + images1
        images = ['arasce.it/' + x for x in images]

        latlng = response.css('script:contains("myLatlng")::text').get()
        coords = latlng.split(".LatLng(")[1].split("');")[0]
        coords = coords.split(",")
        latitude = coords[0]
        longitude = coords[1]
        latitude = latitude.replace("'", '')
        longitude = longitude.replace("'", '')

        responseGeocode = requests.get(
            f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
        responseGeocodeData = responseGeocode.json()

        address = responseGeocodeData['address']['Match_addr']
        city = responseGeocodeData['address']['City']
        zipcode = responseGeocodeData['address']['Postal']

        items['external_source'] = self.external_source
        items['external_link'] = external_link
        items['external_id'] = external_id
        items['title'] = title
        items['description'] = description
        items['property_type'] = "apartment"
        items['square_meters'] = square_meters
        items['room_count'] = room_count
        items['rent'] = rent
        items['address'] = address
        items['city'] = city
        items['zipcode'] = zipcode
        items['latitude'] = latitude
        items['longitude'] = longitude
        items['elevator'] = elevator
        items['balcony'] = balcony
        items['terrace'] = terrace
        items['floor'] = floor
        items['energy_label'] = energy_label
        items['currency'] = "EUR"
        items['landlord_name'] = "Arasce Immobiliare"
        items['landlord_phone'] = "0182647024"
        items['landlord_email'] = "info@arasce.com"
        items['images'] = images

        yield items
