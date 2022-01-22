import scrapy
from scrapy import Request
from ..items import ListingItem
from ..loaders import ListingLoader
import requests

class ArcaseGroupSpider(scrapy.Spider):
    name = 'arcase_group'
    allowed_domains = ['arcase.it']
    start_urls = ['https://www.arcase.it/immobili-arcase?ricerca=immobili&q=&provincia=&comune=&quartiere=&cat_contratto=affitto&prezzo_da=&prezzo_a=&superficie_da=&superficie_a=&ricerca_side=1']
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
        area_urls = response.css('.house-name::attr(href)').extract()
        area_urls = ['https://www.arcase.it/' + x for x in area_urls]
        for area_url in area_urls:
            yield Request(url=area_url,
                          callback=self.parse_pages)

    def parse_pages(self, response):
        try:
            rented = response.css(".aff_dett::text")[0].extract()
            if "affittato" in rented:
                return
        except:
            pass

        title = response.css("h1::text")[0].extract()
        if "uffici" in title or "Locazione" in title:
            return

        items = ListingItem()

        external_link = str(response.request.url)
        external_id = response.css(".codrif::text")[0].extract()
        description = response.css(".col-lg-12 p::text").extract()
        description = ' '.join(description)
        address = response.css("tr:nth-child(1) .text-right::text")[0].extract()

        rent = response.xpath("//*[contains(text(), '€')]").css("::text")[0].extract()
        rent = rent.strip()[:-2]
        if any(char.isdigit() for char in rent):
            rent = int(''.join(x for x in rent if x.isdigit()))
        else:
            rent = " "

        square_meters = response.css("tr:nth-child(2) .text-right::text")[0].extract()
        if any(char.isdigit() for char in square_meters):
            square_meters = int(''.join(x for x in square_meters if x.isdigit()))
        else:
            square_meters = " "

        room_count = response.css("#caratteristiche_Caratteristiche > tbody > tr:nth-child(4) > td.text-right::text")[0].extract()
        if any(char.isdigit() for char in room_count):
            room_count = int(''.join(x for x in room_count if x.isdigit()))
        else:
            room_count = 2
        bathroom_count = None
        balcony = None
        parking = None
        try:
            bathroom_count = response.css("#caratteristiche_Caratteristiche > tbody > tr:nth-child(5) > td.text-right::text")[0].extract()
            if any(char.isdigit() for char in bathroom_count):
                bathroom_count = int(''.join(x for x in bathroom_count if x.isdigit()))
            else:
                bathroom_count = " "
        except:
            pass

        try:
            balcony = response.css("#caratteristiche_Caratteristiche > tbody > tr:nth-child(8) > td.text-right::text")[0].extract()
            balcony = True
        except:
            pass

        try:
            parking = response.css("#caratteristiche_Caratteristiche > tbody > tr:nth-child(8) > td.text-right::text")[0].extract()
            parking = True
        except:
            pass

        latlng = response.css('script:contains("lat:")::text').get()
        coords = latlng.split("lat:")[1].split("};")[0]
        coords = coords.split(",")
        latitude = coords[0]
        latitude = latitude.replace("'", '').strip()
        longitude = coords[1]
        longitude = longitude.replace("lng:", '').strip()

        responseGeocode = requests.get(
            f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
        responseGeocodeData = responseGeocode.json()
        zipcode = responseGeocodeData['address']['Postal']
        city = responseGeocodeData['address']['City']
        address = responseGeocodeData['address']['Match_addr']

        images = response.css('#galleria img::attr(data-src)').extract()
        images = ['https://www.arcase.it/' + x for x in images]

        items['external_source'] = self.external_source
        items['external_link'] = external_link
        items['external_id'] = external_id
        items['address'] = address
        items['zipcode'] = zipcode
        items['city'] = city
        items['title'] = title
        items['description'] = description
        items['property_type'] = "apartment"
        items['square_meters'] = square_meters
        items['room_count'] = room_count
        items['bathroom_count'] = bathroom_count
        items['latitude'] = latitude
        items['longitude'] = longitude
        items['rent'] = rent
        items['parking'] = parking
        items['balcony'] = balcony
        items['floor'] = "1"
        items['currency'] = "EUR"
        items['landlord_name'] = "ARCASE Group"
        items['landlord_phone'] = "011504333"
        items['landlord_email'] = "direzione@arcase.it"
        items['images'] = images

        yield items
