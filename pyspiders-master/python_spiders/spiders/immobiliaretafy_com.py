import requests
import scrapy
from scrapy import Request

from ..helper import remove_unicode_char
from ..loaders import ListingLoader
from scrapy.http import FormRequest


class TammysaddlerComSpider(scrapy.Spider):
    name = 'immobiliaretafy_it'
    allowed_domains = ['www.immobiliaretafy.it']
    start_urls = [
        'https://www.immobiliaretafy.it/web/immobili.asp?num_page=1&language=eng&tipo_contratto=A&cod_categoria=R'
        '&maxann=10&group_cod_agenzia=2981&cod_ordine=O01 '
    ]
    country = 'italy'
    locale = 'it'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'

    next_url_num = 1
    all_rentals = []

    def start_requests(self):
        for url in self.start_urls:
            yield Request(url=url,
                          callback=self.parse,
                          body='',
                          method='GET')

    def parse(self, response):
        if response.css('.pulsante::attr(rel)').extract():
            if ((response.css('.pulsante::attr(rel)').extract())[-1]) == "next":
                rentals = response.css('.item-block::attr(href)').extract()
                for rental in rentals:
                    self.all_rentals.append(rental)
                self.next_url_num += 1
                yield FormRequest.from_response(
                    response=response,
                    formdata={'num_page': str(self.next_url_num)},
                    callback=self.parse)
            else:
                rentals = response.css('.item-block::attr(href)').extract()
                for rental in rentals:
                    self.all_rentals.append(rental)
                for rental in self.all_rentals:
                    rental_url = "https://www.immobiliaretafy.it" + rental
                    yield Request(url=rental_url,
                                  callback=self.parse_area_pages)
        else:
            rentals = response.css('.item-block::attr(href)').extract()
            for rental in rentals:
                self.all_rentals.append(rental)
            for rental in self.all_rentals:
                rental_url = "https://www.immobiliaretafy.it" + rental
                yield Request(url=rental_url,
                              callback=self.parse_area_pages)

    def parse_area_pages(self, response):
        item_loader = ListingLoader(response=response)
        external_id = response.css('#slide_foto strong::text').extract_first()
        external_link = response.url
        title = response.css('h1::text').extract_first()
        description = remove_unicode_char(" ".join(response.css('.imm-det-des::text').extract()))
        property_type = 'apartment'
        square_meters = response.css('#li_superficie strong::text').extract_first()
        room_count = response.css('#li_vani strong::text').extract_first()
        bathroom_count = response.css('#li_bagni strong::text').extract_first()
        address = response.css('.anprovloc ::text').extract_first() + response.css(
            '.anprovloc span::text').extract_first()
        floor_plan_images = response.css('#plangallery .cboxElement::attr(src)').extract()

        zipcode = ''
        latitude = ''
        longitude = ''
        if address: #getting address from address block
            responseGeocode = requests.get(
                "https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/find?f=json&text={address}&maxLocations=1")
            responseGeocodeData = responseGeocode.json()

            longitude = responseGeocodeData['locations'][0]['feature']['geometry']['x']
            latitude = responseGeocodeData['locations'][0]['feature']['geometry']['y']

            responseGeocode = requests.get(
                f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
            responseGeocodeData = responseGeocode.json()
            zipcode = responseGeocodeData['address']['Postal']
            longitude = str(longitude)
            latitude = str(latitude)
        city = response.css('#det_comune .valore::text').extract_first()
        images = response.css('.watermark > img::attr(src)').extract()
        external_images_count = len(images)
        rent = ((((response.css('#sidebar .right::text').extract_first()).split("€ "))[1]).replace(".", ""))
        currency = 'EUR'
        landlord_name = 'immobiliare tafy'

        energy_label = (response.css('#li_clen::text').extract_first())[2]

        furnished = response.css("#det_arredato strong::text").extract_first()
        if furnished:
            furnished = True
        else:
            furnished = False
        floor = ((response.css('#det_piano .valore::text').extract_first()).split(' / '))[0]

        if 'parking' in description:
            parking = True
        else:
            parking = False

        elevator = response.css('#det_ascensore strong::text').extract_first()

        if elevator:
            elevator = True
        else:
            elevator = False

        balcony  = response.css('#det_balcone strong::text').extract_first()

        if balcony:
            balcony  = True
        else:
            balcony  = False

        terrace  = response.css('#det_terrazza strong::text').extract_first()

        if terrace:
            terrace  = True
        else:
            terrace  = False

        swimming_pool  = response.css('#det_piscina strong::text').extract_first()

        if swimming_pool:
            swimming_pool  = True
        else:
            swimming_pool  = False

        item_loader.add_value('zipcode', zipcode)
        item_loader.add_value('city', city)
        item_loader.add_value('longitude', longitude)
        item_loader.add_value('latitude', latitude)

        item_loader.add_value('swimming_pool', swimming_pool)
        item_loader.add_value('terrace', terrace)
        item_loader.add_value('balcony', balcony)
        item_loader.add_value('elevator', elevator)
        item_loader.add_value('parking', parking)
        item_loader.add_value('floor', floor)
        item_loader.add_value('energy_label', energy_label)
        item_loader.add_value('external_id', external_id)
        item_loader.add_value('external_source', self.external_source)
        item_loader.add_value('external_link', external_link)
        item_loader.add_value('title', title)
        item_loader.add_value('description', description)

        item_loader.add_value('property_type', property_type)
        item_loader.add_value('square_meters', square_meters)
        item_loader.add_value('room_count', room_count)
        item_loader.add_value('bathroom_count', bathroom_count)

        item_loader.add_value('address', address)
        item_loader.add_value("images", images)
        item_loader.add_value("external_images_count",
                              external_images_count)
        item_loader.add_value("floor_plan_images", floor_plan_images)
        item_loader.add_value("furnished", furnished)



        # Monetary Status
        item_loader.add_value("rent", int(rent))
        item_loader.add_value("currency", currency)
        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_phone", ' 0577/47921')
        item_loader.add_value("landlord_email", 'info@immobiliaretafy.it')
        item_loader.add_value("landlord_email", 'info@immobiliaretafy.it')



        yield item_loader.load_item()
