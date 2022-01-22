import requests
import scrapy
from scrapy import Request

from ..helper import remove_unicode_char
from ..loaders import ListingLoader
from scrapy.http import FormRequest


class ImmobiliarefaroItSpider(scrapy.Spider):
    name = 'immobiliarefaro_it'
    allowed_domains = ['www.immobiliarefaro.it']
    start_urls = [
        'https://www.immobiliarefaro.it/web/immobili.asp?tipo_contratto=A&cod_categoria=R&cod_ordine=O08&language=eng']
    country = 'italy'
    locale = 'it'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'

    next_url_num = 1
    all_rentals = []

    def parse(self, response):

        for start_url in self.start_urls:
            yield Request(url=start_url,
                          callback=self.parse_area)

    def parse_area(self, response):
        if ((response.css('.pulsante::attr(rel)').extract())[-1]) == "next":
            rentals = response.css('.sfondo_colore1 a::attr(href)').extract()
            for rental in rentals:
                self.all_rentals.append(rental)
            self.next_url_num += 1
            yield FormRequest.from_response(
                response=response,
                formdata={'num_page': str(self.next_url_num)},
                callback=self.parse_area)
        else:
            rentals = response.css('.sfondo_colore1 a::attr(href)').extract()
            for rental in rentals:
                self.all_rentals.append(rental)
            for rental in self.all_rentals:
                rental_url = "https://www.immobiliarefaro.it" + rental
                yield Request(url=rental_url,
                              callback=self.parse_area_pages)

    def parse_area_pages(self, response):
        item_loader = ListingLoader(response=response)

        external_id = response.css('#det_rif .valore::text').extract_first()
        external_link = response.request.url
        title = response.css('.no-btm::text').extract_first()
        description = response.css('.imm-det-des::text').extract_first()
        property_type = "apartment"
        square_meters = response.css('#full-list1 #det_superficie::attr(data-valore)').extract_first()
        room_count = response.css('#full-list1 #det_vani::attr(data-valore)').extract_first()
        bathroom_count = response.css('#full-list1 #det_camere::attr(data-valore)').extract_first()
        province = response.css('#det_prov .valore::text').extract_first()
        single_address = response.css('#det_indirizzo .valore::text').extract_first()
        if single_address:
            single_address = single_address[3:]
        area = response.css('#det_zona .valore::text').extract_first()
        city = response.css('#det_comune .valore::text').extract_first()
        address = single_address, area, province, city
        images = response.css('#freewall div::attr(data-img)').extract()
        external_images_count = len(images)

        responseGeocode = requests.get(
            f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/find?f=json&text={address}&maxLocations=1")
        responseGeocodeData = responseGeocode.json()

        longitude = responseGeocodeData['locations'][0]['feature']['geometry']['x']
        latitude = responseGeocodeData['locations'][0]['feature']['geometry']['y']

        responseGeocode = requests.get(
            f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
        responseGeocodeData = responseGeocode.json()
        zipcode = responseGeocodeData['address']['Postal']
        city = responseGeocodeData['address']['City']
        address = responseGeocodeData['address']['Match_addr']

        longitude = str(longitude)
        latitude = str(latitude)
        energy_label = response.css('#det_cl_en .valore::text').extract_first()
        furnished = response.css('#det_arredato strong').extract_first()
        if furnished:
            furnished = True
        else:
            furnished = False
        floor = ((response.css('#det_piano .valore::text').extract_first()).split("/"))[0]
        rent = int(float(((response.css('.price::text').extract_first()).split("€ "))[1]))

        parking = response.css('#det_parcheggio .valore::text').extract_first()
        if parking:
            parking = True
        else:
            parking = False

        elevator = response.css('#det_ascensore strong::text').extract_first()
        if elevator:
            elevator = True
        else:
            elevator = False

        balcony = response.css('#det_balcone strong::text').extract_first()
        if balcony:
            balcony = True
        else:
            balcony = False

        terrace = response.css('#det_terrazza strong::text').extract_first()
        if terrace:
            terrace = True
        else:
            terrace = False

        swimming_pool = response.css('#det_piscina strong::text').extract_first()
        if swimming_pool:
            swimming_pool = True
        else:
            swimming_pool = False

        currency = 'EUR'
        landlord_name = 'FARO IMMOBILIARE'
        item_loader.add_value('external_id', external_id)
        item_loader.add_value('external_source', self.external_source)
        item_loader.add_value('external_link', external_link)
        item_loader.add_value('title', title)
        item_loader.add_value('description', remove_unicode_char(description))

        item_loader.add_value('property_type', property_type)
        item_loader.add_value('square_meters', int(square_meters))
        item_loader.add_value('room_count', room_count)
        item_loader.add_value('bathroom_count', bathroom_count)

        item_loader.add_value('address', address)
        item_loader.add_value('city', city)
        item_loader.add_value('zipcode', zipcode)
        item_loader.add_value('longitude', longitude)
        item_loader.add_value('latitude', latitude)

        item_loader.add_value("images", images)
        item_loader.add_value("external_images_count",
                              external_images_count)

        # Monetary Status
        item_loader.add_value("rent", rent)
        item_loader.add_value("currency", currency)

        item_loader.add_value("energy_label", energy_label)
        item_loader.add_value("furnished", furnished)
        item_loader.add_value("floor", floor)
        item_loader.add_value("parking", parking)
        item_loader.add_value("elevator", elevator)
        item_loader.add_value("balcony", balcony)
        item_loader.add_value("terrace", terrace)
        item_loader.add_value("swimming_pool", swimming_pool)
        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_phone", "+39-0921-935143")


        yield item_loader.load_item()
