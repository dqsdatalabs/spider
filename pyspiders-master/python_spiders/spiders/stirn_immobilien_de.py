# Author: Saimohanraj
import scrapy
from ..loaders import ListingLoader
from ..helper import *


class Stirn_Immobilien_Spider(scrapy.Spider):
    name = "stirn_immobilien"
    start_urls = ['http://www.stirn-immobilien.de/vermietungen.html']
    allowed_domains = ["stirn-immobilien.de"]
    country = 'germany'
    locale = 'de'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    execution_type = 'testing'

    position = 1

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    def parse(self, response, **kwargs):
        for block in response.xpath('//div[@class="tx-nfcimmoscout_listobject"]'):
            title = block.xpath(
                './/a[@class="tx-nfcimmoscout_listobject_headline"]//text()').get().lower()
            if 'doppelparker' not in title and 'büro'not in title and 'vielseitig' not in title:
                link = block.xpath(
                    './/a[@class="tx-nfcimmoscout_listobject_headline"]//@href').get()
                links = response.urljoin(link)
                yield scrapy.Request(links, callback=self.populate_item, dont_filter=True)
        next_page = response.xpath(
            '//a[contains(text(),"weiter")]//@href').get('')
        next_page = response.urljoin(next_page)
        if next_page:
            yield scrapy.Request(url=next_page, callback=self.parse)

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        title = response.xpath(
            '//div[@id="tx-nfcimmoscout-single-header"]//h1//text()').get('').strip()
        description = response.xpath(
            '//span[contains(text(),"Beschreibung")]//parent::p//text()[2]').getall()
        if description == []:
            description = response.xpath(
                '//span[contains(text(),"Lage")]//parent::p//text()[2]').getall()
        description = ' '.join(description)
        description = remove_white_spaces(description)
        square_meters = response.xpath('//td[contains(text(),"Wohnfläche: ")]//following-sibling::td//text()').get(
            '').replace('m', '').replace('approx. ', '').replace('m', '').replace('ca. ', '')
        square_meters = remove_white_spaces(square_meters)
        external_id = response.xpath(
            '//td[contains(text(),"Objekt-Nr")]//following-sibling::td//text()').get('')
        balcony_extract = response.xpath(
            '//td[contains(text(),"Balkon: ")]//following-sibling::td//text()').get('')
        balcony = True if balcony_extract else False
        rent = response.xpath(
            '//td[contains(text(),"Kaltmiete: ")]//following-sibling::td//text()').get('')
        rent = remove_white_spaces(rent).replace(
            '€', '').replace(',', '').replace('-', '').replace('.', '').strip()
        utilities = response.xpath(
            '//td[contains(text(),"Nebenkosten")]//following-sibling::td//text()').get('')
        utilities = remove_white_spaces(utilities).replace(
            '€', '').replace(',', '').replace('-', '').replace('.', '').strip()
        property_type = 'apartment'
        landlord_name = response.xpath(
            '//div[@class="contact_block"]//div//text()').get('')
        landlord_number = response.xpath(
            '//div[contains(text(),"Mobil")]//text()').get('').replace('Mobil:', '')
        date_format = response.xpath(
            '//div[@id="date"]//text()').get('').replace('Datum:', '')
        date_format = remove_white_spaces(date_format)
        date_format = format_date(date_format, "%d.%m.%Y")
        images = response.xpath(
            '//div[@class="tx-nfcimmoscout-picture"]//img//@src').getall()
        terrace_extract = response.xpath(
            '//td[contains(text(),"Terrasse:")]//following-sibling::td//text()').get('')
        terrace = True if terrace_extract else False
        parking_extract = response.xpath(
            '//td[contains(text(),"Garage:")]//following-sibling::td//text()').get('')
        parkings = True if parking_extract else False
        deposit = response.xpath(
            '//td[contains(text(),"Kaution:")]//following-sibling::td//text()').get('')
        deposit = deposit.replace('€ 2 x netto Kaltmiete', '').replace(
            '€', '').replace('-', '').replace('.', '').replace(',', '')
        room_count = response.xpath(
            '//td[contains(text(),"Zimmeranzahl:")]//following-sibling::td//text()').get('')
        commercial_extract = response.xpath(
            '//td[contains(text(),"Gewerbefläche: ")]//following-sibling::td//text()').get('')
        currency = currency_parser("€", "german")
        picture_link = response.xpath(
            '//a[contains(text(),"Bilder")]//@href').get('')
        picture_links = response.urljoin(picture_link)
        external_link = response.url
        if picture_links:
            yield scrapy.Request(picture_links, callback=self.image_extract, meta={'title': title, 'description': description, 'square_meters': square_meters, 'external_id': external_id, 'balcony': balcony, 'rent': rent, 'utilities': utilities, 'property_type': property_type, 'landlord_name': landlord_name, 'landlord_number': landlord_number, 'date_format': date_format, 'terrace': terrace, 'parkings': parkings, 'deposit': deposit, 'room_count': room_count, 'currency': currency, 'commercial_extract': commercial_extract, 'external_link': external_link})
        else:
            if not commercial_extract:
                item_loader.add_value("property_type", property_type)
                item_loader.add_value(
                    "room_count", convert_to_numeric(room_count))
                item_loader.add_value(
                    "square_meters", convert_to_numeric(square_meters))
                item_loader.add_value("available_date", date_format)
                item_loader.add_value("description", description)
                item_loader.add_value("external_link", response.url)
                item_loader.add_value("currency", currency)
                item_loader.add_value(
                    "external_source", self.external_source)
                item_loader.add_value('utilities', utilities)
                item_loader.add_value("title", title)
                item_loader.add_value("position", self.position)
                item_loader.add_value("balcony", balcony)
                item_loader.add_value("external_id", external_id)
                item_loader.add_value("terrace", terrace)
                item_loader.add_value("images", images)
                item_loader.add_value("external_images_count", len(images))
                item_loader.add_value(
                    "rent", convert_to_numeric(rent))
                item_loader.add_value("deposit", convert_to_numeric(deposit))
                item_loader.add_value("landlord_name", landlord_name)
                item_loader.add_value("landlord_phone", landlord_number)
                item_loader.add_value('parking', parkings)
                self.position += 1
                yield item_loader.load_item()

    def image_extract(self, response):
        item_loader = ListingLoader(response=response)
        images = list(set(list(filter(bool, ['http://www.stirn-immobilien.de/'+e.strip(
        ) for e in response.xpath('//div[@class="myGallery"]//a//@href').extract()]))))
        title = response.meta['title']
        currency = response.meta['currency']
        description = response.meta['description']
        external_link = response.meta['external_link']
        commercial_extract = response.meta['commercial_extract']
        square_meters = response.meta['square_meters']
        external_id = response.meta['external_id']
        balcony = response.meta['balcony']
        rent = response.meta['rent']
        utilities = response.meta['utilities']
        property_type = response.meta['property_type']
        landlord_name = response.meta['landlord_name']
        landlord_number = response.meta['landlord_number']
        date_format = response.meta['date_format']
        terrace = response.meta['terrace']
        parking = response.meta['parkings']
        deposit = response.meta['deposit']
        room_count = response.meta['room_count']

        item_loader.add_value("property_type", property_type)
        item_loader.add_value("room_count", convert_to_numeric(room_count))
        item_loader.add_value(
            "square_meters", convert_to_numeric(square_meters))
        item_loader.add_value("available_date", date_format)
        item_loader.add_value("description", description)
        item_loader.add_value("external_link", external_link)
        item_loader.add_value("currency", currency)
        item_loader.add_value(
            "external_source", self.external_source)
        item_loader.add_value('utilities', utilities)
        item_loader.add_value("title", title)
        item_loader.add_value("position", self.position)
        item_loader.add_value("balcony", balcony)
        item_loader.add_value("external_id", external_id)
        item_loader.add_value("terrace", terrace)
        item_loader.add_value("images", images)
        item_loader.add_value("external_images_count", len(images))
        item_loader.add_value(
            "rent", convert_to_numeric(rent))
        item_loader.add_value("deposit", convert_to_numeric(deposit))
        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_phone", landlord_number)
        item_loader.add_value('parking', parking)
        self.position += 1
        if not commercial_extract:
            yield item_loader.load_item()
