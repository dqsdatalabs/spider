# Author: Saimohanraj
import scrapy
from ..loaders import ListingLoader
from ..helper import *


class Wohnderworld_Spider(scrapy.Spider):
    name = 'wohnderworld'
    start_urls = ['https://www.wohnderworld.de/immobilien']
    allowed_domains = ["wohnderworld.de"]
    execution_type = 'testing'
    country = 'germany'
    locale = 'de'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"

    position = 1

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url=url, callback=self.parse, meta={'property_type': 'apartment'})

    def parse(self, response, **kwargs):
        property_type = response.meta['property_type']
        for links in response.xpath('//div[@class="article-box-wrap"]'):
            link = links.xpath(
                './/a[@class="btn btn-primary btn-readmore"]//@href').get('')
            link = response.urljoin(link)
            if link:
                yield scrapy.Request(link, callback=self.populate_item,  meta={'property_type': property_type})
        next_page = response.xpath('//li[@class="pager-next"]//@href').get('')
        next_page = response.urljoin(next_page)
        if next_page:
            yield scrapy.Request(url=next_page, callback=self.parse, meta={'property_type': property_type})

    def populate_item(self, response):
        property_type = response.meta['property_type']
        item_loader = ListingLoader(response=response)
        title = response.xpath('//h1[@class="title"]//text()').get('')
        address = response.xpath(
            '//div[@class="article-body-section property-options"]//address//text()').get('')
        description = response.xpath(
            '//h6[contains(text(),"Objektbeschreibung")]//parent::div//p//text()').get('').replace('\xa0', '')
        longitude, latitude = extract_location_from_address(address)
        zipcode, city, address = extract_location_from_coordinates(
            longitude, latitude)
        floor = response.xpath(
            '//th[contains(text(),"Etage")]//following-sibling::td//text()').get('')
        square_meters = response.xpath(
            '//th[contains(text(),"Wohnfläche")]//following-sibling::td//text()').get('').strip()
        square_meters = square_meters.replace('m²', '').replace(',', '.')
        square_meters = remove_white_spaces(square_meters)
        utilities = response.xpath(
            '//th[contains(text(),"Nebenkosten")]//following-sibling::td//text()').get('').strip()
        utilities = remove_white_spaces(utilities).replace('€', '').replace(
            ',', '.').replace('.00', '').replace('.', '').strip()
        room_count = response.xpath(
            '//th[contains(text(),"Zimmer")]//following-sibling::td//text()').get('')
        bathroom_count = response.xpath(
            '//th[contains(text(),"Badezimmer")]//following-sibling::td//text()').get('')
        date_format = response.xpath(
            '//th[contains(text(),"Verfügbar ab")]//following-sibling::td//text()').get('')
        date_format = format_date(date_format, "%d.%m.%Y")
        currency = currency_parser("€", "german")
        images = response.xpath(
            '//div[@class="clearfix swiper-slide-wrapper"]//a//@href').getall()
        rent = response.xpath(
            '//th[contains(text(),"Kaltmiete")]//following-sibling::td//text()').get('')
        rent = remove_white_spaces(rent).replace(
            '€', '').replace(',', '.').replace('.00', '').replace('.', '').strip()
        deposit = response.xpath(
            '//th[contains(text(),"Kaution")]//following-sibling::td//text()').get('')
        deposit = deposit.replace('Kaution:', '').replace(
            '(auch in 3 Raten)', '').replace('Kaltmieten', '').strip()
        if len(deposit) is 1:
            deposit = int(deposit) * int(rent)
        landlord_name = response.xpath(
            '//div[@class="clearfix vcard"]//h6//text()').get('')
        if landlord_name is '':
            landlord_name = 'Melle Kerstin'
        landlord_phone = response.xpath(
            '//div[@class="clearfix vcard"]//p//text()').get('')
        if landlord_phone is '':
            landlord_phone = '0341-22 55 55 55'
        landlord_phone = remove_white_spaces(
            landlord_phone).replace('Tel.: ', '').strip()
        parking = response.xpath(
            '//th[contains(text(),"Art u. Objekttyp")]//following-sibling::td//text()').get('')
        parking = remove_white_spaces(parking)

        purchasing_price = response.xpath(
            '//th[contains(text(),"Kaufpreis")]//following-sibling::td//text()')
        landlord_email = 'Wohnen@immobilienbueros.de'
        item_loader.add_value("external_link", response.url)
        item_loader.add_value(
            "external_source", self.external_source)
        item_loader.add_value("position", self.position)
        item_loader.add_value("title", title)
        item_loader.add_value("description", description)
        item_loader.add_value(
            "room_count", convert_to_numeric(room_count))
        item_loader.add_value("address", address)
        item_loader.add_value("latitude", str(latitude))
        item_loader.add_value("images", images)
        item_loader.add_value("rent", convert_to_numeric(rent))
        item_loader.add_value("utilities", utilities)
        item_loader.add_value("external_images_count", len(images))
        item_loader.add_value("longitude", str(longitude))
        item_loader.add_value("zipcode", zipcode)
        item_loader.add_value("city", city)
        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("landlord_email", landlord_email)
        item_loader.add_value("floor", floor)
        item_loader.add_value("property_type", property_type)
        item_loader.add_value(
            "square_meters", convert_to_numeric(square_meters))
        item_loader.add_value("deposit", convert_to_numeric(deposit))
        item_loader.add_value("available_date", date_format)
        item_loader.add_value("currency", currency)
        item_loader.add_value(
            "bathroom_count", convert_to_numeric(bathroom_count))
        if 'Wohnung' in parking and len(purchasing_price) == 0:
            self.position += 1
            yield item_loader.load_item()
