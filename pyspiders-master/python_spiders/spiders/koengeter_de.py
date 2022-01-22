# Author: Saimohanraj
from re import DOTALL
import scrapy
from ..loaders import ListingLoader
from ..helper import *


class Koengeter_immobilien_Spider(scrapy.Spider):
    name = "koengeter_immobilien"
    start_urls = ['https://koengeter-immobilien.de/immobilien-leipzig/miete']
    allowed_domains = ["koengeter-immobilien.de"]
    country = 'germany'
    locale = 'de'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    execution_type = 'testing'

    position = 1

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    def parse(self, response, **kwargs):
        for link in response.xpath('//h3[@class="property-title"]'):
            link_name = link.xpath('.//a//text()').get('')
            if 'ERSTBEZUG' in link_name or 'NEU' in link_name:
                links = link.xpath('.//a//@href').get('')
                yield scrapy.Request(links, callback=self.populate_item, dont_filter=True)

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        title = response.xpath(
            '//h1[@class="av-special-heading-tag"]//text()').get('').strip()
        description = response.xpath(
            '//h3[contains(text(),"Beschreibung")]//following-sibling::p[1]//text()').getall()
        description = ' '.join(description)
        description = remove_white_spaces(description)
        description = re.sub(
            r'Sie\s*auf.*?\s*\:.*?www\..*?\.[a-z]+$', '', description)
        property_type_extract = response.xpath(
            '//div[contains(text(),"Objektt")]//following-sibling::div//text()').get('').strip()
        property_type = ''
        if 'Wohnung' in property_type_extract:
            property_type = 'apartment'
        if 'Haus' in property_type_extract:
            property_type = 'house'
        address = response.xpath(
            '//div[contains(text(),"Adresse")]//following-sibling::div//text()').get().replace('\xa0', ' ')
        address = remove_white_spaces(address)
        longitude, latitude = extract_location_from_address(address)
        zipcode, city, address = extract_location_from_coordinates(
            longitude, latitude)
        floor = response.xpath(
            '//div[contains(text(),"Etage")]//following-sibling::div//text()').get('')
        square_meter = response.xpath(
            '//div[contains(text(),"Wohnfläche")]//following-sibling::div//text()').get('').replace('m²', '')
        if square_meter == '':
            square_meter = response.xpath(
                '//div[contains(text(),"Gesamtfläche")]//following-sibling::div//text()').get('').replace('m²', '')
        square_meters = remove_white_spaces(square_meter)
        room_count = response.xpath(
            '//div[contains(text(),"Zimmer")]//following-sibling::div//text()').get('')
        bathroom_count = response.xpath(
            '//div[contains(text(),"Badezimmer")]//following-sibling::div//text()').get('')
        date_format = response.xpath(
            '//div[contains(text(),"Verfügbar ab")]//following-sibling::div//text()').get('')
        date_format = format_date(date_format, "%d.%m.%Y")
        currency = currency_parser("€", "german")
        deposit = response.xpath(
            '//div[contains(text(),"Kaution")]//following-sibling::div//text()').get('')
        deposit = deposit.replace('Kaution:', '').replace('EUR', '')
        rent = response.xpath(
            '//div[contains(text(),"Nettokaltmiete")]//following-sibling::div//text()').get('')
        rent = remove_white_spaces(rent).replace(
            'EUR', '').replace(',', '.').replace('pro Monat', '').replace('.', '').strip()
        balcony_extract = response.xpath(
            '//div[contains(text(),"Balkone")]//following-sibling::div//text()').get('')
        balcony = True if balcony_extract else False
        images = response.xpath(
            '//picture[@class="sp-thumbnail"]//@src').getall()
        landlord_name = response.xpath(
            '//div[@class="panel-body h-card vcard"]//div[contains(text(),"Name")]//following-sibling::div//text()').get('')
        landlord_email = response.xpath(
            '//div[@class="panel-body h-card vcard"]//div[contains(text(),"E-Mail Direkt")]//following-sibling::div//text()').get('')
        landlord_number = response.xpath(
            '//div[@class="panel-body h-card vcard"]//div[contains(text(),"Tel. Durchwahl")]//following-sibling::div//text()').get('')
        external_id = response.xpath(
            '//div[contains(text(),"Objekt ID")]//following-sibling::div//text()').get('')
        utilities = response.xpath(
            '//div[contains(text(),"Nebenkosten")]//following-sibling::div//text()').get('')
        utilities = remove_white_spaces(utilities).replace(
            'EUR', '').replace(',', '.').replace('.', '').strip()
        heating_cost = response.xpath(
            '//div[contains(text(),"Nebenkosten")]//following-sibling::div//text()').get('')
        heating_cost = remove_white_spaces(heating_cost).replace(
            'EUR', '').replace(',', '.').replace('.', '').strip()

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value(
            "bathroom_count", convert_to_numeric(bathroom_count))
        item_loader.add_value("position", self.position)
        item_loader.add_value("description", description)
        item_loader.add_value("latitude", str(latitude))
        item_loader.add_value("longitude", str(longitude))
        item_loader.add_value(
            "square_meters", convert_to_numeric(square_meters))
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("external_id", external_id)
        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("rent", convert_to_numeric(rent))
        item_loader.add_value("landlord_email", landlord_email)
        item_loader.add_value("landlord_phone", landlord_number)
        item_loader.add_value("title", title)
        item_loader.add_value("floor", floor)
        item_loader.add_value("images", images)
        item_loader.add_value("external_images_count", len(images))
        item_loader.add_value("zipcode", zipcode)
        item_loader.add_value("city", city)
        item_loader.add_value("external_id", external_id)
        item_loader.add_value("utilities", utilities)
        item_loader.add_value("heating_cost", heating_cost)
        item_loader.add_value("balcony", balcony)
        item_loader.add_value("deposit", convert_to_numeric(deposit))
        item_loader.add_value("available_date", date_format)
        item_loader.add_value("currency", currency)
        item_loader.add_value("room_count", convert_to_numeric(room_count))
        item_loader.add_value("address", address)
        self.position += 1
        if 'Wohnung' in property_type_extract:
            yield item_loader.load_item()
