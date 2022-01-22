# -*- coding: utf-8 -*-
# Author: LOGALINGAM
import scrapy
from ..loaders import ListingLoader
from ..helper import *


class HelmutHeinrichImmobilienDeSpider(scrapy.Spider):
    name = "helmut_heinrich_immobilien_de"
    start_urls = ['https://helmut-heinrich-immobilien.de/mietobjekte/']
    allowed_domains = ["helmut-heinrich-immobilien.de"]
    country = 'germany'
    locale = 'de'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    execution_type = 'testing'

    position = 1

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    def parse(self, response, **kwargs):
        for link in response.xpath('//div[@class="inx-property-list__item-wrap"]'):
            links = link.xpath(
                './/div[@class="inx-property-list-item__title uk-card-title"]//@href').get('').strip()
            title = link.xpath(
                './/div[@class="inx-property-list-item__title uk-card-title"]//a//text()').get('')
            rental = link.xpath(
                './/div[@class="inx-property-list-item__labels uk-position-top-right"]//span/text()').get('')
            city = link.xpath(
                './/i[@class="inx-core-detail-icon flaticon-placeholder"]//parent::div[1]//text()').getall()
            city = ' '.join(city).strip()
            if 'Zu Vermieten' in rental:
                yield scrapy.Request(links, callback=self.populate_item, meta={'title': title, 'city': city}, dont_filter=True)

    def populate_item(self, response):
        title = response.meta['title'].strip()
        city = response.meta['city']
        square_meters = int(float(response.xpath(
            '//span[contains(text(),"Wohnfläche:")]//following-sibling::span//text()').get('').strip().replace(",", ".").replace("m²", "")))
        room_count = extract_number_only(response.xpath(
            '//span[contains(text(),"Zimmer insgesamt:")]//following-sibling::span//text()').get(''))
        bathroom_count = extract_number_only(response.xpath(
            '//span[contains(text(),"Badezimmer")]//following-sibling::span//text()').get(''))
        if bathroom_count == 0:
            bathroom_text = ' '.join(response.xpath('//div[@class="inx-description-text"]//text()').getall())
            bathroom_count = re.findall(r'\s+([\d]+)\s*Bädern',str(bathroom_text),re.I)[0]
        date_extract = response.xpath(
            '//span[contains(text(),"Immobilie ist verfügbar ab:")]//following-sibling::span//text()').get('').replace('ab', '').strip()
        date_format = format_date(date_extract, "%d.%m.%Y")
        currency = currency_parser("€", "german")
        floor = response.xpath(
            '//span[contains(text(),"Etagen")]//following-sibling::span//text()').get('')
        landlord_name = response.xpath(
            '//h3[@class="inx-team-single-agent__name uk-margin-remove-top uk-margin-small-bottom"]//text()').get('')
        landlord_number = response.xpath(
            '//span[@uk-icon="receiver"]//parent::div//parent::div//text()[1]').getall()[-1]
        landlord_number = remove_white_spaces(
            landlord_number) if re.search(r'\d+', landlord_number) else ''
        landlord_email = response.xpath(
            '//div[@class="inx-team-single-agent__element-value"]//text()').get('')
        deposit = response.xpath(
            '//span[contains(text(),"Kaution")]//following-sibling::span//text()').get('')
        deposit = deposit.replace('Kaution:', '').replace('€', '').replace('.','')
        
        heating_cost = response.xpath(
            '//span[contains(text(),"Warmmiete")]//following-sibling::span//text()').get('')
        heating_cost = heating_cost.replace('Kaution:', '').replace('€', '').strip()
        utilities = response.xpath(
            '//span[contains(text(),"monatl")]//following-sibling::span//text()').get('')
        utilities = utilities.replace('€', '').strip()
        rent = response.xpath(
            '//span[contains(text(),"Kaltmiete:")]//following-sibling::span//text()').get('')
        rent = remove_white_spaces(rent).replace(
            '€', '').replace(',', '.').replace('pro Monat', '').replace('.', '').strip()
        images = response.xpath(
            '//ul[@class="inx-gallery__images uk-slideshow-items"]//li//@src').getall()
        latitude, longitude = extract_location_from_address(city)
        zipcode, city,address  = extract_location_from_coordinates(
            latitude, longitude)
        property_type = "apartment"
        description = response.xpath(
            '//div[@class="inx-description-text"]//text()').getall()
        description = ' '.join(description)
        description = remove_white_spaces(description)

        heating_cost = int(heating_cost) - int(rent)
        
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value(
            "bathroom_count", convert_to_numeric(bathroom_count))
        item_loader.add_value("position", self.position)
        item_loader.add_value("description", description)
        item_loader.add_value("address", address) # String
        item_loader.add_value("latitude", str(latitude))
        item_loader.add_value("longitude", str(longitude))
        item_loader.add_value(
            "square_meters", convert_to_numeric(square_meters))
        item_loader.add_value("property_type", property_type)
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
        item_loader.add_value("deposit", convert_to_numeric(deposit))
        item_loader.add_value("utilities", convert_to_numeric(utilities)) # Int
        item_loader.add_value("heating_cost", convert_to_numeric(heating_cost)) # Int
        item_loader.add_value("available_date", date_format)
        item_loader.add_value("currency", currency)
        item_loader.add_value("room_count", convert_to_numeric(room_count))
        self.position += 1
        yield item_loader.load_item()
