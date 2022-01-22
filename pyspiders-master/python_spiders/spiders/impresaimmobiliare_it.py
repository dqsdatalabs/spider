# -*- coding: utf-8 -*-
# Author: Abdulrahman Abbas


import scrapy
from ..loaders import ListingLoader
from ..helper import extract_number_only, extract_lat_long, extract_location_from_coordinates, extract_utilities, \
    string_found


class ImpresaImmobiliare(scrapy.Spider):
    name = 'impresaimmobiliare'
    allowed_domains = ['impresaimmobiliare.com']
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = 'ImpresaImmobiliare_PySpider_Italy'
    start_urls = [
        'https://www.impresaimmobiliare.com/immobili/?filtro_motivazione=2&filtro_zona=&filtro_lifestyle=&filtro_comune=&filtro_categoria=&filtro_residenziale=1&form_submit=',
        'https://www.impresaimmobiliare.com/immobili/?filtro_motivazione=2&n_pagina=1&filtro_residenziale=1',
    ]

    def parse(self, response):
        apartment_page_links = response.xpath('//div[@class="wpb_wrapper"]//a[@class="wpb_single_image wpb_content_element vc_align_left immagine-immobile"]')
        yield from response.follow_all(apartment_page_links, self.parse_info)

    def parse_info(self, response):
        item_loader = ListingLoader(response=response)

        description = response.xpath('//div[@class="wpb_wrapper"]//p//text()').getall()
        all_description = " ".join(description)

        title = response.xpath('//div[@class="wpb_wrapper"]//h1[@class="titolo-immobile"]//text()').getall()
        full_title = " ".join(title)

        apartment_content = response.xpath( '//div[@class="icon_title_holder"]//h6[@class="icon_title"]//text()').getall()
        rent = extract_number_only(response.xpath('//div[@class="wpb_wrapper"]//h3//text()').get())
        position = extract_lat_long(response.xpath('//div[@class="vc_column-inner"]//script//text()').get())
        lat = position[0]
        long = position[1]
        location = extract_location_from_coordinates(long, lat)

        external_id = response.xpath('//div[@class="wpb_wrapper"]//h6//text()').get().split(":")
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_id", external_id[1])

        item_loader.add_value('title', full_title)
        item_loader.add_value('description', all_description)

        item_loader.add_value('square_meters', apartment_content[0].replace(".00", " "))
        item_loader.add_value('bathroom_count', apartment_content[6])
        item_loader.add_value('room_count', apartment_content[2])

        item_loader.add_value('currency', "EUR")
        item_loader.add_value('rent', rent)
        item_loader.add_value('utilities', extract_utilities('€', all_description))

        images = response.xpath('//div[@class="wpb_wrapper"]//@src').getall()
        item_loader.add_value("images", images)
        item_loader.add_value("external_images_count", len(images))

        property_type = ['Appartamento']
        if string_found(property_type, full_title):
            item_loader.add_value('property_type', 'apartment')
        else:
            item_loader.add_value('property_type', "house")

        item_loader.add_value('longitude', long)
        item_loader.add_value('latitude', lat)
        item_loader.add_value('address', location[2])
        item_loader.add_value('city', location[1])
        item_loader.add_value('zipcode', location[0])

        swimming_pool = ['piscina']
        item_loader.add_value('swimming_pool', string_found(swimming_pool, all_description))

        furnished = ['arredato']
        item_loader.add_value('furnished', string_found(furnished, all_description))

        balcony = ['Balcone', 'balacone']
        item_loader.add_value('balcony', string_found(balcony, all_description))

        elevator = ['ascensore']
        item_loader.add_value('elevator', string_found(elevator, all_description))

        terrace = ['terrazza']
        item_loader.add_value('terrace', string_found(terrace, all_description))

        parking = ['posto auto']
        item_loader.add_value('parking', string_found(parking, all_description))

        washing_machine = ['lavanderia']
        item_loader.add_value('washing_machine', string_found(washing_machine, all_description))

        item_loader.add_value('landlord_name', "IMPREsa Treviso Latin Quarter ")
        item_loader.add_value('landlord_phone', "04221784050")
        item_loader.add_value('landlord_email', "treviso@impresaimmobiliare.com")

        yield item_loader.load_item()
