# -*- coding: utf-8 -*-
# Author: Abdulrahman Abbas


import scrapy

from ..loaders import ListingLoader
from ..helper import extract_number_only, extract_lat_long, extract_utilities, extract_location_from_coordinates, string_found


class Finim(scrapy.Spider):
    name = 'finim_it'
    allowed_domains = ['finim-online.com']
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = 'finim_PySpider_italy'
    start_urls = ['https://finim-online.com/elenco_immobili.asp?prezzo_min=&prezzo_max=&mq_min=&mq_max=&zona=&page=1&order=&r=&scopo=2&tipologia=&mq=#']
    position = 1

    def parse(self, response):
        apartment_page_links = response.xpath('//div[@class="row row-50"]//h4//a')
        yield from response.follow_all(apartment_page_links, self.parse_info)

        pagination_links = response.xpath('//ul[@class="pagination-custom"]//a')

        yield from response.follow_all(pagination_links, self.parse)

    def parse_info(self, response):

        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value('property_type', 'apartment')
        item_loader.add_value("position", self.position) # Int

        description = response.xpath('//div[@class="col-lg-7 col-xl-8"]//p//text()').get()
        all_futures = " ".join(response.xpath('//ul[@class="list-marked-2 layout-2"]//li//text()').getall())
        position = extract_lat_long(response.xpath('//div[@class="block-group-item"]//script//text()').get())
        apartment = response.xpath('//ul[@class="features-block-list"]/li//text()').getall()

        images = response.xpath('//section[@class="section section-md bg-gray-12"]//img//@src').getall()
        rent = response.xpath('//div[@class="slick-slider-price"]//text()').get()

        item_loader.add_xpath('title', '//h2[@class="breadcrumbs-custom-title"]//text()')
        item_loader.add_value('description', description)

        item_loader.add_value('address', extract_location_from_coordinates(position[1],position[0])[2])
        item_loader.add_value('city', extract_location_from_coordinates(position[1],position[0])[1])
        item_loader.add_value('zipcode', extract_location_from_coordinates(position[1],position[0])[0])
        item_loader.add_value('longitude', position[1])
        item_loader.add_value('latitude', position[0])

        item_loader.add_value("images", images)
        item_loader.add_value("external_images_count", len(images))

        bathroom_count = description.count("bagno")
        bathroom = ["doppi servizi", 'due bagni', 'bagni']
        if string_found(bathroom, description):
            item_loader.add_value('bathroom_count', 2)
        else:
            item_loader.add_value('bathroom_count', bathroom_count)

        if extract_number_only(apartment[1]) == "0":
            item_loader.add_value('room_count', 1)
        else:
            item_loader.add_value('room_count', extract_number_only(apartment[1]))

        item_loader.add_value('square_meters', extract_number_only(apartment[2]))

        item_loader.add_value('currency', "EUR")
        if float(extract_number_only(rent)) > 0:
            item_loader.add_value('rent', rent)
        else:
            rent_position = description.index('€')
            rent = description[(rent_position + 1):(rent_position + 7)]
            item_loader.add_value('rent', rent)

        item_loader.add_value('utilities', extract_utilities("€", description))

        if "energetica" in description:
            x = description.rfind("energetica")
            item_loader.add_value('energy_label', description[x+11])
        elif "Classe" in description:
            x = description.rfind("Classe")
            item_loader.add_value('energy_label', description[x + 7])
        else:
            item_loader.add_value('energy_label', None)

        id = extract_number_only(description.split("-")[0])
        if id !=0:
            item_loader.add_value("external_id", f"L{id}")
        else:
            item_loader.add_value('external_id', None)

        furnished = ['arredato']
        item_loader.add_value('furnished', string_found(furnished, description))

        washing_machine = ['lavanderia']
        item_loader.add_value('washing_machine', string_found(washing_machine, description))

        balcony = ['BALCONE']
        item_loader.add_value('balcony', string_found(balcony, description))

        elevator = ['Ascensore']
        item_loader.add_value('elevator', string_found(elevator, all_futures))

        terrace = ['Terrazzo']
        item_loader.add_value('terrace', string_found(terrace, all_futures))

        parking = ['Parcheggio privato', 'auto']
        item_loader.add_value('parking', string_found(parking, description))

        item_loader.add_value('landlord_name', "Abitare Con Stile Finim S.r.l.")
        item_loader.add_value('landlord_phone', "02.801-537")
        item_loader.add_value('landlord_email', "info@finim-online.com")

        self.position += 1
        yield item_loader.load_item()




