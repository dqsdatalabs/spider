# -*- coding: utf-8 -*-
# Author: Noor
import scrapy
from ..loaders import ListingLoader


class MySpider(scrapy.Spider):
    name = 'sanniccolorealestate_it'
    allowed_domains = ['sanniccolorealestate.it']
    start_urls = ['https://sanniccolorealestate.it/ita-affitto.php?tz=1631802199']
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    thousand_separator = '.'
    scale_separator = ','

    def parse(self, response):
        links = response.css('.btn_columns').xpath('@href').extract()
        for link in links:
            yield scrapy.Request(
                url=link,
                callback=self.get_property_details,
                dont_filter=True)

    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value('external_link', response.url)

        ##
        item_loader.add_value('property_type', 'apartment')
        description = "".join(response.css('.txt_gen p::text').extract()).strip()
        item_loader.add_value('description', description)
        ##
        images = response.css('#masonry_grid li a ').xpath('@href').extract()
        item_loader.add_value('images', images)
        ##
        title = response.css('strong::text').extract()[0]
        item_loader.add_value('title', title)
        item_loader.add_value('currency', 'EUR')
        item_loader.add_value('landlord_name', 'sanniccolorealestate.it')
        item_loader.add_value('landlord_phone', '+39 055 2347386')
        item_loader.add_value('landlord_email', 'SANNICCOLOREALESTATE@GMAIL.COM')
        item_loader.add_value('external_source', self.external_source)
        ##
        external_id = response.css('.pad0x20 .t_upper:nth-child(1)::text').extract()[0][1:-1].strip()
        item_loader.add_value('external_id', external_id)

        dt_details = response.css('#proprieta_immobile span ::text ').extract()
        stripped_details = [i.strip() if type(i) == str else str(i) for i in dt_details]

        ##
        if 'Zona' in stripped_details:
            addr_index = stripped_details.index('Zona')
            address = stripped_details[addr_index + 1]
            item_loader.add_value('address', address)
        ##
        if 'Posto auto' in stripped_details:
            park_index = stripped_details.index('Posto auto')
            parking = stripped_details[park_index + 1]
            if parking == 'SI':
                item_loader.add_value('parking', True)
            else:
                item_loader.add_value('parking', False)
        ##

        if 'Classificazione energetica' in stripped_details:
            energy_index = stripped_details.index('Classificazione energetica')
            energy_label = stripped_details[energy_index + 1]
            item_loader.add_value('energy_label', energy_label)
        if 'Terrazza abitabile' in stripped_details:
            trc_index = stripped_details.index('Terrazza abitabile')
            terrace = stripped_details[trc_index + 1]
            if terrace == 'Si':
                item_loader.add_value('terrace', True)
            else:
                item_loader.add_value('terrace', False)
        if 'Ascensore' in stripped_details:
            elev_index = stripped_details.index('Ascensore')
            elev = stripped_details[elev_index + 1]
            if elev == 'SI':
                item_loader.add_value('elevator', True)
            else:
                item_loader.add_value('elevator', False)
                ###
        if 'N. Locali' in stripped_details:
            room_index = stripped_details.index('N. Locali')
            rooms = int(stripped_details[room_index + 1])
            item_loader.add_value('room_count', rooms)
            ##
        if 'N. Bagni' in stripped_details:
            bath_index = stripped_details.index('N. Bagni')
            bath = int(stripped_details[bath_index + 1])
            item_loader.add_value('bathroom_count', bath)
            ##
        if 'Piano' in stripped_details:
            floor_index = stripped_details.index('Piano')
            floor = stripped_details[floor_index + 1]
            item_loader.add_value('floor', floor)
        ##
        if 'Superficie interna' in stripped_details:
            sq_index = stripped_details.index('Superficie interna')
            sq = stripped_details[sq_index + 1]
            sq_meters = [int(s) for s in sq.split() if s.isdigit()][0]
            item_loader.add_value('square_meters', sq_meters)

        rent = response.css('.price::text ').extract()[0].strip()[:-2]
        item_loader.add_value('rent_string', rent)

        yield item_loader.load_item()
