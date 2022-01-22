# -*- coding: utf-8 -*-
# Author: Noor

import scrapy
from ..loaders import ListingLoader


class MySpider(scrapy.Spider):
    name = 'fiermonteimmobiliare_it'
    allowed_domains = ['fiermonteimmobiliare.it']
    start_urls = [
        'https://fiermonteimmobiliare.it/elenco/in_Affitto/Residenziale/tutte_le_tipologie/tutti_i_comuni/tutte_le_zone/?ordinamento1=5&ordinamento2=decrescente']
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    thousand_separator = '.'
    scale_separator = '.'

    def parse(self, response):
        links = response.css('.bottone-esito').xpath('@href').extract()
        for link in links:
            yield scrapy.Request(
                url='https://fiermonteimmobiliare.it' + link,
                callback=self.get_property_details,
                dont_filter=True)

    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value('external_link', response.url)
        title = response.css('h1::text').extract()[0]
        item_loader.add_value('title', title)
        # address= title.split('-')[:2]
        # item_loader.add_value('address',address)
        if'Bologna' in title:
            item_loader.add_value('city', 'Bologna')
        if 'Calabria' in title:
            item_loader.add_value('city', 'Calabria')
        if 'Mezzolara' in title:
            item_loader.add_value('city', 'Mezzolara')
        item_loader.add_value('property_type', 'apartment')
        images = response.css('.fancybox img').xpath('@src').extract()
        item_loader.add_value('images', images)
        item_loader.add_value('currency', 'EUR')
        item_loader.add_value('landlord_name', 'fiermonteimmobiliare.it')
        item_loader.add_value('landlord_phone', '051.51.53.53')
        item_loader.add_value('external_source', self.external_source)
        desc = response.css('#content2 p::text').extract()[0]
        item_loader.add_value('description', desc)

        dt_details = response.css('.caratteristiche-li ::text').extract()
        stripped_details = [i.strip().lower() if type(i) == str else str(i) for i in dt_details]
        if 'prezzo €' in stripped_details:
            rent_index = stripped_details.index('prezzo €')
            rent = stripped_details[rent_index + 1][:stripped_details[rent_index + 1].index(',')]
            item_loader.add_value('rent_string', rent)
        if 'riferimento' in stripped_details:
            id_index = stripped_details.index('riferimento')
            id = stripped_details[id_index + 1]
            item_loader.add_value('external_id', id)
        if 'piano' in stripped_details:
            floor_index = stripped_details.index('piano')
            floor = stripped_details[floor_index + 1]
            item_loader.add_value('floor', floor)
        if 'classe energetica' in stripped_details:
            energy_index = stripped_details.index('classe energetica')
            energy_label = stripped_details[energy_index + 1]
            item_loader.add_value('energy_label', energy_label.upper())
        if 'superficie mq.' in stripped_details:
            sq_index = stripped_details.index('superficie mq.')
            sq = int(stripped_details[sq_index + 1])
            item_loader.add_value('square_meters', sq)
        if 'n. camere' in stripped_details:
            room_index = stripped_details.index('n. camere')
            if stripped_details[room_index + 1][0].isdigit():
                room_count = int(stripped_details[room_index + 1][0])
                item_loader.add_value('room_count', room_count)
        if 'n. bagni' in stripped_details:
            bathroom_index = stripped_details.index('n. bagni')
            bathroom_count = int(stripped_details[bathroom_index + 1])
            item_loader.add_value('bathroom_count', bathroom_count)

        yield item_loader.load_item()
