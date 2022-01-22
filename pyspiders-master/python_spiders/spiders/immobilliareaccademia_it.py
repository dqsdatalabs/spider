# -*- coding: utf-8 -*-
# Author: Noor

import scrapy
from ..loaders import ListingLoader
import re

class MySpider(scrapy.Spider):
    name = 'immobiliareaccademia_it'
    allowed_domains = ['immobiliareaccademia.it']
    start_urls = ['https://www.immobiliareaccademia.it/web/immobili.asp']
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    thousand_separator = '.'
    scale_separator = '.'

    def parse(self, response):
        links = response.css('.item-block').xpath('@href').extract()
        for link in links:
            yield scrapy.Request(
                url='https://www.immobiliareaccademia.it' + link,
                callback=self.get_property_details,
                dont_filter=True)

    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        features=response.css('.feature-list li::text').extract()
        if features[0]=='Appartamento' and features[1]=='Affitto':
            item_loader.add_value('external_link', response.url)
            title = response.css('h1 ::text').extract()[-1]
            item_loader.add_value('title', title)
            item_loader.add_value('property_type', 'apartment')
            images = response.css('.slides img').xpath('@src').extract()
            item_loader.add_value('images', images)
            item_loader.add_value('currency', 'EUR')
            item_loader.add_value('landlord_name', 'immobiliareaccademia.it')
            item_loader.add_value('landlord_phone', ' 091 583607')
            item_loader.add_value('external_source', self.external_source)
            desc = response.css('.lt_desc::text').extract()[0].strip()
            item_loader.add_value('description', desc)
            rent = response.css('#maincol .colore1::text').extract()[0][2:]
            item_loader.add_value('rent_string', rent)
            item_loader.add_value('city',response.css('.no-btm::text').extract())

            dt_details = response.css('.etichetta ::text').extract()
            stripped_details = [i.strip().lower() if type(i) == str else str(i) for i in dt_details]
            if 'rif' in stripped_details:
                id_index=stripped_details.index('rif')
                id=stripped_details[id_index+2]
                item_loader.add_value('external_id',id)
            if 'piano' in stripped_details:
                floor_index = stripped_details.index('piano')
                floor = stripped_details[floor_index + 2]
                item_loader.add_value('floor', floor)
            if 'classe energetica' in stripped_details:
                energy_index = stripped_details.index('classe energetica')
                energy_label = stripped_details[energy_index + 2]
                item_loader.add_value('energy_label', energy_label.upper())
            if 'superficie' in stripped_details:
                sq_index = stripped_details.index('superficie')
                sq = int(stripped_details[sq_index + 2][:-2].strip())
                item_loader.add_value('square_meters', sq)
            if 'camere' in stripped_details:
                room_index = stripped_details.index('camere')
                if stripped_details[room_index + 2][0].isdigit():
                    room_count = int(stripped_details[room_index + 2][0])
                    item_loader.add_value('room_count', room_count)
            if 'bagni' in stripped_details:
                bathroom_index = stripped_details.index('bagni')
                bathroom_count = int(stripped_details[bathroom_index + 2])
                item_loader.add_value('bathroom_count', bathroom_count)
            if 'arredato' in stripped_details:
                item_loader.add_value('furnished',True)
            else :
                item_loader.add_value('furnished', False)

            if 'ascensore' in stripped_details:
                item_loader.add_value('elevator',True)
            else :
                item_loader.add_value('elevator', False)
            location_link = response.css('iframe').xpath('@src').extract()[0]
            location_regex = re.compile(r'q=([0-9]+\.[0-9]+),([0-9]+\.[0-9]+)')
            long_lat = str(location_regex.search(location_link).group())
            lat = long_lat[long_lat.index('=') + 1:long_lat.index(',')]
            long = long_lat[long_lat.index(',') + 1:]
            item_loader.add_value('longitude', long)
            item_loader.add_value('latitude', lat)

            yield item_loader.load_item()