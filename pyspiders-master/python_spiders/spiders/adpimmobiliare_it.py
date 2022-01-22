# -*- coding: utf-8 -*-
# Author: Noor

import scrapy
from ..loaders import ListingLoader
import re


class MySpider(scrapy.Spider):
    name = 'adpimmobiliare_it'
    allowed_domains = ['adpimmobiliare.it']
    start_urls = ['https://adpimmobiliare.it/affitto_immobili.php']
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    thousand_separator = '.'
    scale_separator = '.'

    def parse(self, response):
        links = response.css('.dettagli a').xpath('@href').extract()
        for link in links:
            yield scrapy.Request(
                url=link,
                callback=self.get_property_details,
                dont_filter=True)

    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value('external_link', response.url)
        title = ''.join(response.css('h1 ::text').extract()).strip()
        item_loader.add_value('title', title)
        if 'TRILOCALE' in title:
            item_loader.add_value('room_count', 3)
        if 'BILOCALE' in title:
            item_loader.add_value('room_count', 2)
        if 'MONOLOCALE' in title:
            item_loader.add_value('room_count', 1)
        sq = title.split(' ')[2][:-2]
        item_loader.add_value('square_meters', sq)

        tt = title.split(',')[0]
        floor = tt[tt.index('al')+3:]
        item_loader.add_value('floor', floor)

        item_loader.add_value('property_type', 'apartment')
        images = response.css('#imgGallery img').xpath('@src').extract()
        item_loader.add_value('images', images)

        item_loader.add_value('currency', 'EUR')
        item_loader.add_value('landlord_name', 'A.D.P. IMMOBILARE A.D.P. IMMOBILARE')
        item_loader.add_value('landlord_phone', '(+39) 011.433.07.84')
        item_loader.add_value('external_source', self.external_source)
        item_loader.add_value('landlord_email', 'info@adpimmobiliare.it')

        desc = response.css('.sunto ::text').extract()[2].strip()
        item_loader.add_value('description', desc)

        rent = response.css('.prezzoDett::text').extract()[0].strip()[:-1].strip()
        item_loader.add_value('rent_string', rent)

        city = response.css('h4::text').extract()[0]
        item_loader.add_value('city', city)

        dt_details = response.css('.content ::text').extract()
        stripped_details = [i.strip().lower() if type(i) == str else str(i) for i in dt_details]

        if 'indirizzo' in stripped_details:
            index = stripped_details.index('indirizzo')
            address = stripped_details[index + 1]
            item_loader.add_value('address', address)
            item_loader.add_value('zipcode',address[address.strip().rfind(' '):].strip())
        if 'rif' in stripped_details:
            id_index = stripped_details.index('rif')
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
        if 'camere' in stripped_details:
            room_index = stripped_details.index('camere')
            if stripped_details[room_index + 1][0].isdigit():
                room_count = int(stripped_details[room_index + 1][0])
                item_loader.add_value('room_count', room_count)
        if 'bagni' in stripped_details:
            bathroom_index = stripped_details.index('bagni')
            bathroom_count = int(stripped_details[bathroom_index + 1])
            item_loader.add_value('bathroom_count', bathroom_count)
        if 'arredato' in stripped_details:
            item_loader.add_value('furnished', True)
        else:
            item_loader.add_value('furnished', False)

        if 'ascensore' in title:
            item_loader.add_value('elevator', True)
        else:
            item_loader.add_value('elevator', False)

        yield item_loader.load_item()
