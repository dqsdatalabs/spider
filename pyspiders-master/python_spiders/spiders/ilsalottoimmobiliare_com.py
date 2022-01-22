# -*- coding: utf-8 -*-
# Author: Noor

import scrapy
from ..loaders import ListingLoader
import re


class MySpider(scrapy.Spider):
    name = 'ilsalottoimmobiliare_com'
    allowed_domains = ['ilsalottoimmobiliare.com']
    start_urls = ['https://www.ilsalottoimmobiliare.com/risultati.php?tipologia=appartamento&tipomediaz=locazione&zona=&prezzo=&prezzo2a=&prezzo2b=&npratica=']
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    thousand_separator = '.'
    scale_separator = '.'

    def parse(self, response):
        lnks= response.css('#guarda-scheda').xpath('@onclick').extract()
        links=[l[l.index("'")+1:l.rfind("'")] for l in lnks]
        for link in links:
            yield scrapy.Request(
                url='https://www.ilsalottoimmobiliare.com/'+link,
                callback=self.get_property_details,
                dont_filter=True)

    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value('external_link', response.url)
        title = response.css('#titolo-pagina-landing::text').extract()[0].strip()
        item_loader.add_value('title', title)
        item_loader.add_value('property_type', 'apartment')
        images = response.css('img').xpath('@src').extract()[2:-3]
        item_loader.add_value('images', images)
        item_loader.add_value('currency', 'EUR')
        item_loader.add_value('landlord_name', 'Il Salotto Immobiliare')
        item_loader.add_value('landlord_phone', '340 3443588')
        item_loader.add_value('external_source', self.external_source)
        item_loader.add_value('landlord_email', 'info@ilsalottoimmobiliare.com')
        desc = response.css('#annuncio-scheda::text').extract()[0]
        item_loader.add_value('description', desc)

        if 'TRILOCALE' in title.upper():
            item_loader.add_value('room_count', 3)
        if 'BILOCALE' in title.upper():
            item_loader.add_value('room_count', 2)
        if 'MONOLOCALE' in title.upper():
            item_loader.add_value('room_count', 1)

        dt_details = response.css('.caratteristiche li ::text').extract()
        stripped_details = [i.strip().lower() if type(i) == str else str(i) for i in dt_details]

        if 'riferimento:' in stripped_details:
            id_index = stripped_details.index('riferimento:')
            id = stripped_details[id_index + 1]
            item_loader.add_value('external_id', id)
        if 'piano:' in stripped_details:
            floor_index = stripped_details.index('piano:')
            floor = stripped_details[floor_index + 1]
            item_loader.add_value('floor', floor)
        if 'classe energetica:' in stripped_details:
            energy_index = stripped_details.index('classe energetica:')
            energy_label = stripped_details[energy_index + 1]
            item_loader.add_value('energy_label', energy_label.upper())
        # if 'camere:' in stripped_details:
        #     room_index = stripped_details.index('camere:')
        #     if stripped_details[room_index + 1][0].isdigit():
        #         room_count = int(stripped_details[room_index + 1][0])
        #         item_loader.add_value('room_count', room_count)
        if 'metri quadri:' in stripped_details:
            index = stripped_details.index('metri quadri:')
            sq = int(stripped_details[index + 1])
            item_loader.add_value('square_meters', sq)
        if 'prezzo:' in stripped_details:
            index = stripped_details.index('prezzo:')
            rent = stripped_details[index + 1]
            item_loader.add_value('rent_string', rent)
        if 'bagni:' in stripped_details:
            bathroom_index = stripped_details.index('bagni:')
            bathroom_count = int(stripped_details[bathroom_index + 1])
            item_loader.add_value('bathroom_count', bathroom_count)
        if 'arredamento:' in stripped_details:
            index = stripped_details.index('arredamento:')
            fur = stripped_details[index + 1]
            if fur != '':
                item_loader.add_value('furnished', True)
            else:
                item_loader.add_value('furnished', False)

        if 'ascensore:' in stripped_details:
            index = stripped_details.index('ascensore:')
            elev = stripped_details[index + 1]
            if elev=='ascensore':
                item_loader.add_value('elevator', True)
            else:
                item_loader.add_value('elevator', False)

        if 'parcheggi:' in stripped_details:
            index = stripped_details.index('parcheggi:')
            prk = stripped_details[index + 1]
            if prk !='no':
                item_loader.add_value('parking', False)
            else:
                item_loader.add_value('parking', True)




        item_loader.add_value('city',response.url.split('/')[-3])
        item_loader.add_value('address',response.url.split('/')[-3]+', '+response.url.split('/')[-2])
        yield item_loader.load_item()
