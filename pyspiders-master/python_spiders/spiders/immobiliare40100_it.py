# -*- coding: utf-8 -*-
# Author: Noor

import scrapy
from ..loaders import ListingLoader
import re

class MySpider(scrapy.Spider):
    name = 'immobiliare40100_it'
    allowed_domains = ['immobiliare40100.it']
    start_urls = [
        'https://www.immobiliare40100.it/index.php?pagina=elenco&tipologia=affitto%20residenziale']
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    thousand_separator = '.'
    scale_separator = '.'

    def parse(self, response):
        links = response.css('font+ a').xpath('@href').extract()
        for link in links:
            yield scrapy.Request(
                url='https://www.immobiliare40100.it/' + link,
                callback=self.get_property_details,
                dont_filter=True)

    def get_property_details(self, response):
        if response.url != 'https://www.immobiliare40100.it/index.php':
            item_loader = ListingLoader(response=response)
            item_loader.add_value('external_link', response.url)
            title = response.css('h2::text').extract()[0]
            item_loader.add_value('title', title)
            item_loader.add_value('property_type', 'apartment')
            imgs=response.css('#kenburns a').xpath('@href').extract()[1:]
            images=[]
            for i in imgs:
                images.append('https://www.immobiliare40100.it/'+i)
            item_loader.add_value('images', images)
            item_loader.add_value('currency', 'EUR')
            item_loader.add_value('landlord_name', 'immobiliare40100.it')
            item_loader.add_value('landlord_phone', '051 236810')
            item_loader.add_value('external_source', self.external_source)
            desc = ''.join(response.css('p::text').extract())[:-78]
            item_loader.add_value('description', desc+ '...')
            id = response.css('font b::text').extract()[0][21:].strip()
            item_loader.add_value('external_id', id)

            dt_details = response.css('td ::text').extract()
            stripped_details = [i.strip().lower() if type(i) == str else str(i) for i in dt_details]

            if 'indirizzo:'  in stripped_details:
                index = stripped_details.index('indirizzo:')
                address = stripped_details[index + 2]
                item_loader.add_value('address', address)
            if 'prezzo/mq:' in stripped_details:
                rent_index = stripped_details.index('prezzo/mq:')
                rent = stripped_details[rent_index + 3][1:stripped_details[rent_index + 3].index('-')].strip()
                item_loader.add_value('rent_string', rent)
            if 'piano:' in stripped_details:
                floor_index = stripped_details.index('piano:')
                floor = stripped_details[floor_index + 3]
                item_loader.add_value('floor', floor)
            if 'classe energetica:' in stripped_details:
                energy_index = stripped_details.index('classe energetica:')
                energy_label = stripped_details[energy_index + 3]
                item_loader.add_value('energy_label', energy_label.upper())
            if 'prezzo/mq:' in stripped_details:
                sq_index = stripped_details.index('prezzo/mq:')
                sq = int(stripped_details[sq_index + 3][-3:].strip())
                item_loader.add_value('square_meters', sq)
            if 'n.\xa0camere:' in stripped_details:
                room_index = stripped_details.index('n.\xa0camere:')
                if stripped_details[room_index + 2][0].isdigit():
                    room_count = int(stripped_details[room_index + 2][0])
                    item_loader.add_value('room_count', room_count)
            if 'n. bagni:' in stripped_details:
                bathroom_index = stripped_details.index('n. bagni:')
                bathroom_count = int(stripped_details[bathroom_index + 2])
                item_loader.add_value('bathroom_count', bathroom_count)
            if 'città/zona:' in stripped_details:
                city_index=stripped_details.index('città/zona:')
                city=stripped_details[city_index+2]
                item_loader.add_value('city',city)
            if 'garage/posto macchina:' in stripped_details:
                index=stripped_details.index('garage/posto macchina:')
                prk=stripped_details[index+2]
                if 'garage' in prk:
                    item_loader.add_value('parking',True)
            if 'interni:' in stripped_details:
                i=stripped_details.index('interni:')
                value=stripped_details[i+2]
                if'arredato' in value:
                    item_loader.add_value('furnished',True)

            loc =''.join(response.css('script').extract())
            if loc:
                location_link = loc
                location_regex = re.compile(r'LatLng\(([0-9]+\.[0-9]+),([0-9]+\.[0-9]+)\)')
                ll = location_regex.search(location_link)
                if ll:
                    long_lat = str(ll.group())
                    lat = long_lat[long_lat.index('(') + 1:long_lat.index(',')]
                    long = long_lat[long_lat.index(',') + 1:long_lat.index(')')]
                    item_loader.add_value('longitude', long)
                    item_loader.add_value('latitude', lat)
            yield item_loader.load_item()
