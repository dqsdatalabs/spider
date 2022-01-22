# -*- coding: utf-8 -*-
# Author: Noor

import scrapy
from ..loaders import ListingLoader
import re

class MySpider(scrapy.Spider):
    name = 'abacusimmobiliare_com'
    allowed_domains = ['abacusimmobiliare.com']
    start_urls = [
        'https://www.abacusimmobiliare.com/immobili-in-affitto/appartamento_5.html']
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    thousand_separator = '.'
    scale_separator = '.'

    def parse(self, response):
        pages_number = int(response.css('.paginazione a::text').extract()[-2])
        start_urls = []
        for i in range(0, pages_number):
            start_urls.append(
                'https://www.abacusimmobiliare.com/elenco_immobili_f.asp?start=' + str(
                    i * 8 + 1) + '&idtip=5&idcau2=1#elenco_imm')
        for url in start_urls:
            yield scrapy.Request(
                url=url,
                dont_filter=True,
                callback=self.parse2,
            )
    def parse2(self, response):
        links = response.css('.proprieta_info-luogo a').xpath('@href').extract()
        for link in links:
            yield scrapy.Request(
                url='https://www.abacusimmobiliare.com/' + link,
                callback=self.get_property_details,
                dont_filter=True)

    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value('external_link', response.url)

        address = response.css('.lista-widget-zona::text').extract()[0]
        item_loader.add_value('address', address)
        item_loader.add_value('city',address.split('-')[0].strip())

        sq = response.css('.superficie span::text').extract()[0]
        sq_meters = [int(s) for s in sq.split() if s.isdigit()][0]
        item_loader.add_value('square_meters', sq_meters)

        external_id =response.css('.title-bg span ::text').extract()[1].strip().split(' ')[1]
        item_loader.add_value('external_id', external_id)

        rent = response.css('.lista-widget-prezzo::text').extract()[0].strip()[2:]
        item_loader.add_value('rent_string', rent)
        item_loader.add_value('property_type', 'apartment')

        desc=response.css('#descrizione ::text').extract()

        for indx in range(len(desc)-1) :
         if '055'in desc[indx]:
             exception=indx
        if exception:
            desc = [x for i, x in enumerate(desc) if i != exception]
        description = ''.join(desc).strip()
        item_loader.add_value('description', description)

        images = response.css('img').xpath('@src').extract()[1:-2]
        item_loader.add_value('images', images)
        title = response.css('.title-bg::text').extract()[0].strip()
        item_loader.add_value('title',title)

        item_loader.add_value('currency', 'EUR')
        item_loader.add_value('landlord_name', 'Abacus Real Estate')
        item_loader.add_value('landlord_phone', '0554684635')
        item_loader.add_value('landlord_email', 'agenzia@abacusimmobiliare.com')
        item_loader.add_value('external_source', self.external_source)
        floor = response.css('.piano span::text').extract()[0]
        item_loader.add_value('floor', floor)

        bathroom_count = int(response.css('.vani span::text').extract()[0])
        item_loader.add_value('bathroom_count', bathroom_count)

        energy = response.css('.classe span::text').extract()[0]
        item_loader.add_value('energy_label', energy)

        utility = response.css('.spese span::text').extract()[0][1:].strip()
        item_loader.add_value('utilities', utility)



        balcony = response.css('.balcon span::text').extract()
        if balcony and balcony[0]:
            if balcony[0] == 'SI':
                item_loader.add_value('balcony', True)
            else:
                item_loader.add_value('balcony', False)

        elevator = response.css('.ascensore span::text').extract()
        if elevator and elevator[0]:
            if elevator[0] == 'SI':
                item_loader.add_value('elevator', True)
            else:
                item_loader.add_value('elevator', False)

        terrace = response.css('.terrazzo span::text').extract()
        if terrace and terrace[0]:
            if terrace[0] == 'SI':
                item_loader.add_value('terrace', True)
            else:
                item_loader.add_value('terrace', False)
        features=response.css('.content-boxed li ::text').extract()
        if 'Box Auto: ' in features:
            indx=features.index('Box Auto: ')
            if features[indx+1]=='nessuno':
                item_loader.add_value('parking',False)
            else:
                item_loader.add_value('parking',True)
        location_link=response.css('iframe').xpath('@src').extract()[0]
        location_regex = re.compile(r'q=([0-9]+\.[0-9]+),([0-9]+\.[0-9]+)')
        ll = location_regex.search(location_link)
        if ll :
            long_lat=str(ll.group())
            lat = long_lat[long_lat.index('=') + 1:long_lat.index(',')]
            long = long_lat[long_lat.index(',') + 1:]
            item_loader.add_value('longitude', long)
            item_loader.add_value('latitude', lat)
        rooms = response.css('.camere span::text').extract()
        if rooms and rooms[0]:
            room_count = int(rooms[0])
            item_loader.add_value('room_count', room_count)
            yield item_loader.load_item()
