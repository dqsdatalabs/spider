
# -*- coding: utf-8 -*-
# Author: Noor

import scrapy
from ..loaders import ListingLoader

import re

class MySpider(scrapy.Spider):
    name = 'immobiliaredallai_com'
    allowed_domains = ['immobiliaredallai.com']

    start_urls = [
        'https://www.immobiliaredallai.com/it/Appartamento-Mugello?affitto=on&com=Qualsiasi&superficie=Qualsiasi&numero_camere=Qualsiasi&price_start=&price_end=&rif=&now=1&tipologia=13']
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    thousand_separator = '.'
    scale_separator = '.'

    def parse(self, response):
        number=response.css('.page ::text').extract()[-1]
        start_urls = []
        if number.isdigit():
            pages_number = int(number)
            for i in range(1,pages_number + 1):
                start_urls.append('https://www.immobiliaredallai.com/it/Appartamento-Mugello?affitto=on&com=Qualsiasi&superficie=Qualsiasi&numero_camere=Qualsiasi&price_start=&price_end=&rif=&now=1&tipologia=13&now='+str(i))
        else:
            start_urls = ['https://www.immobiliaredallai.com/it/Appartamento-Mugello?affitto=on&com=Qualsiasi&superficie=Qualsiasi&numero_camere=Qualsiasi&price_start=&price_end=&rif=&now=1&tipologia=13']
        for url in start_urls:
            yield scrapy.Request(
                url=url,
                dont_filter=True,
                callback=self.parse2,
            )

    def parse2(self, response):
        links = response.css('.brd-c-c').xpath('@href').extract()
        for link in links:
            yield scrapy.Request(
                url='https://www.immobiliaredallai.com'+link,
                callback=self.get_property_details,
                dont_filter=True)

    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        ##
        title = response.css('.f-16::text').extract()[0].strip()
        item_loader.add_value('title', title)
        item_loader.add_value('external_link', response.url)
        item_loader.add_value('property_type', 'apartment')
        description = ''.join(response.css('.xplr0 p::text').extract()[:-2]).strip()[:-95]
        item_loader.add_value('description', description+' ...')

        ##
        imgs=response.css('.m-auto').xpath('@src').extract()
        images = [i[10:] for i in imgs]
        item_loader.add_value('images', images)
        item_loader.add_value('currency', 'EUR')
        item_loader.add_value('landlord_name', 'immobiliaredallai.com')
        item_loader.add_value('landlord_phone', '055 8431039')
        item_loader.add_value('landlord_email', 'franco@immobiliaredallai.com')
        item_loader.add_value('external_source', self.external_source)
        id = response.css('.f-16 ::text').extract()[-2]
        item_loader.add_value('external_id', id)


        location_link = response.css('iframe').xpath('@src').extract()[0]
        location_regex = re.compile(r'q=([0-9]+\.[0-9]+),([0-9]+\.[0-9]+)')
        long_lat = str(location_regex.search(location_link).group())
        lat = long_lat[long_lat.index('=') + 1:long_lat.index(',')]
        long = long_lat[long_lat.index(',') + 1:]
        item_loader.add_value('longitude', long)
        item_loader.add_value('latitude', lat)


        dt_details =response.css('td ::text').extract()
        stripped_details = [i.strip() if type(i) == str else str(i) for i in dt_details]

        if 'Arredato' in stripped_details:
            fur_index = stripped_details.index('Arredato')
            furnished = stripped_details[fur_index+1]
            s= True if furnished == 'Arredato' else False
            item_loader.add_value('furnished', s)

        if 'Ascensore' in stripped_details:
            elev_index = stripped_details.index('Ascensore')
            elevator = stripped_details[elev_index+1]
            if elevator == 'si':
                item_loader.add_value('elevator', True)
            else:
                item_loader.add_value('elevator', False)
        if 'Balconi' in stripped_details:
            balc_index = stripped_details.index('Balconi')
            balcony = stripped_details[balc_index+1]
            if balcony == '0':
                item_loader.add_value('balcony', False)
            else:
                item_loader.add_value('balcony', True)
        if 'Prezzo' in stripped_details:
            rent_index = stripped_details.index('Prezzo')
            rent = stripped_details[rent_index + 1]
            item_loader.add_value('rent_string', rent)
        if 'Piano' in stripped_details:
            floor_index = stripped_details.index('Piano')
            floor = stripped_details[floor_index + 1]
            item_loader.add_value('floor', floor)
        if 'Mq.' in stripped_details:
            sq_index = stripped_details.index('Mq.')
            sq = stripped_details[sq_index + 1]
            item_loader.add_value('square_meters', sq)
        if 'Camere' in stripped_details:
            room_index = stripped_details.index('Camere')
            room_count = int(stripped_details[room_index + 1])
            item_loader.add_value('room_count', room_count)
        if 'Bagni' in stripped_details:
            bathroom_index = stripped_details.index('Bagni')
            bathroom_count = int(stripped_details[bathroom_index + 1])
            item_loader.add_value('bathroom_count', bathroom_count)
        if 'Classe energetica' in stripped_details:
            energy_index = stripped_details.index('Classe energetica')
            energy_label = stripped_details[energy_index + 1]
            item_loader.add_value('energy_label', energy_label)

        yield item_loader.load_item()

