# -*- coding: utf-8 -*-
# Author: Noor

import scrapy
from ..loaders import ListingLoader
import re


class MySpider(scrapy.Spider):
    name = 'sturent_it'
    allowed_domains = ['sturent.it']
    start_urls = ['https://sturent.it/Roma/', 'https://sturent.it/Milano/', 'https://sturent.it/Palermo/',
                  'https://sturent.it/Firenze/']
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    thousand_separator = '.'
    scale_separator = '.'

    def parse(self, response):
        script = ''.join(response.css('script ::text ').extract())
        props_numbers = [x.group()[1:-1] for x in re.finditer(r'\"[a-z]=([0-9]+)"', script)]
        for n in props_numbers:
            yield scrapy.Request(url='https://sturent.it/' + response.url.split('/')[-2] + '/scheda.aspx?' + n,
                                 callback=self.get_property_details,
                                 dont_filter=True)

    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value('external_link', response.url)

        title = response.css('.titolone::text').extract()[0].strip()
        item_loader.add_value('title', title)

        id= response.url[response.url.index('=')+1:]
        item_loader.add_value('external_id',id)

        item_loader.add_value('city', response.url.split('/')[-2])

        item_loader.add_value('currency', 'EUR')

        address = ''.join(response.css('.sottotitolo::text').extract()).strip().replace('\xa0\xa0\xa0\xa0', ',')
        item_loader.add_value('address', address)

        item_loader.add_value('room_count', 1)
        item_loader.add_value('bathroom_count', 1)

        s = response.css('.titoloSchedaNN::text').extract()
        if s and s[1]:
            sq = s[1].strip()
            item_loader.add_value('square_meters', int(sq))


        d = response.css('.descrizioneAppartamento td::text').extract()
        if d and d[0]:
            desc = d[0].strip()
            item_loader.add_value('description', desc)
        else:
            desc=response.css('.dettagliScheda > table td > table td::text').extract()
            item_loader.add_value('description', desc)

        imgs = response.css('.slides li img').xpath('@src').extract()
        images = [i.replace('..', 'https://sturent.it') for i in imgs]
        item_loader.add_value('images', images)

        rent_string = response.css('.descrCostoScheda b::text').extract()[0][:-1].strip()
        item_loader.add_value('rent_string', rent_string)

        item_loader.add_value('landlord_name', 'Sturent Firenze')
        item_loader.add_value('landlord_phone', response.css('.datiAgenzia font::text').extract()[0])
        item_loader.add_value('landlord_email', response.css('.datiAgenzia font::text').extract()[1])

        item_loader.add_value('external_source', self.external_source)

        item_loader.add_value('property_type', 'room')

        item_loader.add_value('furnished', True)

        u=response.css('.campiRuffiani:nth-child(8) ::text').extract()
        if u and u[2]:
            ut=u[2][2:].strip()[1:].strip()
            item_loader.add_value('utilities',ut)

        f = response.css('.campiSchedaAppa td::text').extract()
        if f and f[3]:
            floor = f[3].strip()
            item_loader.add_value('floor', floor)

        ftrs=response.css('.caratteristicheScheda::text').extract()
        features = [i.strip() for i in ftrs]
        if 'Lavastoviglie' in features:
            item_loader.add_value('dishwasher', True)
        if 'Lavatrice' in features:
            item_loader.add_value('washing_machine', True)
        if 'Ascensore' in features:
            item_loader.add_value('elevator', True)
        if 'Posto auto' in features:
            item_loader.add_value('parking', True)

        e=response.css('.campiSchedaAppa:nth-child(13) .dettaglioScheda ::text').extract()
        if e and e[0]:
            energy=e[0].strip()
            if len(energy)>0:
                item_loader.add_value('energy_label',energy)

        l=response.css('script ::text').extract()[3]
        lls=[x.group()[1:-1] for x in re.finditer(r'"[0-9]+.[0-9]+"',l)]
        item_loader.add_value('latitude',lls[0])
        item_loader.add_value('longitude',lls[2])

        # no sq for rooms in missed sq
        yield item_loader.load_item()
