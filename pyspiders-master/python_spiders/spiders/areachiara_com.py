# -*- coding: utf-8 -*-
# Author: Noor

import scrapy
from ..loaders import ListingLoader
import re

class MySpider(scrapy.Spider):
    name = 'areachiara_com'
    allowed_domains = ['areachiara.com']
    start_urls = ['https://www.areachiara.com/abitazioni-in-affitto/']
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    thousand_separator = '.'
    scale_separator = '.'
    def parse(self, response):
        lnks=response.css('.list-halfmap-mappa a').xpath('@href').extract()
        links=list(dict.fromkeys(lnks))
        for link in links:
            yield scrapy.Request(
                url=link,
                callback=self.get_property_details,
                dont_filter=True)

    def get_property_details(self, response):
            item_loader = ListingLoader(response=response)
            if 'garage' not in response.url and response.url!='https://www.areachiara.com/404.php':
                info=response.css('h1 ::text').extract()
                infoo=[i.strip() for i in info]
                item_loader.add_value('external_link', response.url)
                title =infoo[0]
                item_loader.add_value('title', title)
                city=infoo[1].split('-')[0].strip()
                item_loader.add_value('city',city)
                address=infoo[1].split('-')[1].strip()
                item_loader.add_value('address',address)
                description=''.join(response.css('.bounceInUp p::text').extract())
                item_loader.add_value('description',description)
                d=description
                date=d[d.index('Libero dal')+11:d.index('Contratto')-2]
                item_loader.add_value('available_date',date)
                rent=response.css('.bounceInRight::text').extract()[0][1:].strip()
                item_loader.add_value('rent_string',rent)
                ll_name=response.css('strong a::text').extract()[0].strip()
                item_loader.add_value('landlord_name',ll_name)
                ll_email=response.css('a:nth-child(7)::text').extract()[0].strip()
                item_loader.add_value('landlord_email',ll_email)
                ll_phone=response.css('.telefono::text').extract()[0].strip()
                item_loader.add_value('landlord_phone',ll_phone)
                label=response.css('.liv_classe::text').extract()[0].strip()
                item_loader.add_value('energy_label',label)


                item_loader.add_value('currency', 'EUR')
                item_loader.add_value('property_type', 'apartment')
                item_loader.add_value('external_source', self.external_source)
                ##images .... cant be scraped

                features = response.css('.box ::text').extract()
                for i in features:
                    if 'Arredato:' in i:
                        fur = i[i.index(':') + 1:].strip()
                        if fur == 'Arredato':
                            item_loader.add_value('furnished', True)
                        else:
                            item_loader.add_value('furnished', False)
                    if 'Spese condominio:' in i:
                        ut = i[i.index('€') + 1:].strip()
                        item_loader.add_value('utilities', ut)
                    if 'Piano:' in i:
                        floor = i[i.index(':') + 1:].strip()
                        item_loader.add_value('floor', floor)

                    if 'Ascensore:' in i:
                        e = i[i.index(':') + 1:].strip()
                        if e == 'Si':
                            item_loader.add_value('elevator', True)
                        else:
                            item_loader.add_value('elevator', False)
                    if 'Bagni:' in i:
                        bath = i[i.index(':') + 1:].strip()
                        item_loader.add_value('bathroom_count', int(bath))
                    if 'Locali:' in i:
                        rooms = i[i.index(':') + 1:].strip()
                        item_loader.add_value('room_count', int(rooms))
                    if 'Totale mq:' in i:
                        sq = i[i.index(':') + 1:].strip()[:-3]
                        item_loader.add_value('square_meters', int(sq))
                if 'Stato conservazione: Buono' not in features:
                    yield item_loader.load_item()