
# -*- coding: utf-8 -*-
# Author: Noor
import scrapy
from ..loaders import ListingLoader
import re

class MySpider(scrapy.Spider):
    name = 'exporemilano_it'
    allowed_domains = ['exporemilano.it']
    start_urls = ['https://www.exporemilano.it/web/immobili.asp']
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    thousand_separator = '.'
    scale_separator = '.'

    def parse(self, response):
        pages = int(response.css('.paginator::text').extract()[2].strip()[-1])
        for p in range(pages+1):
            yield scrapy.Request(
                url='https://www.exporemilano.it/web/immobili.asp?num_page='+str(p)+'&showkind=&group_cod_agenzia=9258&cod_sede=0&cod_sede_aw=0&cod_gruppo=0&cod_agente=0&pagref=0&ref=&language=ita&maxann=10&estero=0&cod_nazione=&cod_regione=&ricerca_testo=&indirizzo=&tipo_contratto=A&cod_categoria=R&cod_tipologia=3&cod_provincia=0&cod_comune=0&localita=&prezzo_min=0&prezzo_max=100000000&mq_min=0&mq_max=10000&vani_min=0&vani_max=1000&camere_min=0&camere_max=100&riferimento=&cod_ordine=O02&garage=0&ascensore=0&balcone=0&soffitta=0&cantina=0&taverna=0&condizionamento=0&parcheggio=0&giardino=0&piscina=0&camino=0&prestigio=0&cod_campi=',
                callback=self.parse2,
                dont_filter=True)

    def parse2(self, response):
        links = response.css('.annuncio a').xpath('@href').extract()
        for link in links:
            yield scrapy.Request(
                url='https://www.exporemilano.it' + link,
                callback=self.get_property_details,
                dont_filter=True)

    def get_property_details(self, response):

        item_loader = ListingLoader(response=response)
        item_loader.add_value('external_link', response.url)
        ##
        e=response.css('#li_clen::text').extract()
        if e and e[0]:
            energy_label=e[0][-1]
            item_loader.add_value('energy_label',energy_label)
        ##
        sq=response.css('#li_superficie strong::text').extract()
        if sq and sq[0]:
            sq_meters = int(sq[0])
            item_loader.add_value('square_meters', sq_meters)
         ##
        r=response.css('.right::text').extract()
        if r and r[-1]:
            rent = r[-1][2:]
            item_loader.add_value('rent_string', rent)
        ##
        item_loader.add_value('property_type', 'apartment')
        description = ''.join(response.css('.imm-det-des::text').extract())
        item_loader.add_value('description', description)
        ##
        images =  response.css('.imgw img').xpath('@src').extract()
        item_loader.add_value('images', images)
        ##
        title = response.css('h1::text').extract()[0]
        item_loader.add_value('title', title)

        ##
        item_loader.add_value('currency', 'EUR')
        item_loader.add_value('landlord_name', 'exporemilano.it')
        item_loader.add_value('landlord_phone','0287178747')
        item_loader.add_value('external_source', self.external_source)

        room=response.css('#li_vani strong::text').extract()
        if room and room[0]:
            room_count =int(room[0])
            item_loader.add_value('room_count', room_count)

        bath=response.css('#li_bagni strong::text').extract()
        if bath and bath[0]:
            bathroom_count = int(bath[0])
            item_loader.add_value('bathroom_count', bathroom_count)
        location_link=response.css('iframe').xpath('@src').extract()[-2]
        location_regex = re.compile(r'q=([0-9]+\.[0-9]+),([0-9]+\.[0-9]+)')
        long_lat= str(location_regex.search(location_link).group())
        lat=long_lat[long_lat.index('=')+1:long_lat.index(',')]
        long=long_lat[long_lat.index(',')+1:]
        item_loader.add_value('longitude',long)
        item_loader.add_value('latitude',lat)

        yield item_loader.load_item()
