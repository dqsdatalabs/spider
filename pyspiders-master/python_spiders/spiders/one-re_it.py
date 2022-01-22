# -*- coding: utf-8 -*-
# Author: Noor

import scrapy
from ..loaders import ListingLoader
import re


class MySpider(scrapy.Spider):
    name = 'one_re_it'
    allowed_domains = ['one-re.it']
    start_urls = ['https://www.one-re.it/immobili/AFFITTO-RESIDENZIALE.html']
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    thousand_separator = '.'
    scale_separator = '.'

    def parse(self, response):
        pages=int(''.join(response.css('.paginator::text').extract()).strip()[-1])
        for i in range(1,pages+1):
            yield scrapy.Request(
                url='https://www.one-re.it/web/immobili.asp?num_page='+str(i)+'&showkind=&group_cod_agenzia=6580&cod_sede=0&cod_sede_aw=0&cod_gruppo=26&cod_agente=0&pagref=78149&ref=AFFITTO-RESIDENZIALE&language=ita&maxann=10&estero=0&cod_nazione=&cod_regione=&ricerca_testo=&indirizzo=&tipo_contratto=A&cod_categoria=%25&cod_tipologia=3%2C30%2C31%2C100%2C34%2C7%2C22%2C16%2C48%2C11%2C40%2C49%2C23%2C12%2C47%2C42%2C24%2C10%2C32%2C33%2C29&cod_provincia=&cod_comune=0&localita=&prezzo_min=0&prezzo_max=100000000&mq_min=0&mq_max=10000&vani_min=0&vani_max=1000&camere_min=0&camere_max=100&riferimento=&cod_ordine=O01&garage=0&ascensore=0&balcone=0&soffitta=0&cantina=0&taverna=0&condizionamento=0&parcheggio=0&giardino=0&piscina=0&camino=0&prestigio=0&cod_campi=',
                callback=self.parse2,
                dont_filter=True)

    def parse2(self, response):
        links=response.css('.item-block').xpath('@href').extract()
        for link in links:
            yield scrapy.Request(
                url='https://www.one-re.it'+link,
                callback=self.get_property_details,
                dont_filter=True)

    def get_property_details(self, response):
            item_loader = ListingLoader(response=response)
            item_loader.add_value('external_link', response.url)
            title =response.css('h1::text').extract()[0].strip()
            item_loader.add_value('title', title)
            if 'ARREDATO' in title.upper():
                item_loader.add_value('furnished',True)
            else:
                item_loader.add_value('furnished',False)
            item_loader.add_value('property_type', 'apartment')
            images = response.css('#photogallery img').xpath('@src').extract()
            item_loader.add_value('images', images)
            item_loader.add_value('currency', 'EUR')
            item_loader.add_value('landlord_name','ONE REAL ESTATE MILANO')
            item_loader.add_value('landlord_phone', '0395970670')
            item_loader.add_value('external_source', self.external_source)
            desc =''.join(response.css('.imm-det-des::text').extract())
            item_loader.add_value('description', desc)
            id=response.css('#slide_foto strong::text').extract()[0]
            item_loader.add_value('external_id', id)
            rent = response.css('#sidebar .right::text').extract()[0][2:]
            item_loader.add_value('rent_string', rent)
            sq = response.css('#li_superficie strong::text').extract()
            if sq and sq[0]:
                item_loader.add_value('square_meters', int(sq[0]))
            rooms=response.css('#li_vani strong::text').extract()
            if rooms and rooms[0]:
                item_loader.add_value("room_count",int(rooms[0]))
            bath=response.css('#li_bagni strong::text').extract()
            if bath and bath[0]:
                item_loader.add_value("bathroom_count", int(bath[0]))
            energy=response.css('#li_clen ::text').extract()
            if energy and energy[1]:
                item_loader.add_value('energy_label', energy[1][-1:])

            location_link = response.css('.map-tab iframe').xpath('@src').extract()[0]
            location_regex = re.compile(r'q=([0-9]+\.[0-9]+),([0-9]+\.[0-9]+)')
            long_lat = str(location_regex.search(location_link).group())
            lat = long_lat[long_lat.index('=') + 1:long_lat.index(',')]
            long = long_lat[long_lat.index(',') + 1:]
            item_loader.add_value('longitude', long)
            item_loader.add_value('latitude', lat)

            city=response.css('.ancom::text').extract()[0]
            item_loader.add_value('city',city)
            address=response.css('.anprovloc ::text').extract()
            item_loader.add_value('address',address)

            features=response.css('.etichetta ::text').extract()
            if 'Spese condominiali' in features:
                i=features.index('Spese condominiali')
                value=features[i+2]
                item_loader.add_value('utilities',value)
            if 'Arredato' in features:
                i=features.index('Arredato')
                value=features[i+2]
                fur=True if value=='si' else False
                item_loader.add_value('furnished',fur)
            if 'Ascensore' in features:
                i=features.index('Ascensore')
                value=features[i+2]
                fur=True if value=='si' else False
                item_loader.add_value('elevator',fur)
            if 'Balcone' in features:
                i=features.index('Balcone')
                value=features[i+2]
                fur=True if value=='si' else False
                item_loader.add_value('balcony',fur)
            if 'Terrazza' in features:
                i=features.index('Terrazza')
                value=features[i+2]
                fur=True if value=='si' else False
                item_loader.add_value('terrace',fur)
            if 'Piano' in features:
                i=features.index('Piano')
                value=features[i+2]
                item_loader.add_value('floor',value)


            yield item_loader.load_item()
