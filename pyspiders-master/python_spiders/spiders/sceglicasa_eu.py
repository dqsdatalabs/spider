# -*- coding: utf-8 -*-
# Author: Noor
import scrapy
from ..loaders import ListingLoader


class MySpider(scrapy.Spider):
    name = 'sceglicasa_eu'
    allowed_domains = ['sceglicasa.eu']
    start_urls = [
        'https://sceglicasa.eu/immobiliare/trova/immobili/elenco/residenziale?ricerca.tipo_elenco_annunci=6&ricerca.idagenzia=0&ricerca.idregione=0&ricerca.flagVacanza=false&ricerca.idprovincia=65&ricerca.idlocalita=0&ricerca.prezzo=0&ricerca.metriq=0&ricerca.prezzo_min=0&ricerca.metriq_min=0&ricerca.update_filter=1&ricerca.update_order=0&ricerca.raggio=0&ricerca.idzona=0&ricerca.numlocali=&ricerca.residenziale=1&ricerca.commerciale=0&ricerca.terreno=0&ricerca.order_result=0&ricerca.page=0&ricerca.page_ini=1&ricerca.pagelimit=40&ricerca.idfiltro=0&ricerca.idcontratto=2&ricerca.idtipologia=1&ricerca.provincia=Roma&ricerca.localita=&ricerca.zona=&ricerca.flt_categoria%5B%5D=1'
        ,
        'https://sceglicasa.eu/immobiliare/trova/immobili/elenco/residenziale?ricerca.tipo_elenco_annunci=6&ricerca.idagenzia=0&ricerca.idregione=0&ricerca.flagVacanza=false&ricerca.idprovincia=91&ricerca.idlocalita=0&ricerca.prezzo=0&ricerca.metriq=0&ricerca.prezzo_min=0&ricerca.metriq_min=0&ricerca.update_filter=1&ricerca.update_order=0&ricerca.raggio=0&ricerca.idzona=0&ricerca.numlocali=&ricerca.residenziale=1&ricerca.commerciale=0&ricerca.terreno=0&ricerca.order_result=0&ricerca.page=0&ricerca.page_ini=1&ricerca.pagelimit=40&ricerca.idfiltro=0&ricerca.idcontratto=2&ricerca.idtipologia=1&ricerca.provincia=Trapani&ricerca.localita=&ricerca.zona=&ricerca.flt_categoria%5B%5D=1'
        ,
        'https://sceglicasa.eu/immobiliare/trova/immobili/elenco/residenziale?ricerca.tipo_elenco_annunci=6&ricerca.idagenzia=0&ricerca.idregione=0&ricerca.flagVacanza=false&ricerca.idprovincia=92&ricerca.idlocalita=0&ricerca.prezzo=0&ricerca.metriq=0&ricerca.prezzo_min=0&ricerca.metriq_min=0&ricerca.update_filter=1&ricerca.update_order=0&ricerca.raggio=0&ricerca.idzona=0&ricerca.numlocali=&ricerca.residenziale=1&ricerca.commerciale=0&ricerca.terreno=0&ricerca.order_result=0&ricerca.page=0&ricerca.page_ini=1&ricerca.pagelimit=40&ricerca.idfiltro=0&ricerca.idcontratto=2&ricerca.idtipologia=1&ricerca.provincia=Palermo&ricerca.localita=&ricerca.zona=&ricerca.flt_categoria%5B%5D=1']
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    thousand_separator = '.'
    scale_separator = '.'

    def parse(self, response):
        links = response.css('.search-immobile-scheda').xpath('@data-id-immobile').extract()
        for link in links:
            yield scrapy.Request(
                url='https://sceglicasa.eu/immobiliare/trova/immobili/scheda/' + link,
                callback=self.get_property_details,
                dont_filter=True)

    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value('external_link', response.url)
        addr = response.css('h1.font-26::text').extract()[0]
        address = response.css('h1.font-26::text').extract()[0][addr.index('-') + 2:]
        item_loader.add_value('address', address)

        sq = response.css('h2.font-26 ::text').extract()[0]
        sq_meters = [int(s) for s in sq.split() if s.isdigit()][0]
        item_loader.add_value('square_meters', sq_meters)

        title_info = response.css('h2.font-26 ::text').extract()[2].split('|')
        rent = title_info[3][3:-8]
        item_loader.add_value('rent_string', rent)
        item_loader.add_value('property_type', 'apartment')
        d = response.css('.small-12 .text-justify::text').extract()[0]
        if 'Prezzo' in d:
            description = d[:d.index('Prezzo')]
        else:
            description = d
        item_loader.add_value('description', description)
        images = response.css('img').xpath('@src').extract()[1:-1]
        item_loader.add_value('images', images)
        title = response.css('h1.font-26::text').extract()[0]
        item_loader.add_value('title', title)
        item_loader.add_value('city',title[title.rfind(',')+1:].strip())
        item_loader.add_value('currency', 'EUR')
        item_loader.add_value('landlord_name', response.css('.montserrat::text').extract()[2])
        phone = response.css('.checkmap li a::text').extract()[0].strip()
        item_loader.add_value('landlord_phone', phone)
        mail = response.css('.checkmap li a::text').extract()[1].strip()
        item_loader.add_value('landlord_email', mail)
        item_loader.add_value('external_source', self.external_source)
        bathrooms = title_info[-2]
        bathroom_count = [int(s) for s in bathrooms.split() if s.isdigit()]
        if bathroom_count and bathroom_count[0]:
            item_loader.add_value('bathroom_count', bathroom_count[0])

        dt_details = response.css('.immobile-info li span::text').extract()
        stripped_details = [i.strip() if type(i) == str else str(i) for i in dt_details]
        dd_values = response.css('.immobile-info li::text').extract()
        stripped_values = [i.strip() if type(i) == str else str(i) for i in dd_values]

        if 'Rif:' in stripped_details:
            id_index = stripped_details.index('Rif:')
            external_id = stripped_values[id_index]
            item_loader.add_value('external_id', external_id)
        if 'Tipologia:' in stripped_details:
            type_index = stripped_details.index('Tipologia:')
            ttype = stripped_values[type_index]
            if ttype == 'Stanza':
                item_loader.add_value('property_type', 'studio')
        if 'Locali:' in stripped_details:
            room_index = stripped_details.index('Locali:')
            if stripped_values[room_index][0].isdigit():
                room_count = int(stripped_values[room_index][0])
                item_loader.add_value('room_count', room_count)
        if 'Bagni:' in stripped_details:
            bath_index = stripped_details.index('Bagni:')
            bath = int(stripped_values[bath_index])
            item_loader.add_value('bathroom_count', bath)
        if 'Piano:' in stripped_details:
            floor_index = stripped_details.index('Piano:')
            floor = stripped_values[floor_index]
            item_loader.add_value('floor', floor)
        if 'Data:' in stripped_details:
            index = stripped_details.index('Data:')
            date = stripped_values[index]
            item_loader.add_value('available_date', date)
        if 'Balcone:' in stripped_details:
            balcony_index = stripped_details.index('Balcone:')
            balcony = stripped_values[balcony_index]
            if balcony == 'Presente':
                item_loader.add_value('balcony', True)
            else:
                item_loader.add_value('balcony', False)
        if 'Ascensore:' in stripped_details:
            elev_index = stripped_details.index('Ascensore:')
            elevator = stripped_values[elev_index]
            if elevator == 'Presente':
                item_loader.add_value('elevator', True)
            else:
                item_loader.add_value('elevator', False)
        lat=response.css('.map-container').xpath('@data-lat').extract()[0]
        item_loader.add_value('latitude',lat)
        lng=response.css('.map-container').xpath('@data-long').extract()[0]
        item_loader.add_value('longitude',lng)
        yield item_loader.load_item()
