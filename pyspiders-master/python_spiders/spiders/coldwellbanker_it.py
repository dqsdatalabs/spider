# -*- coding: utf-8 -*-
# Author: Noor
import requests
import scrapy
from scrapy import Selector
from scrapy import FormRequest

from ..loaders import ListingLoader
import re


class MySpider(scrapy.Spider):
    name = 'coldwellbanker_it'
    allowed_domains = ['coldwellbanker.it']
    start_urls = [
        'https://www.coldwellbanker.it/r-immobili/?Motivazione%5B%5D=2&localita=&Regione%5B%5D=0&Provincia%5B%5D=0&Comune%5B%5D=0&Codice=&Prezzo_da=&Prezzo_a=&Camere_da=&Bagni_da=&Tipologia%5B%5D=116%2C115%2C112%2C114%2C36%2C131%2C141%2C75&Locali_da=&cf=yes&map_circle=0&map_polygon=0&map_zoom=0'    ]
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    thousand_separator = '.'
    scale_separator = '.'

    def parse(self, response):
        pages_number = 11
        for i in range(1, pages_number + 1):
            frmdata = {"p": str(i),
                       "loadAjax": 'yes'}
            url = 'https://www.coldwellbanker.it/r-immobili/?Motivazione%5B%5D=2&localita=&Regione%5B%5D=0&Provincia%5B%5D=0&Comune%5B%5D=0&Codice=&Prezzo_da=&Prezzo_a=&Camere_da=&Bagni_da=&Tipologia%5B%5D=116%2C115%2C112%2C114%2C36%2C131%2C141%2C75&Locali_da=&cf=yes&map_circle=0&map_polygon=0&map_zoom=0'
            yield FormRequest(url, callback=self.parse2, formdata=frmdata, dont_filter=True)

    def parse2(self, response):
        links = response.css('.realestate-lista a ').xpath('@href').extract()
        ids=response.css('.list-halfmap-mappa ').xpath('@data-idimmobile').extract()
        c = response.css('.codice ::text ').extract()
        codes=[i.split('-')[-2] for i in c]
        for i in range (len(links)):
            frmdata = {"idImmobile": ids[i],
                       "tipoEvidenza":"agente",
                       "idEvidenza":codes[i]}
            yield FormRequest(
                url=links[i],
                formdata=frmdata,
                callback=self.get_property_details,
                dont_filter=True)

    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value('external_link', response.url)
        info=response.css('.bounceInDown ::text').extract()
        title = response.css('h1 ::text').extract()[0]
        item_loader.add_value('title', title)
        address = info[0]
        item_loader.add_value('address', address)
        city_info=address
        if '-' in city_info:
            city = city_info.split('-')[0]
        else:
            city = address
        item_loader.add_value('city', city)
        description =''.join(response.css('.bounceInUp p::text').extract())
        item_loader.add_value('description', description)
        label= response.css('.liv_classe ::text').extract()
        if label and label[0]:
            item_loader.add_value('energy_label',label[0])
        id=response.css('.padding_testata ::text').extract()[4].strip()[2:]
        item_loader.add_value('external_id',id)
        item_loader.add_value('currency', 'EUR')
        sq=response.css('.ico span::text').extract()[2][:-2].strip()
        item_loader.add_value('square_meters',int(sq))
        name= response.css('#formContatto strong ::text').extract()
        item_loader.add_value('landlord_name', name[0])
        item_loader.add_value('landlord_phone', response.css('.telefono::text').extract()[0])
        contact=response.css('#formContatto ::text').extract()
        for i in contact:
            if '@' in i:
                item_loader.add_value('landlord_email', i)
        item_loader.add_value('external_source', self.external_source)
        item_loader.add_value('property_type', 'apartment')


        details = response.css('.box ::text').extract()
        stripped_details = [i.strip().lower() if type(i) == str else str(i) for i in details]
        for d in stripped_details:
            if 'piano:' in d:
                floor = d[d.rfind(":") + 1:].strip()
                item_loader.add_value('floor', floor)
            if 'cap:' in d:
                floor = d[d.rfind(":") + 1:].strip()
                item_loader.add_value('zipcode', floor)
            if 'metri quadri:' in d:
                sq = int(d[d.rfind(":") + 1:].strip())
                item_loader.add_value('square_meters', sq)
            if 'posto auto:' in d:
                if d[-2:] == 'si':
                    item_loader.add_value('parking', True)
                else:
                    item_loader.add_value('parking', False)
            if 'bagni:' in d:
                bathroom_count = int(d[-1])
                item_loader.add_value('bathroom_count', bathroom_count)
            if 'locali:' in d:
                room_count = int(d[-1])
                item_loader.add_value('room_count', room_count)
            if 'indirizzo' in d:
                address =d[d.rfind(":") + 1:].strip()
                item_loader.add_value('address', address)
            if 'arredato:' in d:
                fur = d[d.rfind(":") + 1:].strip()
                if fur == 'arredato' or fur=='parzialmente arredato':
                    item_loader.add_value('furnished', True)
                else:
                    item_loader.add_value('furnished', False)
            if 'ascensore:' in d:
                elev = d[-2:]
                if elev == 'si':
                    item_loader.add_value('elevator', True)
                else:
                    item_loader.add_value('elevator', False)
            if 'terraz' in d:
                value= d[d.rfind(":") + 1:].strip()
                if 'present' in value:
                    item_loader.add_value('terrace',True)
                else:
                    item_loader.add_value('terrace' ,False)
            if 'balcon' in d:
                value= d[d.rfind(":") + 1:].strip()
                if 'present' in value:
                    item_loader.add_value('balcony',True)
                else:
                    item_loader.add_value('balcony' ,False)

            if 'spese condominio:' in d:
                if '/' in d:
                    utility = d[d.rfind('€') + 1:d.rfind('/')].strip()
                else:
                    utility = d[d.rfind('€') + 1:].strip()
                item_loader.add_value('utilities', utility)
        loc=response.css('.corpo script ::text').extract()
        if loc and loc[0]:
            l= loc[0]
            long_lat=l[l.index('lat'):l.index('lgt')+20]
            lat = long_lat[long_lat.index('= "') + 3:long_lat.index('";')]
            long =long_lat[long_lat.rfind('= "') + 3:long_lat.rfind('";')]
            item_loader.add_value('longitude', long)
            item_loader.add_value('latitude', lat)

        dataForImage = response.css(
            '.slFotoNow::attr(data-id), .slFotoNow::attr(data-tipo), .slFotoNow::attr(data-pagination)').getall()
        responseImages = requests.post("https://www.coldwellbanker.it/moduli/swiper_foto.php",
                                       data={
                                           "id_slider": dataForImage[0],
                                           "sezione": dataForImage[1],
                                           "pagination": dataForImage[2],
                                       })
        images = scrapy.selector.Selector(text=responseImages.text).css(
            '.ClickZoomGallery img::attr(data-src)').getall()
        item_loader.add_value('images',images)

        rent = response.css('.prezzo.bounceInRight ::text').extract()
        if rent[0] != 'Tratt. riservata':
            if len(rent) > 1:
                r = rent[1]
                item_loader.add_value('rent_string', r[r.rfind('€') + 1:].strip())
            else:
                item_loader.add_value('rent_string', rent[0][2:])
            yield item_loader.load_item()
