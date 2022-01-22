# -*- coding: utf-8 -*-
# Author: Noor

import scrapy
from ..loaders import ListingLoader


class MySpider(scrapy.Spider):
    name = 'cecchiniregazzola_it'
    allowed_domains = ['cecchiniregazzola.com']
    start_urls = [
        'https://www.cecchiniregazzola.com/r-immobili/?Codice=&Motivazione%5B%5D=2&Tipologia%5B%5D=1&Prezzo_a_da=&Prezzo_a_a=&Locali_da=&Camere_da=&Bagni_da=&Totale_mq_da=&Totale_mq_a=&cf=yes&map_circle=0&map_polygon=0&map_zoom=0']
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    thousand_separator = '.'
    scale_separator = '.'

    def parse(self, response, *args):
        main_links = []
        for i in range(1, 4):
            main_links.append(
                'https://www.cecchiniregazzola.com/moduli/realestate/immobili_elenco_dettaglio.php?p=' + str(
                    i) + '&loadAjax=yes')
        for main_link in main_links:
            yield scrapy.Request(url=main_link, callback=self.parse2, cb_kwargs={'main_link': main_link},
                                 dont_filter=True)

    def parse2(self, response, main_link):
        pages_links = response.css('li a').xpath('@href').extract()
        pages_links = list(dict.fromkeys(pages_links))
        for link in pages_links:
            yield scrapy.Request(url=link, callback=self.get_property_details, cb_kwargs={'link': link},
                                 dont_filter=True)

    def get_property_details(self, response, link):
        item_loader = ListingLoader(response=response)
        item_loader.add_value('external_link', link)
        address = response.css('.zonaSchimmo::text').extract()[0]
        item_loader.add_value('address', address)
        item_loader.add_value('city', address[0:address.index('-')].strip())
        sq_meters = int(response.css('.boxCx .ico:nth-child(1) span::text').extract()[0][:-2].replace('.', '').strip())
        item_loader.add_value('square_meters', sq_meters)
        external_id = response.css('.codiceSchimmo::text').extract()[0][5:]
        item_loader.add_value('external_id', external_id)
        rent_string = response.css('.prezzoSchimmo::text').extract()[0].replace('€', '').strip()
        item_loader.add_value('rent_string', rent_string)
        item_loader.add_value('property_type', 'apartment')
        description = ''.join(response.css('.bounceInUp p::text').extract())
        item_loader.add_value('description', description)
        images = response.css('li.ClickZoomGallery img').xpath('@data-src').extract()
        item_loader.add_value('images', images)
        title = response.css('h1::text').extract()[0].strip()
        item_loader.add_value('title', title)
        item_loader.add_value('currency', 'EUR')
        item_loader.add_value('landlord_name', 'cecchiniregazzola')
        item_loader.add_value('landlord_email', 'info@cecchiniregazzola.com')
        item_loader.add_value('landlord_phone', '0289452255')
        item_loader.add_value('external_source', self.external_source)
        bath_count = response.css('.boxCx .ico~ .ico+ .ico span::text').extract()
        if bath_count and bath_count[0]:
            bathroom_count = int(bath_count[0])
            item_loader.add_value('bathroom_count', bathroom_count)

        features = response.css('.box::text').extract()
        for i in features:
            if i[0:9] == 'Arredato:':
                if i[9:] == ' Parzialmente arredato di cucina' or i[9:] == ' Arredato':
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
        loc = response.css('#sezMappa ::text').extract()
        if loc and loc[3]:
            location = loc[3]
            lat = location[location.index('= "') + 3:location.index('";')]
            item_loader.add_value('latitude', lat)
            lng = location[location.rfind('= "') + 3:location.rfind('";')]
            item_loader.add_value('longitude', lng)
        rooms = response.css('.boxCx .ico:nth-child(2) span::text').extract()
        if rooms and rooms[0]:
            room_count = int(rooms[0])
            item_loader.add_value('room_count', room_count)
        yield item_loader.load_item()
