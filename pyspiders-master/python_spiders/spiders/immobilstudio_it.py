# -*- coding: utf-8 -*-
# Author: Noor
import scrapy
from ..loaders import ListingLoader

class MySpider(scrapy.Spider):
    name = 'immobilstudio_it'
    allowed_domains = ['immobilstudio.it']
    start_urls = ['https://www.immobilstudio.it/ricerca.php?compravendita=affitto&tipologia=Appartamento&vani=tutti&min=1&max=500&cerca=Cerca']
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    thousand_separator = '.'
    scale_separator = ','

    def parse(self, response):
        links = response.css('.Titolo~ table a').xpath('@href').extract()
        for link in links:
            yield scrapy.Request(
                url='https://www.immobilstudio.it/'+link,
                callback=self.get_property_details,
                dont_filter=True)

    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value('external_link', response.url)
        item_loader.add_value('property_type', 'apartment')
        item_loader.add_value('external_id',response.url[response.url.index('=')+1:])
        imgs = response.css('.Principale table a img').xpath('@src').extract()
        images=['https://www.immobilstudio.it/'+i for i in imgs]
        item_loader.add_value('images', images)
        title = response.css('.Titolo div::text').extract()[0]
        item_loader.add_value('title', title)
        item_loader.add_value('currency', 'EUR')
        item_loader.add_value('landlord_name', 'immobilstudio.it')
        item_loader.add_value('landlord_phone', '0805227284')
        item_loader.add_value('external_source', self.external_source)
        desc_index=response.css('tr ::text').extract().index('Descrizione: ')
        desc=response.css('tr ::text').extract()[desc_index+1]
        item_loader.add_value('description', desc)

        features=response.css('td ::text').extract()
        if 'Prezzo di Locazione: 'in features:
            indx=features.index('Prezzo di Locazione: ')
            rent=features[indx+1][:-1]
            item_loader.add_value("rent_string",rent)
        if 'Spese Condominiali: 'in features:
            indx=features.index('Spese Condominiali: ')
            utility=features[indx+1][:-1]
            item_loader.add_value("utilities",utility)
        if 'Classe Energetica: ' in features:
            indx = features.index('Classe Energetica: ')
            energy = features[indx + 1]
            item_loader.add_value("energy_label", energy)
        if 'Indirizzo: 'in features:
            indx = features.index('Indirizzo: ')
            address = features[indx + 1]
            item_loader.add_value("address", address)
        if 'Località: 'in features:
            indx = features.index('Località: ')
            city = features[indx + 1]
            item_loader.add_value("city", city)
        if 'Vani: 'in features:
            indx =len(features) - 1 - features[::-1].index('Vani: ')
            rooms = features[indx + 1]
            item_loader.add_value("room_count", int(rooms))
        if 'Bagni: 'in features:
            indx = features.index('Bagni: ')
            rooms = features[indx + 1]
            item_loader.add_value("bathroom_count", int(rooms))
        if 'MQ: 'in features:
            indx = len(features) - 1 - features[::-1].index('MQ: ')
            sq = features[indx + 1]
            item_loader.add_value("square_meters", int(sq))
        if 'Posto Auto: 'in features:
            indx = features.index('Posto Auto: ')
            prk = features[indx + 1]
            if prk =='NO':
                item_loader.add_value("parking",False)
            else:
                item_loader.add_value("parking",True)
        if 'Ascensore: 'in features:
            indx = features.index('Ascensore: ')
            elev = features[indx + 1]
            if elev =='NO':
                item_loader.add_value("elevator",False)
            else:
                item_loader.add_value("elevator",True)


        yield item_loader.load_item()
