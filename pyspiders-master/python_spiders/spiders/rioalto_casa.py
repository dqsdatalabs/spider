# -*- coding: utf-8 -*-
# Author: Noor
import scrapy
from ..loaders import ListingLoader

class MySpider(scrapy.Spider):
    name = 'rioalto_casa'
    allowed_domains = ['rioalto_casa']
    start_urls = ['https://www.rioalto.casa/r/annunci/affitto-appartamento-.html?Codice=&Motivazione%5B%5D=2&Tipologia%5B%5D=1&Comune=0&Prezzo_da=&Prezzo_a=&cf=yes']
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    thousand_separator = '.'
    scale_separator = '.'


    def parse(self, response):
        links = response.css('.button').xpath("@href").extract()
        for link in links:
            yield scrapy.Request(
                url=link,
                callback=self.get_property_details,
                dont_filter=True)

    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        ##
        item_loader.add_value('external_link', response.url)
        address = response.css('.dove_schimmo::text').extract()[0].strip()
        item_loader.add_value('address', address)
        item_loader.add_value('city',address.split('-')[0])
        ##
        item_loader.add_value('property_type', 'apartment')
        description ="".join(response.css('.realestate-scheda .testo p::text').extract())
        item_loader.add_value('description', description)

        indx = description.lower().index('deposit')
        if indx:
            chosen_description = description[indx - 20:indx-3]
            deposit=chosen_description[chosen_description.index('€')+1:chosen_description.rfind(',')].strip()
            item_loader.add_value('deposit',int(deposit.replace('.','')))

        ##
        images =  response.css('.swiper-slide img').xpath('@data-src').extract()[6:]
        item_loader.add_value('images', images)
        ##
        title = response.css('.titoloscheda::text').extract()[0]
        item_loader.add_value('title', title)
        item_loader.add_value('currency', 'EUR')
        item_loader.add_value('landlord_name', 'rioalto.casa')
        item_loader.add_value('landlord_phone', '041- 244 00 23')
        item_loader.add_value('landlord_email','info@rioalto.casa')
        item_loader.add_value('external_source', self.external_source)
        external_id = response.css('.codice span::text').extract()[0].strip()
        item_loader.add_value('external_id', external_id)
        rent = response.css('.prezzo::text').extract()[0][2:].strip()
        item_loader.add_value('rent_string', rent)

        bath = response.css('.ico-24-bagni span::text').extract()[0].strip()
        bath_count = [int(s) for s in bath.split() if s.isdigit()][0]
        item_loader.add_value('bathroom_count', bath_count)

        room = response.css('.ico-24-camere span::text').extract()[0].strip()
        room_count = [int(s) for s in room.split() if s.isdigit()][0]
        item_loader.add_value('room_count', room_count)

        sq =response.css('.ico-24-mq span::text').extract()[0].strip()
        sq_meters = [int(s) for s in sq.split() if s.isdigit()][0]
        item_loader.add_value('square_meters', sq_meters)

        dt_details = response.css('.box strong::text').extract()
        stripped_details = [i.strip() if type(i) == str else str(i) for i in dt_details]
        dd_values = response.css('.box::text').extract()
        stripped_values = [i.strip() if type(i) == str else str(i) for i in dd_values]



        if 'Arredato' in stripped_details:
            fur_index = stripped_details.index('Arredato')
            furnished = stripped_values[fur_index]
            if furnished == 'Si':
                item_loader.add_value('furnished', True)
            else:
                item_loader.add_value('furnished', False)



        if 'Piano' in stripped_details:
            floor_index = stripped_details.index('Piano')
            floor = stripped_values[floor_index]
            item_loader.add_value('floor', floor)







        yield item_loader.load_item()
