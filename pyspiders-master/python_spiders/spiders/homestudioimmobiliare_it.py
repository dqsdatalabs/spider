# -*- coding: utf-8 -*-
# Author: Noor

import scrapy
from scrapy import FormRequest

from ..loaders import ListingLoader

class MySpider(scrapy.Spider):
    name = 'homestudioimmobiliare_it'
    allowed_domains = ['homestudioimmobiliare.it']
    start_urls = [
        'https://www.homestudioimmobiliare.it/immobili/?pagina=0&contratto=Affitto&tipologia=1&localita='    ]
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    scale_separator = '.'
    thousand_separator=','

    def parse(self, response):
        links = response.css('.left_lente a').xpath('@href').extract()
        s=response.css('.right_mq ::text').extract()
        sqs=[i[:-2] for i in s]
        for i in range (len(links)):
            yield FormRequest(
                url=links[i],
                callback=self.get_property_details,
                cb_kwargs={"sq":sqs[i]},
                dont_filter=True)


    def get_property_details(self, response,sq):
        item_loader = ListingLoader(response=response)
        item_loader.add_value('external_link', response.url)
        item_loader.add_value('square_meters',int(sq))
        ##heeere
        item_loader.add_value('title', 'Appartmento')
        city=response.css('h3::text').extract()[0]
        item_loader.add_value('city', city)
        item_loader.add_value('address',city)
        description =response.css('.col-sm-4 .col-sm-12 p::text').extract()[0]
        item_loader.add_value('description', description)
        d = description
        if 'camere' in description.lower():
            i = d.lower().index('camere')
            item_loader.add_value('room_count',int(d[i-2]))
        if 'bagno' in description.lower():
            item_loader.add_value('bathroom_count', 1)
        if 'disponibile dal' in description.lower():
            i = d.lower().index('disponibile dal')
            date=d[i+15:i+30]
            item_loader.add_value('available_date',date)
        id = response.url.split('/')[-1]
        item_loader.add_value('external_id',id)
        item_loader.add_value('currency', 'EUR')
        item_loader.add_value('landlord_name', 'Home Studio Immobiliare')
        item_loader.add_value('landlord_phone', '051 5876516')
        item_loader.add_value('landlord_email','homestudioimmobiliare@gmail.com')
        item_loader.add_value('external_source', self.external_source)
        item_loader.add_value('property_type', 'apartment')
        rent =response.css('.prezzo ::text').extract()[0]
        item_loader.add_value('rent_string', rent)
        imgs=response.css('img').xpath('@src').extract()[1:-5]
        item_loader.add_value('images',imgs)

        details = response.css('.caratter_tecnic ::text').extract()
        stripped_details = [i.strip().lower() if type(i) == str else str(i) for i in details]

        if 'spese condominiali:' in stripped_details:
                i = stripped_details.index('spese condominiali:')
                value = stripped_details[i + 1]
                item_loader.add_value('utilities', value)


        yield item_loader.load_item()
