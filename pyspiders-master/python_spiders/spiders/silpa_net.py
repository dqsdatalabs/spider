# -*- coding: utf-8 -*-
# Author: Noor

import scrapy
from ..loaders import ListingLoader
import json

class MySpider(scrapy.Spider):
    name = 'silpa_net'
    allowed_domains = ['silpa.net']
    start_urls = [
        'https://silpa.net/Services/ImmobiliJSON.ashx?contratto=A&tipo=Alloggio&regione=&provincia=&comune=&indirizzo=&pg=1'
        ,
        'https://silpa.net/Services/ImmobiliJSON.ashx?contratto=A&tipo=Alloggio%20arredato&regione=&provincia=&comune=&indirizzo=&pg=1']
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    thousand_separator = '.'
    scale_separator = '.'

    def parse(self, response, *args):
        jsonresponse = json.loads(response.text)
        ids = []
        for i in range(jsonresponse['itemsOnPg']):
            ids.append(jsonresponse['listResult'][i]["IDScheda"])
        for id in ids:
            link = 'https://silpa.net/scheda.aspx?id=' + str(id)
            index = ids.index(id)
            yield scrapy.Request(url=link,
                                 callback=self.get_property_details,
                                 cb_kwargs={'link': link, 'jsonresponse': jsonresponse, 'i': index},
                                 dont_filter=True)

    def get_property_details(self, response, link, jsonresponse, i):
        item_loader = ListingLoader(response=response)

        item_loader.add_value('external_link', link)
        external_id = jsonresponse['listResult'][i]["IDScheda"]
        item_loader.add_value('external_id', str(external_id))

        item_loader.add_value('currency', 'EUR')
        item_loader.add_value('landlord_name', 'silpa')
        item_loader.add_value('landlord_email', ' info@silpa.net')
        item_loader.add_value('landlord_phone', '(011) 619.94.94')
        item_loader.add_value('external_source', self.external_source)
        item_loader.add_value('property_type', 'apartment')
        address = jsonresponse['listResult'][i]["Indirizzo"]
        item_loader.add_value('address', address)

        rent_string = jsonresponse['listResult'][i]["Prezzo"]
        item_loader.add_value('rent_string', str(rent_string))

        floor = jsonresponse['listResult'][i]["Piano"]
        item_loader.add_value('floor', floor)

        description = jsonresponse['listResult'][i]["DescrizioneEstesa"]
        item_loader.add_value('description', description)

        if 'balcon' in description:
            item_loader.add_value('balcony',True)
        else:
            item_loader.add_value('balcony', False)

        sq_meters = jsonresponse['listResult'][i]["Superficie"]
        item_loader.add_value('square_meters', sq_meters)

        if 'camer' in description:
            room_index=description.index('camer')
            room_count = description[room_index - 2:room_index].strip()
            if room_count in '1 2 3 4 5 6 7 8 9':
                item_loader.add_value('room_count', int(room_count))
            else:
                item_loader.add_value('room_count',1)
        elif 'soggiorn' in description:
            item_loader.add_value('room_count',1)
        if 'bagn' in description:
            bathroom_index=description.index('bagn')
            bathroom_count = description[bathroom_index - 2:bathroom_index].strip()
            if bathroom_count in '1 2 3 4 5 6 7 8 9':
                item_loader.add_value('bathroom_count', int(bathroom_count))
            else:
                item_loader.add_value('bathroom_count',1)

        city = jsonresponse['listResult'][i]["Citta"]
        item_loader.add_value('city', city)

        item_loader.add_value('title', 'Alloggio in Affitto')
        zipcode = jsonresponse['listResult'][i]["CAP"]
        item_loader.add_value('zipcode', zipcode)
        yield scrapy.Request(url=link,
                             callback=self.extract_imgs,
                             cb_kwargs={'item_loader': item_loader},
                             dont_filter=True)

    def extract_imgs(self, response, item_loader):
        images = response.css('#carousel img').xpath('@src').extract()
        item_loader.add_value('images', images)
        yield item_loader.load_item()
