# -*- coding: utf-8 -*-
# Author: Noor

import scrapy
from ..loaders import ListingLoader


class MySpider(scrapy.Spider):
    name = 'immobiliareuniversitaria_com'
    allowed_domains = ['immobiliareuniversitaria.com']
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    thousand_separator = '.'
    scale_separator = '.'
    start_urls = ['https://www.immobiliareuniversitaria.com/?search-listings=true']

    def parse(self, response):
        pages_num = int(response.css('#search-listing-mapper a ::text').extract()[-2])
        for i in range(1, pages_num + 1):
            yield scrapy.Request(
                url='https://www.immobiliareuniversitaria.com/page/' + str(i) + '/?search-listings=true',
                callback=self.parse2,
                dont_filter=True)

    def parse2(self, response):
        main_links = response.css('#search-listing-mapper .marB0 a').xpath('@href').extract()
        for main_link in main_links:
            yield scrapy.Request(url=main_link, callback=self.get_details,
                                 dont_filter=True)

    def get_details(self, response):
        item_loader = ListingLoader(response=response)
        contract = response.css('.snipe-wrap span::text').extract()[0].strip()
        title = response.css('#listing-title::text').extract()[0].strip()
        if contract == 'Affitto' and 'commercial' not in title:
            item_loader.add_value('title', title)
            item_loader.add_value('external_link', response.url)
            external_id = response.css('.propid ::text').extract()[1]
            item_loader.add_value('external_id', external_id)
            address = response.css('.location ::text').extract()[0]
            item_loader.add_value('address', address)
            city = address.split('/')[0].strip()
            item_loader.add_value('city', city)
            item_loader.add_value('zipcode', address.split(' ')[-1])
            rent_string = response.css('.marT0 .listing-price::text').extract()[0][:-1]
            item_loader.add_value('rent_string', rent_string)
            item_loader.add_value('property_type', 'apartment')
            images = response.css('#carousel .size-listings-featured-image').xpath('@src').extract()
            item_loader.add_value('images', images)
            item_loader.add_value('currency', 'EUR')
            item_loader.add_value('landlord_name', 'immobiliareuniversitaria')
            email=response.css('.row~ .row+ .row a::text').extract()
            if email:
                item_loader.add_value('landlord_email', email[0])
            else:
                item_loader.add_value('landlord_email', 'appia@immobiliareuniversitaria.com')
            item_loader.add_value('landlord_phone', '0664803183')
            item_loader.add_value('external_source', self.external_source)
            rooms = response.css('.marB60 .beds .right::text').extract()
            if rooms:
                item_loader.add_value('room_count', int(rooms[0]))
            bath = response.css('.marB60 .baths .right::text').extract()
            if bath:
                item_loader.add_value('bathroom_count', int(bath[0]))

            sq= response.css('.sqft .right::text').extract()
            if sq:
                item_loader.add_value('square_meters', int(sq[0]))
            description = response.css('#listing-content p::text').extract()[0]
            item_loader.add_value('description', description)
            if 'piano' in description:
                d = description
                floor = d[d.index('piano') - 2:d.index('piano')].strip()
                item_loader.add_value('floor', floor)
            location = response.css('#listing-location script::text').extract()[0]
            l = location
            if 'LatLng' in l:
                latlng = l[l.index('LatLng'):l.index('LatLng') + 40].strip()
                lat = latlng[latlng.index('(') + 1:latlng.index(',')]
                lng = latlng[latlng.index(',') + 1:latlng.index(')')]
                item_loader.add_value('latitude', lat)
                item_loader.add_value('longitude', lng)

            yield item_loader.load_item()
