# -*- coding: utf-8 -*-
# Author: Noor

import scrapy
from ..loaders import ListingLoader

import re

class MySpider(scrapy.Spider):
    name = 'accomodationsrome_com'
    allowed_domains = ['accomodationsrome.com']
    start_urls = ['https://accomodationsrome.com/rome-properties-for-rent/']
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    thousand_separator = ','
    scale_separator = '.'

    def parse(self, response):
        links = response.css('.btn-item').xpath('@href').extract()
        for link in links:
            yield scrapy.Request(url=link, callback=self.get_property_details, cb_kwargs={'link': link},
                                 dont_filter=True)

    def get_property_details(self, response, link):
        item_loader = ListingLoader(response=response)
        item_loader.add_value('external_link', link)
        details_values = response.css('.list-2-cols span::text').extract()
        details_array = response.css('.list-2-cols strong::text').extract()

        address = response.css('.item-address::text').extract()[0]
        item_loader.add_value('address', address)

        if 'Zip/Postal Code' in details_array:
            index = details_array.index('Zip/Postal Code')
            zip = details_values[index]
            item_loader.add_value('zipcode', zip)

        if 'Property Size:' in details_array:
            sq_index = details_array.index('Property Size:')
            sq = details_values[sq_index]
            sq_meters = [int(s) for s in sq.split() if s.isdigit()][0]
            item_loader.add_value('square_meters', sq_meters)
        if 'City' in details_array:
            index = details_array.index('City')
            city = details_values[index]
            item_loader.add_value('city', city)
        if 'Property ID:' in details_array:
            id_index = details_array.index('Property ID:')
            external_id = details_values[id_index]
            item_loader.add_value('external_id', external_id)
        else:
            external_id = response.xpath('//link[@rel="shortlink"]').xpath('@href').extract()[0][-4:]
            item_loader.add_value('external_id', external_id)


        rent_string = response.css('.item-price::text').extract()[0][1:]
        item_loader.add_value('rent_string', rent_string)

        if 'Property Type:' in details_array:
            prop_index = details_array.index('Property Type:')
            property_type = details_values[prop_index]
            if property_type=="Penthouse":
                item_loader.add_value('property_type', 'house')
            else:
                item_loader.add_value('property_type', property_type)
        else:
            item_loader.add_value('property_type', 'apartment')


        desc_contact = response.css('p::text').extract()[0]
        dot_index = desc_contact[0:len(desc_contact)-5].rfind('.')
        description = response.css('p::text').extract()[0][:dot_index]
        item_loader.add_value('description', description)
        ##
        images = [response.css('#pills-gallery').xpath('@style').extract()[0][22:-2]]
        item_loader.add_value('images', images)

        title = response.css('.page-title h1::text').extract()[0]
        item_loader.add_value('title', title)
        item_loader.add_value('currency', 'EUR')
        name = response.css('.agent-name::text').extract()
        if name and name[0]:
            item_loader.add_value('landlord_name', name[0])
        else:
            item_loader.add_value('landlord_name', 'accomodationsrome')

        phone = response.css('.agent-phone::text').extract()
        if phone and phone[0]:
            item_loader.add_value('landlord_phone', phone[0])
        else:
            item_loader.add_value('landlord_phone', '+39 06 98184459')
        item_loader.add_value('landlord_email', 'info@accomodationsrome.com')
        item_loader.add_value('external_source', self.external_source)

        if 'Bathrooms:' in details_array:
            bath_index = details_array.index('Bathrooms:')
            bathroom_count = int(details_values[bath_index])
            item_loader.add_value('bathroom_count', bathroom_count)

        if 'Bedrooms:' in details_array:
            room_index = details_array.index('Bedrooms:')
            room_count = int(details_values[room_index])
            item_loader.add_value('room_count', room_count)
        else:
            item_loader.add_value('room_count', 1)

        location_link = response.css('script::text').extract()[-2]
        if "lat" in location_link:
            l = location_link[location_link.index('"lat"'):location_link.index('"lat"')+50].split(',')
            lat =l[0][l[0].rfind(':')+2:l[0].rfind('"')]
            long =l[1][l[1].rfind(':')+2:l[1].rfind('"')]
            item_loader.add_value('longitude', long)
            item_loader.add_value('latitude', lat)


        yield item_loader.load_item()
