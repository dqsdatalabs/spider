# -*- coding: utf-8 -*-
# Author: Noor
import scrapy
from ..loaders import ListingLoader


class MySpider(scrapy.Spider):
    name = 'toplocations_it'
    allowed_domains = ['toplocations.it']
    start_urls = ['https://www.toplocations.it/ricerca-immobili/?type=living&status=for-rent']
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    thousand_separator = '.'
    scale_separator = '.'

    def parse(self, response):
        links = response.css('.rh_list_card__details a').xpath('@href').extract()
        for link in links:
            yield scrapy.Request(
                url=link,
                callback=self.get_property_details,
                dont_filter=True)

    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value('external_link', response.url)
        ##
        address = response.css('.rh_page__property_address::text').extract()[0].strip()
        item_loader.add_value('address', address)
        item_loader.add_value('property_type', 'apartment')
        ##
        description = ''.join(response.css('.rh_content p::text').extract()).strip()
        item_loader.add_value('description', description)
        images = response.css('.slides img').xpath('@src').extract()
        item_loader.add_value('images', images)
        ##
        title =response.css('.rh_page__title::text').extract()[0].strip()
        item_loader.add_value('title', title)
        item_loader.add_value('currency', 'EUR')
        item_loader.add_value('landlord_name', 'toplocations.it')
        item_loader.add_value('landlord_phone', '02 45474306')
        item_loader.add_value('landlord_email','info@toplocations.it')
        item_loader.add_value('external_source', self.external_source)

        ##
        external_id = response.css('.id::text').extract()[0].strip()
        item_loader.add_value('external_id', external_id)

        ##
        rooms = int(response.css('.rh_property__meta_wrap span::text').extract()[1])
        item_loader.add_value('room_count', rooms)
        bath = int(response.css('.rh_property__meta_wrap span::text').extract()[3])
        item_loader.add_value('bathroom_count', bath)


        ##
        sq_meters = int(response.css('.rh_property__meta_wrap span::text').extract()[5])
        item_loader.add_value('square_meters', sq_meters)
        ##
        rent = response.css('.price::text').extract()[0].strip()[1:]
        item_loader.add_value('rent_string', rent)

        yield item_loader.load_item()
