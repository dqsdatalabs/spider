# -*- coding: utf-8 -*-
# Author: Noor

import scrapy
from ..loaders import ListingLoader


class MySpider(scrapy.Spider):
    name = 'wolfandwolf_it'
    allowed_domains = ['wolfandwolf.it']
    start_urls = [
        'https://wolfandwolf.it/residenziali.php?lang_id=1&Contratto=Affitto&Comune=Milano&Superficie=&Prezzo=&submit=CERCA']
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    thousand_separator = '.'
    scale_separator = '.'

    def parse(self, response):
        links =  response.css('.butt').xpath('@href').extract()
        for link in links:
            yield scrapy.Request(
                url='https://wolfandwolf.it/' + link,
                callback=self.get_property_details,
                dont_filter=True)

    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value('external_link', response.url)
        title=response.css('.list_titles::text').extract()[0]
        item_loader.add_value('title',title)
        address = response.css('.nomobile .subtitles::text').extract()[0].strip()
        item_loader.add_value('address', address)
        d=address
        item_loader.add_value('city', d[d.index('(')+1:d.index(")")])
        sq = response.css('.boxed_infos::text').extract()[2].strip()
        sq_meters = [int(s) for s in sq.split() if s.isdigit()][0]
        item_loader.add_value('square_meters', sq_meters)
        external_id = response.css('.nomobile .subtitles strong::text').extract()[0][6:-1]
        item_loader.add_value('external_id', external_id)
        rent = response.css('.boxed_infos .subtitles::text').extract()[0].strip()[2:]
        item_loader.add_value('rent_string', rent)
        item_loader.add_value('property_type', 'apartment')
        description = ''.join(response.css('.col::text').extract()).strip()
        item_loader.add_value('description', description)
        images =response.css('#fancybox-close , .ico').xpath('@src').extract()
        item_loader.add_value('images', images)
        item_loader.add_value('currency', 'EUR')
        item_loader.add_value('landlord_name', 'wolfandwolf')
        item_loader.add_value('landlord_phone', '02 84964381')
        item_loader.add_value('landlord_email', 'info@wolfandwolf.it')
        item_loader.add_value('external_source', self.external_source)
        bath=response.css('.boxed_infos::text').extract()[8]
        bathroom_count = [int(s) for s in bath.split() if s.isdigit()][0]
        item_loader.add_value('bathroom_count', bathroom_count)


        script = ''.join(response.css('script ::text').extract())
        latlng = script[script.index('LatLng'):script.index('LatLng') + 60]
        lat=latlng[latlng.index("(")+1:latlng.index(',')]
        lng=latlng[latlng.index(",")+2:latlng.index(')')]
        item_loader.add_value('latitude',lat)
        item_loader.add_value('longitude',lng)

        info =response.css('.boxed_infos ::text').extract()
        for i in info:
            if'Spese condominiali mensili:' in i:
                ut=i[i.index(':')+1:i.index('€')].strip()
                item_loader.add_value('utilities',ut)
        rooms = response.css('.boxed_infos::text').extract()[4]
        if rooms :
            room_count =[int(s) for s in rooms.split() if s.isdigit()][0]
            item_loader.add_value('room_count', room_count)

        if response.css('.col::text').extract()[0].strip() !='AFFITTATO':
            yield item_loader.load_item()
