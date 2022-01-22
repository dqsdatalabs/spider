# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy
from scrapy import Request
import re
from ..loaders import ListingLoader
from ..user_agents import random_user_agent


class LatoileImmoSpider(scrapy.Spider):
    name = 'latoile_immo'
    allowed_domains = ['www.latoile.immo']
    start_urls = ['http://www.latoile.immo/']
    execution_type = 'testing'
    country = 'france'
    locale = 'fr'
    thousand_separator = ' '
    scale_separator = ','
    position = 0

    def start_requests(self):
        start_urls = [{
            'url': 'http://www.latoile.immo/fr/liste.htm?page=1&TypeModeListeForm=text&ope=2&filtre=8#page=1&TypeModeListeForm=text&ope=2&filtre=8',
            'property_type': 'house'
            },
            {
            'url': 'http://www.latoile.immo/fr/liste.htm?page=1&TypeModeListeForm=text&ope=2&filtre=2#page=1&TypeModeListeForm=text&ope=2&filtre=2',
            'property_type': 'apartment'
            },
        
        ]

        for url in start_urls:
            yield Request(url=url.get('url'),
                          callback=self.parse,
                          meta={'page': 1,
                                'response_url': url.get('url'),
                                'property_type': url.get('property_type')})

    def parse(self, response, **kwargs):
        listings = response.xpath('.//a[contains(text(),"Plus")]/@href').extract()

        for url in listings:
            url = url.split('&')[0]
            yield Request(url=url,
                          callback=self.get_property_details,
                          meta={'response_url': url,
                                'property_type': response.meta.get('property_type')})

        if len(listings) == 10:
            next_page_url = re.sub(r"page=\d+", 'page='+str(response.meta.get('page')+1), response.meta.get('response_url'))
            yield Request(url=next_page_url,
                          callback=self.parse,
                          meta={'response_url': next_page_url,
                                'property_type': response.meta.get('property_type'),
                                'page': response.meta.get('page')+1})

    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value('external_link', response.meta.get('response_url'))
        item_loader.add_xpath('external_id', '//div[span[.="Ref"]][last()]/span[@itemprop="productID"]/text()')
        item_loader.add_xpath('title', './/div[contains(@class,"bien-title")]/child::*/text()')
        
        item_loader.add_xpath('rent_string', './/div[contains(@class,"detail-bien-prix")]/text()')
        item_loader.add_xpath('deposit', '//span[contains(text(),"Dépôt de garantie")]/following-sibling::span/text()[.!="0"]')
        item_loader.add_xpath('square_meters', './/span[contains(@class,"surface")]/following-sibling::text()')
        room_count = response.xpath("//li[contains(., 'pièce')]").re_first(r'\d+')
        if room_count:
            item_loader.add_value('room_count', room_count)
        else:
            item_loader.add_xpath('room_count', 'substring-before(//span[contains(@class,"chambre")]/following-sibling::text(),"chambre")')
        item_loader.add_xpath('description', './/span[contains(@itemprop,"description")]/child::text()')
        item_loader.add_xpath('images', './/div[contains(@class,"large-flap")]//img[contains(@src,"photos-biens")]/@src')
        utilities = response.xpath("//span[contains(@itemprop,'description')]/child::text()[contains(.,'charges comprises dont')]").extract_first()
        if utilities:
            try:
                utilities = utilities.split("charges comprises dont")[1].split("euro")[0].split(",")[0]
                item_loader.add_value('utilities', utilities)
            except: pass
        
        energy_label = response.xpath('.//img[contains(@src, "-consommation-") and contains(@src, "image/nrj-")]/@src').extract_first()
        if energy_label:
            energy_label = re.findall(r"(?<=-consommation-)\d+", energy_label)[0]
            item_loader.add_value('energy_label', energy_label)

        if not item_loader.get_output_value('room_count'):
            item_loader.add_xpath('room_count', './/span[contains(@class,"piece")]/following-sibling::text()')
            if item_loader.get_output_value('room_count') == 1:
                item_loader.add_value('property_type', 'studio')

        if not item_loader.get_output_value('property_type'):
            item_loader.add_value('property_type', response.meta.get('property_type'))

        item_loader.add_xpath('latitude', './/li[@class="gg-map-marker-lat"]/text()')
        item_loader.add_xpath('longitude', './/li[@class="gg-map-marker-lng"]/text()')
        address = response.xpath('.//div[contains(@class,"bien-title")]/h2/text()').extract_first().split('(')
        if address:
            item_loader.add_value('city', address[0].strip())
            item_loader.add_value('zipcode', address[1].replace(')', '').strip())
            item_loader.add_value('address', "{} {}".format(address[0].strip(),address[1].replace(')', '').strip()))

        # if item_loader.get_output_value('latitude') and item_loader.get_output_value('longitude'):
        #     location = geolocator.reverse([item_loader.get_output_value('latitude'), item_loader.get_output_value('longitude')])
        #     if location.address:
        item_loader.add_xpath('latitude', "//div[@class='gg-map-container']//li[@class='gg-map-marker-lat']/text()")
        item_loader.add_xpath('longitude', "//div[@class='gg-map-container']//li[@class='gg-map-marker-lng']/text()")
        
        if item_loader.get_output_value('description'):
            # http://www.latoile.immo/fr/detail.htm?cle=340553561&monnaie=2
            if 'parking' in item_loader.get_output_value('description').lower():
                item_loader.add_value('parking', True)
            
            # http://www.latoile.immo/fr/detail.htm?cle=340553382&monnaie=2
            if 'ascenseur' in item_loader.get_output_value('description').lower():
                item_loader.add_value('elevator', True)
            
            # http://www.latoile.immo/fr/detail.htm?cle=340553566&monnaie=2
            if 'terrasse' in item_loader.get_output_value('description').lower():
                item_loader.add_value('terrace', True)

        item_loader.add_value('landlord_name', 'LA TOILE IMMOBILIÈRE')
        item_loader.add_value('landlord_phone', '04.67.30.93.93')

        self.position += 1
        item_loader.add_value('position', self.position)
        item_loader.add_value("external_source", "LatoileImmo_PySpider_{}_{}".format(self.country, self.locale))
        yield item_loader.load_item()
