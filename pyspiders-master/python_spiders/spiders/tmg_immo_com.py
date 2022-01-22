# -*- coding: utf-8 -*-
# Author: Madhumitha S
# Team: Sabertooth
import re
#import js2xml
#import lxml.etree
import scrapy
#from scrapy import Selector
from ..helper import extract_number_only, extract_rent_currency, remove_white_spaces
from ..loaders import ListingLoader
from math import ceil

class TmgImmoComSpider(scrapy.Spider):
    name = 'tmg_immo_com'
    allowed_domains = ['www.tmg-immo.com']
    start_urls = ['http://www.tmg-immo.com/']
    execution_type = 'testing'
    country = 'france'
    locale = 'fr'
    thousand_separator=' '
    scale_separator=','
    position = 0
    external_source = 'Tmg_Immo_PySpider_france'
    

    custom_settings = {
        "PROXY_TR_ON": True,
        "CONCURRENT_REQUESTS" : 4,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": .5,
        "AUTOTHROTTLE_MAX_DELAY": 2,
        "RETRY_TIMES": 3,
        "DOWNLOAD_DELAY": 1,
    }

    def start_requests(self):     
        start_urls = ["http://www.tmg-immo.com/a-louer/1"]   
        for url in start_urls:
            yield scrapy.Request(
                url=url,
                callback=self.parse,
                )

    def parse(self, response, **kwargs):
        listings = response.xpath('.//li[contains(@class,"col-xs-12")]')
        
        for property_item in listings:
            # title = property_item.xpath('.//h1/a/text()').extract_first()
            external_link = f"http://www.tmg-immo.com{property_item.xpath('.//h1/a/@href').extract_first()}"
            external_id = property_item.xpath('.//span[@itemprop="productID"]/text()').extract_first()

            yield scrapy.Request(
                url = external_link,
                callback=self.get_property_details,
                meta={
                    'external_link' : external_link,
                    # 'title' : title,
                    'external_id' : external_id
                    })
        next_page = response.xpath('(//span[@class="paginationChevron"])[4]/../@href')
        if next_page:
            next_page_url = f'''http://www.tmg-immo.com{response.xpath('(.//span[@class="paginationChevron"])[4]/../@href').extract_first()}'''
            yield scrapy.Request(url=next_page_url,
                                 callback=self.parse)

    def get_property_details(self, response):

        item_loader = ListingLoader(response=response)
        item_loader.add_value('external_link', response.meta.get('external_link'))
        item_loader.add_value('external_id', remove_white_spaces(response.meta.get('external_id')))
        item_loader.add_xpath('title', "//title/text()")
        item_loader.add_xpath('images', '//div[@class="mainImg"]//li/img/@src')
        item_loader.add_xpath('description', './/p[@itemprop="description"]/text()')
        rent_string = response.xpath('.//span[contains(text(),"Loyer CC* / mois")]/following::span[1]/text()').extract_first()
        item_loader.add_value('rent_string', rent_string)
        zipcode = response.xpath('.//span[contains(text(),"Code postal")]/following::span[1]/text()').extract_first()
        if zipcode:
            item_loader.add_value('zipcode', remove_white_spaces(zipcode))        
        city = response.xpath('.//span[contains(text(),"Ville")]/following::span/text()').extract_first()
        if city:
            item_loader.add_value('city', remove_white_spaces(city))
        square_meters = response.xpath('.//span[contains(text(),"Surface habitable (m²)")]/following::span[1]/text()').extract_first()
        if square_meters:
            square_meters = str(int(ceil(float(extract_number_only(remove_white_spaces(square_meters),thousand_separator=' ',scale_separator=',')))))
            item_loader.add_value('square_meters', square_meters)
        room_count = response.xpath('.//span[contains(text(),"Nombre de chambre")]/following::span[1]/text()').extract_first()
        if room_count:
            item_loader.add_value('room_count', remove_white_spaces(room_count))
        elif not room_count:
            room_count = response.xpath('.//span[contains(text(),"Nombre de pièces")]/following::span[1]/text()').extract_first()
            if room_count:
                item_loader.add_value('room_count', remove_white_spaces(room_count))
        floor = response.xpath('.//span[contains(text(),"Etage")]/following::span/text()').extract_first()
        if floor:
            item_loader.add_value('floor', remove_white_spaces(floor))
        elevator = response.xpath('.//span[contains(text(),"Ascenseur")]/following::span/text()').extract_first()
        if elevator:
            if remove_white_spaces(elevator) == 'OUI':
                item_loader.add_value('elevator', True)
            elif remove_white_spaces(elevator) == 'NON':
                item_loader.add_value('elevator', False)
        furnished = response.xpath('.//span[contains(text(),"Meublé")]/following::span[1]/text()').extract_first()
        if furnished:
            if remove_white_spaces(furnished) == 'OUI':
                item_loader.add_value('furnished', True)
            elif remove_white_spaces(furnished) == 'NON':
                item_loader.add_value('furnished', False)
        parking = response.xpath('.//span[contains(text(),"garage")]/following::span[1]/text()').extract_first()
        if parking:
            if remove_white_spaces(parking) == '0':
                item_loader.add_value('parking', False)
            else:
                item_loader.add_value('parking', True)
        
        bathroom_count = response.xpath('.//span[contains(text(),"Nb de salle de bain")]/following::span[1]/text()').extract_first()
        if bathroom_count:
            item_loader.add_value('bathroom_count', remove_white_spaces(bathroom_count))
        elif not bathroom_count:
            bathroom_count = response.xpath('.//span[contains(text(),"eau")]/following::span[1]/text()').extract_first()
            if bathroom_count:
                item_loader.add_value('bathroom_count', remove_white_spaces(bathroom_count))
        terrace = response.xpath('.//span[contains(text(),"Terrasse")]/following::span[1]/text()').extract_first()
        if terrace:
            if remove_white_spaces(terrace) == 'OUI':
                item_loader.add_value('terrace', True)
            elif remove_white_spaces(terrace) == 'NON':
                item_loader.add_value('terrace', False)
        balcony = response.xpath('.//span[contains(text(),"Balcon")]/following::span[1]/text()').extract_first()
        if balcony:
            if remove_white_spaces(balcony) == 'OUI':
                item_loader.add_value('balcony', True)
            elif remove_white_spaces(balcony) == 'NON':
                item_loader.add_value('balcony', False)
        deposit = response.xpath('.//span[contains(text(),"Dépôt de garantie")]/following::span[1]/text()').extract_first()
        if remove_white_spaces(deposit) != 'Non renseigné':
            item_loader.add_value('deposit', deposit)
        utilities = response.xpath('//p[span[contains(text(),"Charges locatives")]]/span[2]/text()').extract_first()
        if utilities:
            item_loader.add_value('utilities', utilities)

        item_loader.add_value('address', item_loader.get_output_value('city')+ ', ' +item_loader.get_output_value('zipcode'))
        latitude = response.xpath('.//script[contains(text(), "lat")]/text()').re_first(r'lat : (\d{0,3}\.\d{1,15})')
        if latitude:
            item_loader.add_value('latitude',latitude)
        longitude = response.xpath('.//script[contains(text(), "lng")]/text()').re_first(r'lng:  (\d{0,3}\.\d{1,15})')
        if longitude:
            item_loader.add_value('longitude',longitude)

        property_string = remove_white_spaces(response.xpath('.//div[@class="bienTitle themTitle"]/h1/text()').extract_first())
        apartment_types = ["appartement", "apartment", "flat",
                           "penthouse", "duplex", "triplex"]
        house_types = ['mobile home','park home','character property',
                'chalet', 'bungalow', 'maison', 'house', 'home', ' villa ',
                'holiday complex', 'cottage', 'semi-detached']
        studio_types = ["studio"]
        if any (i in  property_string.lower() or i in item_loader.get_output_value('description').lower()  for i in studio_types):
            item_loader.add_value('property_type','studio')
        elif any (i in property_string.lower() for i in apartment_types):
            item_loader.add_value('property_type','apartment')
        elif any (i in property_string.lower() for i in house_types):
            item_loader.add_value('property_type','house')
                        
        item_loader.add_value('landlord_name','Terres et Maisons Gestion')
        phone = re.findall(r'(?:\d{2}\.){4}\d{2}', item_loader.get_output_value('description'))
        if phone:
            landlord_phone = ' '.join(phone[0].split('.'))
            item_loader.add_value('landlord_phone', landlord_phone)
        elif not phone:    
            item_loader.add_value('landlord_phone', '03 88 54 85 09')
        email = re.findall(r'\S+@\S+', item_loader.get_output_value('description'))
        if email:
            item_loader.add_value('landlord_email', email)
        elif not email:
            item_loader.add_value('landlord_email', ' tmg67@wanadoo.fr')
        self.position+=1
        item_loader.add_value('position',self.position)
        item_loader.add_value("external_source", self.external_source)
        if item_loader.get_output_value('property_type'):
            yield item_loader.load_item()