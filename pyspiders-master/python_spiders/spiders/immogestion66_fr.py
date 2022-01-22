# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy
from ..loaders import ListingLoader
from ..helper import extract_number_only, remove_unicode_char
import re
import js2xml
import lxml.etree
from parsel import Selector

class Immogestion66Spider(scrapy.Spider):
    
    name = 'immogestion66_fr'
    allowed_domains = ['immogestion66.fr']
    start_urls = ['https://www.immogestion66.fr']
    execution_type = 'testing'
    country = 'france'
    locale = 'fr'
    thousand_separator=','
    scale_separator='.'
    position = 0
    
    def start_requests(self):

        start_urls = [{'url':'https://immogestion66.fr/recherche-de-proprietes/page/1/?type=appartement',
        'property_type': 'apartment'},
        {'url':'https://immogestion66.fr/recherche-de-proprietes/page/1/?type=duplex',
        'property_type': 'apartment'},
        {'url':'https://immogestion66.fr/recherche-de-proprietes/page/1/?type=maison',
        'property_type': 'house'},
        {'url':'https://immogestion66.fr/recherche-de-proprietes/page/1/?type=maison-de-village',
        'property_type': 'house'},
        {'url':'https://immogestion66.fr/recherche-de-proprietes/page/1/?type=villa',
        'property_type': 'house'},
        {'url':'https://immogestion66.fr/recherche-de-proprietes/page/1/?type=studio',
        'property_type': 'apartment'}]
        for url in start_urls:
            yield scrapy.Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'request_url':url.get('url'),
                                 'property_type':url.get('property_type')})
                
    def parse(self, response, **kwargs):
        for item in response.xpath('.//*[contains(@class,"list_card__details")]'):
            url = item.xpath(".//a/@href").get()
            status = item.xpath(".//span[@class='status']//text()[contains(.,'Location')]").get()
            if status:
                yield scrapy.Request(
                    url=url,
                    callback=self.get_property_details,
                    meta={'request_url': url,
                            'property_type':response.meta.get('property_type')})
        
        if len(response.xpath('.//*[contains(@class,"list_card__details")]//a')) > 0:
            current_page = re.findall(r"(?<=page/)\d+", response.meta["request_url"])[0]
            next_page_url = re.sub(r"(?<=page/)\d+", str(int(current_page) + 1), response.meta["request_url"])
            yield scrapy.Request(
                url=response.urljoin(next_page_url),
                callback=self.parse,
                meta={'request_url': next_page_url,
                   'property_type':response.meta.get('property_type')}
            )
        
    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value('external_link', response.meta.get('request_url'))
        item_loader.add_value("external_source", "Immogestion66_PySpider_france_fr")
        """
        external_id = response.xpath('.//*[contains(text(),"ID du bien")]/following-sibling::p/text()').extract_first()
        if 'aucune' not in external_id.lower():
            item_loader.add_value('external_id', remove_unicode_char(external_id))
        """
        item_loader.add_xpath('external_id', './/input[@name="property_id"]/@value')
        title = response.xpath('.//*[contains(@class,"rh_page__title")]/text()').extract_first()
        item_loader.add_xpath('title','.//*[contains(@class,"rh_page__title")]/text()')
        
        item_loader.add_value('property_type',response.meta.get('property_type'))
            
        item_loader.add_xpath('rent_string','.//*[@class="price"]/text()')
        item_loader.add_xpath('address','.//*[contains(@class,"property_address")]/text()')

        # duplicate images are found so this methos is used for images
        images=response.xpath('.//*[@class="slides"]//a/@href').extract()
        item_loader.add_value('images',list(set(images)))
        item_loader.add_value('landlord_phone','04 68 35 67 55')
        item_loader.add_value('landlord_email','syndic@immogestion66.fr')
        item_loader.add_value('landlord_name','IMMOGESTION66')
        item_loader.add_xpath('description','.//*[@class="rh_content"]//text()')
        item_loader.add_xpath('bathroom_count','.//*[@class="rh_property__meta"]//*[contains(text(),"Salles de bain")]//following-sibling::div//*[@class="figure"]/text()')
        item_loader.add_xpath('square_meters','.//*[@class="rh_property__meta"]//*[contains(text(),"Surface")]//following-sibling::div//*[@class="figure"]/text()')
        #description = item_loader.get_output_value('description')
        room_count=response.xpath("//span[contains(.,'Pièces')]//following-sibling::span//text()").extract_first()
        if room_count:
            item_loader.add_value('room_count',room_count)
        elif room_count == None and item_loader.get_output_value('property_type')=='studio':
            item_loader.add_value('room_count','1')
        else:
            room_count = "".join(response.xpath("//p[contains(.,'chambre')]//text()").getall())
            if room_count:
                room_count = room_count.split("chambres")[0].strip().split(" ")[-1]
                item_loader.add_value("room_count", room_count)
            
        #https://immogestion66.fr/bien/f2-secteur-place-rigaud-5/
        #utilities and deposit
        item_loader.add_xpath('floor','.//span[contains(text(),"Etage")]/following-sibling::span/text()')
        deposit = re.search(r'(?<=Caution).*\d+\W',item_loader.get_output_value('description'),re.IGNORECASE)
        if deposit:
            if extract_number_only(deposit.group()).isnumeric():
                item_loader.add_value('deposit', extract_number_only(deposit.group()))

        utilities = re.search(r'(?<=Honoraire).*\d+\W',item_loader.get_output_value('description'),re.IGNORECASE)
        if utilities:
            if extract_number_only(utilities.group()).isnumeric():
                item_loader.add_value('utilities', extract_number_only(utilities.group()))
            
        item_loader.add_xpath('zipcode','.//span[contains(text(),"Code postal")]/following-sibling::span/text()')
        item_loader.add_xpath('city','.//span[contains(text(),"Ville")]/following-sibling::span/text()')

        #https://immogestion66.fr/bien/t2-avec-buanderie-secteur-torcatis-2/
        #balcony
        balcony = response.xpath('.//span[contains(text(),"Balcon")]/following-sibling::span/text()').extract_first()
        if balcony and balcony.lower() != ['0','non']:
            item_loader.add_value('balcony',True)

        # https://immogestion66.fr/bien/villa-3-faces-plain-pied-piscine-garage/
        swimming_pool = response.xpath('.//span[contains(text(),"Piscine")]/following-sibling::span/text()').extract_first()
        if swimming_pool:
            if swimming_pool.lower() != ['0','non']:
                item_loader.add_value('swimming_pool',True)
            else:
                item_loader.add_value('swimming_pool',False)

        # https://immogestion66.fr/bien/villa-3-faces-plain-pied-piscine-garage/
        terrace = response.xpath('.//span[contains(text(),"Terrasse")]/following-sibling::span/text()').extract_first()
        if terrace:
            if terrace.lower() != ['0','non']:
                item_loader.add_value('terrace',True)
            else:
                item_loader.add_value('terrace',False)

        # https://immogestion66.fr/bien/villa-3-faces-plain-pied-piscine-garage/
        if 'garage' in item_loader.get_output_value('description').lower():
            item_loader.add_value('parking',True)
        
        #https://immogestion66.fr/bien/studio-meuble-secteur/
        #furnished
        if 'non meuble' in title.lower():
            item_loader.add_value('furnished',False)
        elif 'meuble' in title.lower():
            item_loader.add_value('furnished', True)
        furnished = response.xpath('.//span[contains(text(), "Meublé:")]/../span[@class="value"]/text()').extract_first()
        if furnished and furnished == "1":
            item_loader.add_value('furnished', True)
        elif furnished and furnished == "0":
            item_loader.add_value('furnished', False)
        
        self.position += 1
        item_loader.add_value('position', self.position)
        status = response.xpath("//p[contains(@class,'status')]//text()[not(contains(.,'Vente') or contains(.,'Programme Neuf'))]").get()
        if status:
            yield item_loader.load_item()
        
                