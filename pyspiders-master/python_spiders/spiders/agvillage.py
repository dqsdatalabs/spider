# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy
import re
from ..loaders import ListingLoader
from python_spiders.helper import string_found, format_date

class AgvillageSpider(scrapy.Spider):
    name = 'agvillage'
    allowed_domains = ['agvillage']
    start_urls = ['https://www.agvillage.fr/']
    execution_type = 'testing'
    country = 'france'
    locale ='fr'
    thousand_separator='.'
    scale_separator=','
    
    def start_requests(self):
        start_urls = [
            {'url': 'https://www.agvillage.fr/catalog/advanced_search_result.php?action=update_search&search_id=&map_polygone=&C_28=Location&C_28_search=EGAL&C_28_type=UNIQUE&C_27_search=EGAL&C_27_type=TEXT&C_27=1&C_27_tmp=1&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&C_64_search=INFERIEUR&C_64_type=TEXT&C_64=&C_34=0&C_34_search=COMPRIS&C_34_type=NUMBER&C_34_MIN=&C_34_MAX=&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MIN=&C_30_MAX=&C_33_search=COMPRIS&C_33_type=NUMBER&C_33_MAX=&C_33_MIN=0&C_38_search=COMPRIS&C_38_type=NUMBER&C_38_MAX=&C_38_MIN=0&C_36_search=COMPRIS&C_36_type=NUMBER&C_36_MAX=&C_36_MIN=&C_2038_temp=on&C_2038=&C_2038_search=EGAL&C_2038_type=FLAG&keywords=', 'property_type': 'apartment'},
            {'url': 'https://www.agvillage.fr/catalog/advanced_search_result.php?action=update_search&search_id=1680735207251900&map_polygone=&C_28=Location&C_28_search=EGAL&C_28_type=UNIQUE&C_27_search=EGAL&C_27_type=TEXT&C_27=2&C_27_tmp=2&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&C_64_search=INFERIEUR&C_64_type=TEXT&C_64=&C_34=0&C_34_search=COMPRIS&C_34_type=NUMBER&C_34_MIN=&C_34_MAX=&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MIN=&C_30_MAX=&C_33_search=COMPRIS&C_33_type=NUMBER&C_33_MAX=&C_33_MIN=0&C_38_search=COMPRIS&C_38_type=NUMBER&C_38_MAX=&C_38_MIN=0&C_36_search=COMPRIS&C_36_type=NUMBER&C_36_MAX=&C_36_MIN=&C_2038_temp=0&C_2038=&C_2038_search=EGAL&C_2038_type=FLAG&keywords=', 'property_type': 'house'}
        ]
        for url in start_urls:
            yield scrapy.Request(
                url=url.get('url'),
                callback=self.parse, meta={'property_type': url.get('property_type')},
                dont_filter=True
            )

    def parse(self, response, **kwargs):
        links = response.xpath('//div[@class="un-bien-bloc-photo"]/a')
        for link in links: 
            url = response.urljoin(link.xpath('./@href').extract_first())
            yield scrapy.Request(url=url, callback=self.get_property_details, dont_filter=True, meta={'property_type': response.meta.get('property_type')})
        if response.xpath('//a[@class="page_suivante"]/@href'):
            next_link = response.urljoin(response.xpath('//a[@class="page_suivante"]/@href').extract_first())
            yield scrapy.Request(url=next_link, callback=self.parse, dont_filter=True, meta={'property_type': response.meta.get('property_type')}) 

    def get_property_details(self, response):
        external_id = response.xpath('//span[contains(text(), "Ref")]/text()').extract_first().strip().replace('Ref. :', '')
        external_link = response.url
        city = response.xpath('//div[contains(text(), "Ville")]/following-sibling::div/b/text()|//div[contains(text(), "ville")]/following-sibling::div/b/text()').extract_first()
        zipcode = response.xpath('//div[contains(text(), "Postal")]/following-sibling::div/b/text()|//div[contains(text(), "postal")]/following-sibling::div/b/text()').extract_first()
        address = city + ' ' + zipcode  
        property_type = response.meta.get('property_type')
        available_date = response.xpath('//div[contains(text(), "Disponibilité")]/following-sibling::div/b/text()').extract_first()
        description = ''.join(response.xpath('//p[@class="fiche-description"]/text()').extract())
        rent = response.xpath('//span[@class="alur_loyer_price"]/text()').extract_first()
        rent = re.sub(r'[\s]+', '', rent)
        room_count = response.xpath('//div[contains(text(), "Chambres")]/following-sibling::div/b/text()|//div[contains(text(), "chambres")]/following-sibling::div/b/text()').extract_first()
        item_loader = ListingLoader(response=response)
        if property_type:
            item_loader.add_value('property_type', property_type)
        item_loader.add_value('external_id', external_id)
        item_loader.add_value('external_link', external_link)
        item_loader.add_value('address', address)
        item_loader.add_value('description', description)
        item_loader.add_value('rent_string', rent)
        item_loader.add_xpath('images', '//ul[@class="slides"]/li/a/@href')
        item_loader.add_xpath('square_meters', '//div[text()="Surface"]/following-sibling::div/b/text()')
        item_loader.add_value('zipcode', zipcode)
        if available_date:
            item_loader.add_value('available_date', format_date(available_date))
        terrace_texts = response.xpath('//div[contains(text(), "Nombre de terrasses")]/following-sibling::div/b/text()').extract_first('')
        if terrace_texts:
            if int(terrace_texts) > 1:
                item_loader.add_value('terrace', True)
        parking_texts = response.xpath('//div[contains(text(), "parking")]/following-sibling::div/b/text()').extract_first('')
        if parking_texts: 
            if int(parking_texts) > 1:
                item_loader.add_value('parking', True)
        elevator_texts = response.xpath('//div[contains(text(), "Ascenseur")]/following-sibling::div/b/text()').extract_first('')
        if elevator_texts and 'oui' in elevator_texts:
            item_loader.add_value('elevator', True)
        item_loader.add_value('city', city)
        item_loader.add_xpath('floor', '//div[contains(text(), "étage")]/following-sibling::div/b/text()')
        item_loader.add_value('room_count', room_count)
        item_loader.add_value('landlord_name', "GNIMMO - L'AGENCE DU VILLAGE Villeneuve les sablons")
        item_loader.add_value('landlord_email', 'contact@agvillage.fr')
        item_loader.add_value('landlord_phone', '03.44.45.02.95')
        item_loader.add_value('external_source', 'AGVILLAGE_PySpider_france_fr')
        yield item_loader.load_item()


         