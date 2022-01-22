# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
import scrapy
from ..loaders import ListingLoader
from python_spiders.helper import string_found
import re
def extract_city_zipcode(_address):
    city, zipcode =  _address.split("-")
    return zipcode, city

class PortleucateimmobilierSpider(scrapy.Spider):
    name = 'portleucateimmobilier'
    allowed_domains = ['portleucateimmobilier']
    start_urls = ['https://www.portleucateimmobilier.com/']
    execution_type = 'testing'
    country = 'france'
    locale ='fr' 
    thousand_separator=','
    scale_separator='.'
     
    def start_requests(self):
        start_urls = [
            {'url': 'https://www.portleucateimmobilier.com/index_catalogue.php?marche=1&sous_type=1&transaction=3&nb_adultes=0&nb_enfants=0&nb_bebe=0&nb_animaux=0&prix_f=&prix_t=&sous_type_1=1', 'property_type': 'apartment'},
            {'url': 'https://www.portleucateimmobilier.com/index_catalogue.php?marche=1&sous_type=2&transaction=3&nb_adultes=0&nb_enfants=0&nb_bebe=0&nb_animaux=0&prix_f=&prix_t=&sous_type_2=2', 'property_type': 'house'},
        ]
        for url in start_urls:
            yield scrapy.Request(
                url=url.get('url'),
                callback=self.parse, meta={'property_type': url.get('property_type')},
                dont_filter=True
            )
    
    def parse(self, response):
        links = response.xpath('//a[contains(text(), "suite")]')
        for link in links: 
            url = link.xpath('./@href').extract_first()
            yield scrapy.Request(url=url, callback=self.get_property_details, dont_filter=True, meta={'property_type': response.meta.get('property_type')})
        next_page = response.xpath("//a[span[.='Page suivante']]/@href[.!='javascript:void(0);']").get()
        if next_page:
            next_url = response.urljoin(next_page)
            yield scrapy.Request(url=next_url, callback=self.parse, dont_filter=True, meta={'property_type': response.meta.get('property_type')})

    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        external_id = response.xpath('//span[contains(text(), "Référence")]/following-sibling::span/text()').extract_first().strip()
        external_link = response.url
        title=response.xpath("//div/h1/span/text()").get()
        if title:
            item_loader.add_value("title",title)
        address_code = response.xpath('//span[contains(text(), "Réf")][@class="line-2"]/text()').extract_first()
        address = address_code.split('/')[0] 
        zipcode, city = extract_city_zipcode(address)
        description = ''.join(response.xpath('//p[contains(@class, "escription-text")]//text()').extract())
        property_type = response.meta.get('property_type')
        rent = response.xpath('//span[@data-toggle="tooltip"]/text()').extract_first()
        if rent:
            rent=rent.replace("€","").strip()
            item_loader.add_value('rent',int(rent)*4)
        item_loader.add_value("currency","EUR")
        
        if property_type:
            item_loader.add_value('property_type', property_type)
        item_loader.add_value('external_id', external_id)
        item_loader.add_value('external_link', external_link)
        item_loader.add_value('address', address)
        item_loader.add_xpath('description', '//p[contains(@class, "escription-text")]//text()')
        
        images =response.xpath("//div[@class='cover-inner-image-container']/img/@srcset").getall()
        if images:
            for i in range(1,len(images)):
                item_loader.add_value("images",(images[i].split("'")[0]))

        item_loader.add_xpath('square_meters', '//span[contains(text(), "Surface")]/following-sibling::span/text()')
        item_loader.add_value('zipcode', zipcode)
        if string_found(['parking', 'parkeerplaats', 'garage'], description):
            item_loader.add_value('parking', True)
        if string_found(['balcon'], description): 
            item_loader.add_value('balcony', True)
        if string_found(['ascenseur','lift'], description):
            item_loader.add_value('elevator', True) 
        if string_found(['terrasse', 'terrace'], description):
            item_loader.add_value('terrace', True)
        if string_found(['dishwasher', 'vaatwasser'], description):
            item_loader.add_value('dishwasher', True)
        item_loader.add_value('city', city)
        room_count=response.xpath('//span[contains(text(), "Chambre")]/following-sibling::span/text() | //span[contains(text(), "Pièce")]/following-sibling::span/text()').get()
        if room_count:
            item_loader.add_value('room_count', room_count)
        bathroom_count=response.xpath("//li[@class='list-inline-item  nombre_sde']/span/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count.strip())
        item_loader.add_value('landlord_name', 'PORT LEUCATE IMMOBILIER')
        item_loader.add_value('landlord_phone', '04.68.40.78.78')
        item_loader.add_value('external_source', 'Portleucateimmobilier_PySpider_france_fr')
        yield item_loader.load_item()



         