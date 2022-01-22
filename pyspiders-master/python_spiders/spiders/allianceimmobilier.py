# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
import scrapy
from ..loaders import ListingLoader
from python_spiders.helper import string_found, remove_white_spaces

def extract_city_zipcode(_address):
    if _address: 
        zipcode_city = _address.split(",")[1] 
        zipcode, city = zipcode_city.split(" ")
        return zipcode, city

class AllianceimmobilierSpider(scrapy.Spider):
    name = 'allianceimmobilier'
    allowed_domains = ['allianceimmobilier']
    start_urls = ['http://www.allianceimmobilier.com/index.html?type=location']
    execution_type = 'testing'
    country = 'france'
    locale ='fr'
    thousand_separator=','
    scale_separator='.'
    
    def start_requests(self):
        url = "http://www.allianceimmobilier.com/index.html?type=location"
        yield scrapy.Request(url=url, callback=self.parse_first, dont_filter=True) 
   
    def parse_first(self, response):
        start_urls = [
            {'url': 'http://www.allianceimmobilier.com/location.html?type_bien=APPARTEMENT&ville=&rayon=0&area=0&surfaceRange=1&surface_min=0&surface_max=9999999&priceRange=1&price_min=0&price_max=9999', 'property_type': 'apartment'},
            {'url': 'http://www.allianceimmobilier.com/location.html?surfaceRange=1&surface_min=0&surface_max=9999999&priceRange=1&price_min=0&price_max=9999999&type_bien=MAISON&ville=&rayon=0', 'property_type': 'house'},
        ]
        for url in start_urls:
            yield scrapy.Request(
                url=url.get('url'),
                callback=self.parse, meta={'property_type': url.get('property_type')},
                dont_filter=True
            )

    def parse(self, response, **kwargs):
        links = response.xpath('//div[@class="propertyItem"]//a[@class="propertyImgLink"]')
        for link in links: 
            url = response.urljoin(link.xpath('./@href').extract_first())
            yield scrapy.Request(url=url, callback=self.get_property_details, dont_filter=True, meta={'property_type': response.meta.get('property_type')})

        if response.xpath('//a[contains(text(), ">")]/@href'):
            next_link = response.urljoin(response.xpath('//a[contains(text(), ">")]/@href').extract_first())
            yield scrapy.Request(url=next_link, callback=self.parse, dont_filter=True, meta={'property_type': response.meta.get('property_type')}) 

    def get_property_details(self, response):
        external_id = response.xpath('//input[@name="ref"]/@value').extract_first().strip().replace('Ref :', '')
        external_link = response.url
        contact_infos = response.xpath('//div[contains(@class, "recentPosts")]/text()').extract()
        if ''.join(contact_infos).strip():
            address = remove_white_spaces(contact_infos[2] + ',' + contact_infos[3])
            landlord_phone = contact_infos[5] 
            zipcode, city = extract_city_zipcode(address)
        else:
            address = ''
        property_type = response.meta.get('property_type')
        description = ''.join(response.xpath('//meta[@property="og:description"]/@content').extract())
    
        item_loader = ListingLoader(response=response)
        if property_type:
            item_loader.add_value('property_type', property_type)
        title = response.xpath("//h1/text()").extract_first()
        if title:
            item_loader.add_value('title',title.strip())
        item_loader.add_value('external_id', external_id)
        item_loader.add_value('external_link', external_link)
        item_loader.add_value('address', address)
        item_loader.add_value('zipcode', zipcode)
        item_loader.add_value('city', city)
        item_loader.add_xpath('description', '//meta[@property="og:description"]/@content')
        rent = ''.join(response.xpath('//p[@class="price"]//text()').extract())
        if rent:
            item_loader.add_value('rent_string', rent.strip().replace(" ",""))
        square_meters = ''.join(response.xpath('//li[contains(text(), "Surface")]/span/text()').extract())
        if square_meters:
            item_loader.add_value('square_meters', square_meters.split("m")[0].split(".")[0])
        item_loader.add_xpath('images', '//div[@id="bx-pager"]/a/img/@src')
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
        item_loader.add_xpath('room_count', '//li[contains(text(), "Pièce")]/span/text()')
        item_loader.add_value('landlord_name', 'Alliance Immobilier')
        item_loader.add_xpath('landlord_email', '//div[contains(@class, "recentPosts")]/a/text()')
        item_loader.add_value('landlord_phone', landlord_phone)
        item_loader.add_value('external_source', 'Allianceimmobilier_PySpider_france_fr')
        yield item_loader.load_item()



         