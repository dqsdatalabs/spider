# -*- coding: utf-8 -*-
# Author: Valerii Nikitiuk
import scrapy
import html
import re
from ..loaders import ListingLoader
from python_spiders.helper import string_found

class ImmotSpider(scrapy.Spider):
    name = "immolegrand"
    allowed_domains = ["immolegrand"]
    start_urls = (
        'https://www.immolegrand.com/',
    )
    execution_type = 'testing'
    country = 'belgium'
    locale ='nl'
    thousand_separator='.'
    scale_separator=','

    def start_requests(self):
        start_urls = [
            {'url': 'https://www.immolegrand.com/a-louer.php?typeFilter=Appartement&priceFilter=&cityFilter=', 'property_type': 'apartment'},
            {'url': 'https://www.immolegrand.com/a-louer.php?typeFilter=Flat/studio&priceFilter=&cityFilter=', 'property_type': 'house'}
        ]
        for url in start_urls:
            yield scrapy.Request(
                url=url.get('url'),
                callback=self.parse, meta={'property_type': url.get('property_type')},
                dont_filter=True
            )

    def parse(self, response, **kwargs):
        pagenations = response.xpath('//div[contains(@class, "pagination")]/a')
        for pagenation in pagenations:
            next_link = response.urljoin(pagenation.xpath('./@href').extract_first())
            link_text = html.unescape(pagenation.xpath('./text()').extract_first())
            if '»' in link_text or '«' in link_text:
                continue
            yield scrapy.Request(url=next_link, callback=self.get_detail_urls, dont_filter=True, meta={'property_type': response.meta.get('property_type')})
 
    def get_detail_urls(self, response):
        links = response.xpath('//div[@class="listing-biens"]//a[@class="infos"]')
        for link in links:
            url = response.urljoin(link.xpath('./@href').extract_first())
            yield scrapy.Request(url=url, callback=self.get_property_details, dont_filter=True, meta={'property_type': response.meta.get('property_type')})
 
    def get_property_details(self,response):
        address = response.xpath('//h2[@class="sousTitre"]/text()').extract_first()
        # property_type = response.meta.get('property_type')
        property_type_text = response.xpath('//span[contains(text(), "Type de bien")]/following-sibling::text()').extract_first('').strip()
        if 'appartement' in property_type_text.lower():
            property_type = 'apartment'
        elif 'maison' in property_type_text.lower():
            property_type = 'house'
        else:
            property_type = ''
        city = address.split(' ')[-1]
        zipcode = address.split(' ')[0]  
        if response.xpath('//span[contains(text(), "Chambre(s)")]/../text()'): 
            room_count = str(response.xpath('//span[contains(text(), "Chambre(s)")]/../text()').extract_first('').strip()) 
        else:
            room_count = "1"
        rent = response.xpath('//h1[@class="titreCap"]/text()').extract_first() + "€"
        rent = re.sub(r'[\s+]', '', rent)
        square_meters = response.xpath('//span[contains(text(), "Surface habitale")]/../text()').extract_first('').strip()
        square_meters = re.findall(r'\d+', square_meters)[0]
        if property_type and int(square_meters) > 0:
            item_loader = ListingLoader(response=response)
            item_loader.add_value('property_type', property_type)
            item_loader.add_value("external_link", response.url)
            item_loader.add_xpath('external_id', '//span[contains(text(), "Code unique")]/following-sibling::text()')
            item_loader.add_value('address', address)
            item_loader.add_value('city', city)
            item_loader.add_value('zipcode', zipcode)
            item_loader.add_xpath('title', '//meta[@porperty="og:title"]/@content')
            item_loader.add_xpath('description', '//h3[contains(text(), "Description")]/following-sibling::p/text()')
            item_loader.add_value('rent_string', rent)
            item_loader.add_xpath('images', '//div[@class="photos animate"]//img/@src')
            item_loader.add_xpath('square_meters', '//span[contains(text(), "Surface habitale")]/../text()')
            item_loader.add_value('room_count', room_count)
            item_loader.add_value('landlord_name', 'Immo Legrand')
            item_loader.add_value('landlord_email', 'info@immolegrand.com')
            item_loader.add_value('landlord_phone', '071 36 21 30')
            item_loader.add_value("external_source", "{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale))
            yield item_loader.load_item()
