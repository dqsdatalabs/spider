# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
import scrapy
import js2xml
import re
from ..loaders import ListingLoader
from python_spiders.helper import remove_unicode_char, extract_rent_currency, format_date

class CornelispartnersSpider(scrapy.Spider):
    name = 'cornelispartners'
    allowed_domains = ['cornelis-partners']
    start_urls = ['http://www.cornelis-partners.be/']
    execution_type = 'testing'
    country = 'belgium'
    locale ='fr'
    thousand_separator=','
    scale_separator='.'
    
    def start_requests(self):
        start_urls = [
            {'url': 'https://www.cornelis-partners.be/huren/appartement/alle-gemeenten?type=appartement', 'property_type': 'apartment'},
            {'url': 'https://www.cornelis-partners.be/huren/huis/alle-gemeenten?type=huis','property_type': 'house'}
        ]
        for url in start_urls:
            yield scrapy.Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})
    
    def parse(self, response, **kwargs):
    
        for link in response.xpath("//section[@class='estate-gallery']//div[@class='image'][not(contains(.,'verhuurd'))]/a/@href").extract():
            yield scrapy.Request(
                url=response.urljoin(link),
                callback=self.get_property_details,
                 meta={'property_type': response.meta.get('property_type')}, dont_filter=True)
        if response.xpath('//section[contains(@class, "pubs-wrapper")]/following-sibling::section//a[@title="Volgende"]'):
            next_url = response.urljoin(response.xpath('//section[@class="container pubs-wrapper"]/following-sibling::section//a[@title="Volgende"]/@href').extract_first())
            yield scrapy.Request(
                url=next_url,
                callback=self.parse,
                meta={'property_type': response.meta.get('property_type')}, dont_filter=True
                )

    def get_property_details(self, response):
        external_link = response.url
        property_type = response.meta.get('property_type')
        item_loader = ListingLoader(response=response)
        title = response.xpath('//meta[@property="og:title"]/@content').extract_first()
        item_loader.add_value('title', title)
        item_loader.add_value('external_source', 'Cornelispartners_PySpider_belgium_fr')
        address = response.xpath("//div[div[.='Adres:']]/div[2]/text()").extract_first('')
        if address:
            item_loader.add_value('address', address)        
            address = address.split(",")[-1].strip()
            # zipcode, city = extract_city_zipcode(address)
            zipcode = address.split(" ")[0]
            city = " ".join(address.split(" ")[1:])
            item_loader.add_value('zipcode', zipcode)
            item_loader.add_value('city', city)
        # details_text = ''.join(response.xpath('//div[@id="omschrijving"]/p//text()').extract())
     
        images = [response.urljoin(x) for x in response.xpath("//div[@class='slider']//a//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)

        room_count_text = response.xpath("//div[div[.='Slaapkamers:']]/div[2]/text()").extract_first('')
        if room_count_text:
            item_loader.add_value('room_count', room_count_text)
  
        square_meters_text = response.xpath("//div[div[.='Bewoonbare oppervlakte:']]/div[2]/text()").extract_first('')
        if square_meters_text:
            item_loader.add_value('square_meters', square_meters_text.split("m")[0])
        else:
            square_meters_text = response.xpath("//div[div/img[@alt='oppervlakte']]/div[2]/text()").extract_first('')
            if square_meters_text:
                item_loader.add_value('square_meters', square_meters_text.split("m")[0])
     
        item_loader.add_xpath('bathroom_count', "//div[@class='slide']//div[div[.='Badkamers:']]/div[2]/text()")
        energy_label = response.xpath("//div[div[.='EPC klasse:']]/div[2]/text()").extract_first('')
        if energy_label:
            item_loader.add_value('energy_label', energy_label.strip())
        floor = response.xpath("//div[div[.='Verdiepingen:']]/div[2]/text()").extract_first('')
        if floor:
            item_loader.add_value('floor', floor.strip())
        rent_string = response.xpath("//div[div[.='Prijs:']]/div[2]/text()").extract_first('')
        if rent_string:
            item_loader.add_value('rent_string', rent_string.replace(".","").strip())
        item_loader.add_value('external_id', response.url.split("/")[-2])
        item_loader.add_value('property_type', property_type)
        item_loader.add_value('external_link', external_link)
      
        item_loader.add_xpath('description', '//div[@class="paragraph"]//text()')   
        parking = response.xpath("//div[div[.='Garages:' or .='Parkings (buiten):']]/div[2]/text()").extract_first('')
        if parking:
            item_loader.add_value('parking',True)
     
        terrace = response.xpath("//div[div[.='Terras oppervlakte:']]/div[2]/text()").extract_first('')
        if terrace:
            item_loader.add_value('terrace',True)
        item_loader.add_xpath('landlord_name', '//div[@class="name"]/span/text()')
        item_loader.add_xpath('landlord_email', '//div[@class="contact"]//a[contains(@href,"mail")]/text()')
        item_loader.add_xpath('landlord_phone', '//div[@class="contact"]//a[contains(@href,"tel")]/text()')

        if not item_loader.get_collected_values("landlord_name"):
            item_loader.add_value("landlord_name", "Cornelis & Partners")
        
        yield item_loader.load_item()



         