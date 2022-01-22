# -*- coding: utf-8 -*-
# Author: Valerii Nikitiuk
import scrapy
import re, json
from bs4 import BeautifulSoup
from ..loaders import ListingLoader
from python_spiders.helper import string_found, format_date, remove_white_spaces

class WaltonrobinsonSpider(scrapy.Spider):
    name = "waltonrobinson"
    allowed_domains = ["www.waltonrobinson.com"]
    start_urls = (
        'http://www.www.waltonrobinson.com/',
    )
    execution_type = 'testing'
    country = 'united_kingdom'
    locale ='en'
    thousand_separator=','
    scale_separator='.'

    def start_requests(self):
        start_urls = "https://www.waltonrobinson.com/rental-search"
        yield scrapy.Request( url=start_urls, callback=self.parse, dont_filter=True )

    def parse(self, response, **kwargs):
        page_number_text = response.xpath('//div[@class="pagination"]//li/a[contains(text(), "»")]/@href').extract_first()
        page_number = re.search(r'\?page=(.*)', page_number_text).group(1)
        for page in range(0, int(page_number)):
            next_link = 'https://www.waltonrobinson.com/rental-search?page={}'.format(page)
            yield scrapy.Request(url=next_link, callback=self.get_detail_urls, dont_filter=True)
    
    def get_detail_urls(self, response):
        links = response.xpath('//div[@class="grid-view search-results-view"]//a[contains(text(), "More Info")]')
        for link in links: 
            url = response.urljoin(link.xpath('./@href').extract_first())
            yield scrapy.Request(url=url, callback=self.get_property_details, dont_filter=True)
    def get_property_details(self, response):
        external_link = response.url
        title = response.xpath('//title/text()').extract_first().strip()
        if 'apartment' in title.lower():
            property_type = 'apartment'
        elif 'house' in title.lower():
            property_type = 'house'
        else:
            property_type = ''
        listings = response.xpath('//div[contains(@class, "information")]//li')
        square_meters = ''
        for listing in listings:
            elenments = listing.xpath('./text()').extract_first().strip()
            if 'm2' in elenments:
                square_meters = elenments.replace('m2', '')
        if property_type:
            room_count = str(response.xpath('//div[contains(@class, "main-room-info")]//span[contains(@id, "BedroomsLabel")]/text()').extract_first('').strip())        
            bathrooms = str(response.xpath('//div[contains(@class, "main-room-info")]//span[contains(@id, "BathroomsLabel")]/text()').extract_first('').strip()) 
            address = ''.join(response.xpath('//div[@class="main"]//h1/text()').extract()).strip()
            address = re.sub(r'[\r\n\s]+', '', address)
            rent_string = response.xpath('//span[contains(text(), "PCM")]/../text()').extract_first('').strip()
            avaiable_date_text = response.xpath('//div[@class="main"]//span[@class="avail-date"]/text()').extract_first('').strip().split(' ')[-1]
            available_date = format_date(avaiable_date_text, '%d %B %Y')
            description = ''.join(response.xpath('//div[@id="details-tab"]/p//text()').extract())
            item_loader = ListingLoader(response=response)
            item_loader.add_value('property_type', property_type)
            lat_lon = re.search(r'new google\.maps\.LatLng\((.*?)\)', response.text).group(1)
            lat = lat_lon.split(', ')[0]
            lon = lat_lon.split(', ')[1]  
            item_loader.add_value('latitude', str(lat))
            item_loader.add_value('longitude', str(lon))
            item_loader.add_value('external_link', external_link)
            listings = response.xpath('//div[contains(@class, "information")]//li')
            city = 'Newcastle'
            deposit_text = response.xpath('//p[contains(text(), "Deposit")]/text()').extract_first('').strip()
            if deposit_text:
                deposit = re.findall(r'([\d|,|\.]+)', deposit_text.split(' per ')[0])[0]
                item_loader.add_value('deposit', str(deposit))
            else:
                deposit = ''
            for listing in listings:
                elenments = listing.xpath('./text()').extract_first()
                if 'Furnished' in elenments:
                    item_loader.add_value('furnished', True)
                elif 'Parking' in elenments:
                    item_loader.add_value('parking', True)
                elif 'elevator' in elenments.lower():
                    item_loader.add_value('elevator', True)
                elif 'swimming' in elenments.lower():
                    item_loader.add_value('swimming_pool', True)
                elif 'terrace' in elenments.lower():
                    item_loader.add_value('terrace', True)
                elif 'balcony' in elenments.lower():
                    item_loader.add_value('balcony', True)
            item_loader.add_value('square_meters', str(square_meters))
            energy_level = response.xpath('//div[@class="current"]/span/span/text()').extract_first('').strip()    
            if energy_level: 
                item_loader.add_xpath('energy_label', energy_level)
            item_loader.add_xpath('title', '//title/text()')
            item_loader.add_value('address', address)
            item_loader.add_value('city', city)
            item_loader.add_xpath('description', '//div[@id="details-tab"]/p//text()')
            item_loader.add_value('rent_string', rent_string)
            item_loader.add_xpath('images', '//div[@class="item"]/img[@itemprop="image"]/@src')
            item_loader.add_value('room_count', room_count)
            item_loader.add_value('bathroom_count', bathrooms)
            item_loader.add_value('available_date', available_date)
            item_loader.add_value('landlord_name', 'Walton Robinson')
            item_loader.add_value('landlord_email', 'enquiries@waltonrobinson.com')
            item_loader.add_value('landlord_phone', '0191 243 1000')
            item_loader.add_value("external_source", "{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale))
            yield item_loader.load_item()

       
