# -*- coding: utf-8 -*-
# Author: Valerii Nikitiuk
import scrapy
import re
from ..loaders import ListingLoader

def extract_city_zipcode(_address):
    city = _address.split(", ")[-2]
    zipcode = _address.split(", ")[-1]
    return city, zipcode 

class CluttonsSpider(scrapy.Spider):
    name = "cluttons"
    allowed_domains = ["cluttons.com"]
    start_urls = (
        'http://www.cluttons.com/',
    )
    execution_type = 'testing'
    country = 'united_kingdom'
    locale ='en'
    thousand_separator=','
    scale_separator='.'

    def start_requests(self):
        start_urls = [
            {'url': 'https://www.cluttons.com/property-search/residential-apartments-available-to-rent-in-uk', 'property_type': 'apartment'},
            {'url': 'https://www.cluttons.com/property-search/residential-houses-available-to-rent-in-uk', 'property_type': 'house'}
        ]
        for url in start_urls:
            yield scrapy.Request( url=url.get('url'), callback=self.parse, meta={'property_type': url.get('property_type')})

    def parse(self, response, **kwargs):
        if 'apartment' in response.meta.get('property_type'): 
            pages_text = response.xpath('//div[@class="section__content-inner"]/h1/text()').extract_first()
            pages = re.findall(r'\d+', pages_text)[0]
            total_pages = int(pages) + 1 
            for page in range(1, total_pages):
                link = response.url + '/page-{}'.format(page)
                yield scrapy.Request(url=link, callback=self.get_detail_urls, meta={'property_type': response.meta.get('property_type')})
        else:
            yield scrapy.Request(url=response.url, callback=self.get_detail_urls, meta={'property_type': response.meta.get('property_type')})
    def get_detail_urls(self, response):
        links = response.xpath('//div[@class="card__inner"]/a')
        for link in links: 
            url = response.urljoin(link.xpath('./@href').extract_first())
            yield scrapy.Request(url=url, callback=self.get_property_details, meta={'property_type': response.meta.get('property_type')})

    def get_property_details(self, response):
        # parse details of the propery
        property_type = response.meta.get('property_type')
        external_link = response.url
        external_id = response.xpath('//p[@class="ref_no"]/text()').extract_first('').strip().split(': ')[-1]
        address = response.xpath('//div[@class="details__content"]/h1/text()').extract_first('').strip()
        city, zipcode = extract_city_zipcode(address) 
        room_count = response.xpath('//i[@class="icon-Bedroom"]/following-sibling::span/text()').extract_first('').strip()
        try:
            lat = re.findall(r'\"lat\"\:(.*?)\,', response.text)[1]
            lon = re.findall(r'\"lon\"\:(.*?)\}', response.text)[-1]
        except:
            lat = ''
            lon = ''
        bathrooms = response.xpath('//i[@class="icon-Bathroom"]/following-sibling::span/text()').extract_first('').strip() 
        rent_string = response.xpath('//div[@class="details"]//span[@class="price-qualifier"]/text()').extract_first('').strip()
        rent_value = re.findall(r'([\d|,|\.]+)', rent_string)[0].replace(',', '')
        rent_month = str(int(rent_value) * 4) + '£'
        images = re.findall('background-image\:url\((.*?)\)', response.text)
        detail_text = response.xpath('//div[@class="section__inner-content section__inner-content-main"]/h5/text()').extract_first('')
        if detail_text and 'nd floor' in detail_text.lower():
            try:
                floor = re.search(r'the\s(.*?)nd floor', detail_text).group(1)      
            except:
                floor = ''
        else:
            floor = ''
        list_texts = response.xpath('//div[@class="section__inner-content section__inner-content-main"]//li')
        lift = ''
        swimming_pool = ''
        balcony =''
        square_meters = ''
        for list_text in list_texts:
            text = list_text.xpath('.//text()').extract_first('')
            if 'sq ft' in text.lower():  
                try:
                    square_meters_text = re.findall(r'([\d|,|\.]+)', text)[0]
                    square_meters = str(float(square_meters_text) / 10.764)  
                except:
                    square_meters = ''
            if 'lift access' in text.lower():
                lift = True
            if 'swimming pool' in text.lower():
                swimming_pool = True
            if 'balcony' in text.lower():
                balcony = True
        if room_count and lat and lon:
            item_loader = ListingLoader(response=response)
            item_loader.add_value('property_type', property_type)
            item_loader.add_value('external_link', external_link)
            item_loader.add_xpath('title', '//title/text()')
            item_loader.add_value('external_id', external_id)
            item_loader.add_value('address', address)
            item_loader.add_value('city', city)
            item_loader.add_value('zipcode', zipcode)
            item_loader.add_xpath('description', '//div[@class="section__inner-content section__inner-content-main"]//text()')
            item_loader.add_value('rent_string', rent_month)
            item_loader.add_value('images', images)
            item_loader.add_value('room_count', str(room_count))
            if lat:
                item_loader.add_value('latitude', str(lat))
            if lon:
                item_loader.add_value('longitude', str(lon))
            if bathrooms: 
                item_loader.add_value('bathroom_count', str(bathrooms))
            if square_meters:
                item_loader.add_value('square_meters', square_meters)
            if floor:
                item_loader.add_value('floor', str(floor))
            if lift:
                item_loader.add_value('elevator', True)
            if swimming_pool:
                item_loader.add_value('swimming_pool', True) 
            if balcony:
                item_loader.add_value('balcony', True) 
            item_loader.add_value('landlord_name', 'Cluttons - Islington')
            item_loader.add_value('landlord_email', 'info@cluttons.com')
            item_loader.add_value('landlord_phone', '+44 (0) 20 7408 1010')
            item_loader.add_value("external_source", "{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale))
            yield item_loader.load_item() 