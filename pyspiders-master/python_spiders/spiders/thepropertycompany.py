# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
import scrapy
import re
from ..loaders import ListingLoader
from python_spiders.helper import format_date

def cleanText(text):
    price = re.findall(r'([\d|,|\.]+)', text, re.S | re.M | re.I)[0]
    price_re = price.replace('.00', '').replace(',', '')
    return price_re 
 
class ThepropertycompanySpider(scrapy.Spider):
    name = "thepropertycompany"
    allowed_domains = ["www.thepropertycompany.co.uk"]
    start_urls = (
        'http://www.www.thepropertycompany.co.uk/',
    )
    execution_type = 'testing'
    country = 'united_kingdom'
    locale ='en'
    thousand_separator=','
    scale_separator='.'

    def start_requests(self):
        start_urls = [
            {'url': 'https://www.thepropertycompany.co.uk/propertycompany/?wppf_location=&wppf_property_type=flat&wppf_min_rent=&wppf_max_rent=&wppf_min_bedrooms=&wppf_max_bedrooms=&wppf_max_bathrooms=&wppf_max_bathrooms=&wppf_radius=3', 'property_type': 'apartment'},
            {'url': 'https://www.thepropertycompany.co.uk/propertycompany/?wppf_location=&wppf_property_type=house&wppf_min_rent=&wppf_max_rent=&wppf_min_bedrooms=&wppf_max_bedrooms=&wppf_max_bathrooms=&wppf_max_bathrooms=&wppf_radius=3', 'property_type': 'house'}
        ]
        for url in start_urls:
            yield scrapy.Request(
                url=url.get('url'),
                callback=self.parse, meta={'property_type': url.get('property_type')},
                dont_filter=True
            )

    def parse(self, response, **kwargs):
        links = response.xpath('//a[@title="View Property"]')
        for link in links: 
            url = response.urljoin(link.xpath('./@href').extract_first())
            if 'station-road-finchley-central-n3' not in url: 
                yield scrapy.Request(url=url, callback=self.get_property_details, dont_filter=True, meta={'property_type': response.meta.get('property_type')})

    def get_property_details(self, response):
        property_type = response.meta.get('property_type')
        external_link = response.url
        external_id = response.xpath('//p[contains(text(), "ID")]/text()').extract_first('').strip().replace('ID: ', '')
        title=response.xpath('//div[@class="property-details"]//h2/text()').extract_first('').strip() 
        address = response.xpath('//div[@class="property-details"]//h2/text()').extract_first('').strip()   
        city_zipcode = address.split(',')
        city = city_zipcode[-2]
        zipcode = city_zipcode[-1].strip().split(" ")[-1]
        room_count = response.xpath('//ul[@class="property-meta"]/li[@class="bed"]/text()').extract_first('').strip()
        bathrooms = response.xpath('//ul[@class="property-meta"]/li[@class="bathroom"]/text()').extract_first('').strip() 
        rent_string = response.xpath('//p[contains(text(), "week")]/text()').extract_first('').strip().split(' per week ')[1]
        rent = re.sub(r'[\s]+', '', rent_string)
        lat_lon_text = response.xpath('//a[contains(text(), "Open In Google Maps")]/@href').extract_first().split('/')[-1]
        lat_lon = re.search(r'query=(.*)', lat_lon_text).group(1)
        latitude = str(lat_lon.split(',')[0])
        longitude = str(lat_lon.split(',')[-1])  
        available_date_tx = response.xpath('//p[contains(text(), "Available from")]/text()').extract_first('').strip().split(': ')[-1]
        available_date = format_date(available_date_tx, '%d %B %Y')
        details = response.xpath('//div[@class="property-details"]//div[contains(@id, "tab")]//text()').extract()
        details_text = []
        for det in details:
            det_text = re.sub(r'[\t\n]+', '', det)
            det_text = det_text.strip()  
            details_text.append(det_text)
        letters = []
        for letter in details_text:
            if letter:
                letters.append(letter)   
        letter_detail = ' '.join(letters)
        description = ''.join(response.xpath('//div[@class="description"]/p//text()').extract())
        dec = description + ' '+ letter_detail
        try:
            deposit = cleanText(response.xpath('//span[contains(text(), "Deposit")]/following-sibling::text()').extract_first('').strip())
        except:
            deposit = ''
        try:
            utility = cleanText(response.xpath('//span[contains(text(), "Tax Per Month")]/following-sibling::text()').extract_first('').strip())
        except:
            utility = ''
        images = []
        for img in response.xpath('//div[contains(@class, "slideshow-slide")]'):
            img_text = img.xpath('./@style').extract_first()
            image = re.search(r'url\((.*?)\)', img_text).group(1) 
            images.append(image)
        item_loader = ListingLoader(response=response)
        if property_type:
            item_loader.add_value('property_type', property_type)

        item_loader.add_value('external_link', external_link)
        item_loader.add_value('external_id', str(external_id))
        item_loader.add_value('title', title)
        item_loader.add_value('address', address)
        item_loader.add_value('city', city)
        item_loader.add_value('zipcode', zipcode)
        if deposit:
            item_loader.add_value('deposit', deposit)
        if utility:
            item_loader.add_value('utilities', utility)
        item_loader.add_value('description', dec)
        item_loader.add_value('rent_string', rent)
        item_loader.add_value('available_date', available_date)
        item_loader.add_value('images', images)
        item_loader.add_value('latitude', latitude)
        item_loader.add_value('longitude', longitude)
        item_loader.add_value('room_count', str(room_count))
        item_loader.add_value('bathroom_count', str(bathrooms))
        item_loader.add_value('landlord_name', 'Thepropertycompany')
        item_loader.add_value('landlord_email', 'n8@thepropertycompany.co.uk')
        item_loader.add_value('landlord_phone', '0208 348 8833')
        item_loader.add_value("external_source", "{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale))
        yield item_loader.load_item()