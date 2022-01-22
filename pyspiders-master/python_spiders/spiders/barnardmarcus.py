# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
import scrapy
import re
import html 
from scrapy import FormRequest
from ..loaders import ListingLoader

class BarnardmarcusSpider(scrapy.Spider):
    name = "barnardmarcus"
    allowed_domains = ["barnardmarcus.co.uk"]
    start_urls = (
        'http://www.barnardmarcus.co.uk/',
    )
    execution_type = 'testing'
    country = 'united_kingdom'
    locale ='en'
    thousand_separator=','
    scale_separator='.'

    def start_requests(self):
        start_urls = [
            {'url': 'https://www.barnardmarcus.co.uk/property/results', 'property_type': 'apartment', 'property_type_site': 'Flat'},
            {'url': 'https://www.barnardmarcus.co.uk/property/results', 'property_type': 'house', 'property_type_site': 'House'}
        ]
        for url in start_urls:
            data = {
                'SearchParameters.SaleType': 'Let',
                'SearchParameters.Location': 'London',
                'SearchParameters.UrlToken': 'london',
                'SearchParameters.PropertyType': '{}'.format(url.get('property_type_site')),
                'SearchParameters.SearchRadius': '16093',
                'SearchParameters.SSTC': 'False',
                'SearchParameters.Premium': 'Include',
                'page': '1',
                'sort': 'Price',
                'sortDirection': 'Descending',
                'searchResultsViewMode': 'List',
                'itemsPerPage': '12',
                'X-Requested-With': 'XMLHttpRequest'
            }
            yield FormRequest(
                url=url.get('url'),
                formdata=data,
                callback=self.parse, 
                meta={'property_type': url.get('property_type'), 'property_type_site': url.get('property_type_site')},
                dont_filter=True
            )
    def parse(self, response, **kwargs):
        page_number_text = response.xpath('//h5/text()').extract_first()
        page_number = re.findall(r'\d+', page_number_text)[0]
        pages = int(int(page_number) / 12) + 1
        for page in range(1, pages):
            data = {
                'SearchParameters.SaleType': 'Let',
                'SearchParameters.Location': 'London',
                'SearchParameters.UrlToken': 'london',
                'SearchParameters.PropertyType': '{}'.format(response.meta.get('property_type_site')),
                'SearchParameters.SearchRadius': '16093',
                'SearchParameters.SSTC': 'False',
                'SearchParameters.Premium': 'Include',
                'page': '{}'.format(page),
                'sort': 'Price',
                'sortDirection': 'Descending',
                'searchResultsViewMode': 'List',
                'itemsPerPage': '12',
                'X-Requested-With': 'XMLHttpRequest'
            }
            yield FormRequest(
                url=response.url,
                formdata=data,
                callback=self.get_parse_details, 
                meta={'property_type': response.meta.get('property_type')},
                dont_filter=True
            )
    def get_parse_details(self, response):
        links = response.xpath('//div[@class="property-images"]//a')
        for link in links: 
            url = response.urljoin(link.xpath('./@href').extract_first())
            yield scrapy.Request(url=url, callback=self.get_property_details, dont_filter=True, meta={'property_type': response.meta.get('property_type')})
    def get_property_details(self, response):
        # parse details of the propery
        item_loader = ListingLoader(response=response)
        
        property_type = response.meta.get('property_type')
        external_link = response.url
        external_id = response.xpath('//span[contains(text(), "Reference")]/span/text()').extract_first().strip()
        street = response.xpath('//h2[@itemprop="streetAddress"]/text()').extract_first()
        city_zipcode = response.xpath('//small[@itemprop="addressLocality"]/strong/text()').extract_first()
        if street and city_zipcode: 
            address = street + ' ' + city_zipcode 
        elif city_zipcode:
            address = city_zipcode
        
        if city_zipcode and city_zipcode.strip():
            city_zipcode = city_zipcode.strip().strip(",")
            if "," in city_zipcode:
                city = city_zipcode.split(",")[-1].strip() 
                if city.isalpha():
                    item_loader.add_value("city", city) 
                else:
                    item_loader.add_value("city", city_zipcode.split(",")[0].strip())
            elif city_zipcode.replace(" ","").isalpha():
                item_loader.add_value("city", city_zipcode)
            
        room_count = response.xpath('//div[@title="Bedrooms"]/span/text()').extract_first('').strip()
        bathrooms = response.xpath('//div[@title="Bathrooms"]/span/text()').extract_first('').strip() 
        lat_lon_text = re.search(r'\"setViewForLocation\"\,([\s\S]+?)\)', response.text).group(1).strip().split(',')
        lon = lat_lon_text[0].strip()
        lat = lat_lon_text[1].strip()  
        rent_string = response.xpath('//span[@class="price"]/text()').extract_first('').strip()
        item_loader.add_value('property_type', property_type)
        item_loader.add_value('external_link', external_link)
        item_loader.add_xpath('title', '//title/text()')
        item_loader.add_value('external_id', external_id)
        item_loader.add_value('address', address)
        item_loader.add_xpath('description', '//p[@class="property-description"]/text()')
        item_loader.add_value('rent_string', rent_string)
        item_loader.add_xpath('images', '//div[@id="property-images-thumbnails"]//img/@src')
        item_loader.add_value('latitude', str(lat))
        item_loader.add_value('longitude', str(lon))
        if room_count:
            item_loader.add_value('room_count', str(room_count))
        if bathrooms: 
            item_loader.add_value('bathroom_count', str(bathrooms))
        item_loader.add_value('landlord_name', 'Barnard Marcus')
        landlord_phone = response.xpath("//a[@class='phone-link']/span/text()").extract_first() 
        if landlord_phone:
            item_loader.add_value('landlord_phone', landlord_phone.strip())
        landlord_email = response.xpath("//a[@class='branch-print-email']/span[@class='print-show']/text()").extract_first() 
        if landlord_email:
            item_loader.add_value('landlord_email', landlord_email.strip())
        furnished = response.xpath("//li[contains(.,'Furnished') or contains(.,'furnished')]/text()").extract_first()
        if furnished:
            if "unfurnished" in furnished.lower():
                item_loader.add_value('furnished', False)
            elif "furnished" in furnished.lower():
                item_loader.add_value('furnished', True)

        parking = response.xpath("//li[contains(.,'Parking') or contains(.,'Car Park')]/text()").extract_first()
        if parking:
            item_loader.add_value('parking', True)

        item_loader.add_value("external_source", "{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale))
        
        yield item_loader.load_item()