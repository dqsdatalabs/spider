# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
import scrapy
import re
from ..loaders import ListingLoader
from ..helper import format_date, remove_white_spaces  

class TheumbrellahomesSpider(scrapy.Spider):
    name = "theumbrellahomes"
    allowed_domains = ["theumbrellahomes.co.uk"]
    start_urls = (
        'http://www.theumbrellahomes.co.uk/',
    )
    execution_type = 'testing'
    country = 'united_kingdom'
    locale ='en'
    external_source = "Theumbrellahomes_PySpider_united_kingdom_en"
    thousand_separator=','
    scale_separator='.'

    def start_requests(self):
        start_urls = [
            {'url': 'https://www.theumbrellahomes.co.uk/lettings.php?pge=1&bedrooms=0&bathrooms=0&type=Flat', 'property_type': 'apartment'},
            {'url': 'https://www.theumbrellahomes.co.uk/lettings.php?pge=1&bedrooms=0&bathrooms=0&type=House', 'property_type': 'house'},
            {'url':'https://www.theumbrellahomes.co.uk/lettings.php?bedrooms=0&bathrooms=0','property_type': 'apartment'}
        ]

        for url in start_urls:
            yield scrapy.Request(
                url=url.get('url'),
                callback=self.parse, 
                meta={'property_type': url.get('property_type')},
                dont_filter=True
            )

    def parse(self, response, **kwargs):
        page_numbers = response.xpath('//span[contains(text(), "of")]/text()').extract_first('').strip().replace('of', '')
        pages = int(int(page_numbers) + 1)
        for page in range(1, pages):
            link = response.url.replace('pge=1', 'pge={}'.format(page))
            yield scrapy.Request(url=link, callback=self.get_url_details, dont_filter=True, meta={'property_type': response.meta.get('property_type')})
        
    def get_url_details(self, response):
        links = response.xpath('//div[contains(@class, "latest-property")]/a')
        for link in links: 
            url = response.urljoin(link.xpath('./@href').extract_first())
            yield scrapy.Request(url=url, callback=self.get_property_details, dont_filter=True, meta={'property_type': response.meta.get('property_type')})
    
    def get_property_details(self, response):
        # parse details of the propery
        property_type = response.meta.get('property_type')
        external_link = response.url
        address_list = response.xpath('//div[contains(@class, "sliderbox")]/h3/text()').extract()
        city = address_list[1].split(', ')[0]
        zipcode = address_list[1].split(', ')[1] 
        address = address_list[0] + ' ' + address_list[1]
        available_data_text = address_list[2].replace('Available', '')
        try:
            available_date = format_date(remove_white_spaces(available_data_text), '%d %B %Y')
        except:
            available_date = ''
        room_count_text = response.xpath('//li[contains(text(), "Bedroom")]/text()').extract_first('').strip()
        if room_count_text:
            try:
                room_count = re.findall(r'\d+', room_count_text)[0]    
            except:
                room_count = ''
        else:
            room_count = ''
        bathrooms_text = response.xpath('//li[contains(text(), "Bathroom")]/text()').extract_first('').strip() 
        if bathrooms_text:
            try:
                bathrooms = re.findall(r'\d+', bathrooms_text)[0]      
            except:
                bathrooms = ''
        else:
            bathrooms = ''
        lat_lon = re.search(r'new google\.maps\.LatLng\((.*?)\)', response.text).group(1)
        lat = lat_lon.split(',')[0]
        lon = lat_lon.split(',')[1]  
        rent_string = response.xpath('//h2[@class="thefeat"]/text()').extract_first('').strip()
        item_loader = ListingLoader(response=response)
        item_loader.add_value('property_type', property_type)
        item_loader.add_value('external_link', external_link)
        if available_date:
            item_loader.add_value('available_date', available_date)     
        item_loader.add_value('address', address)
        item_loader.add_value('city', city)
        item_loader.add_value('zipcode', zipcode)
        item_loader.add_xpath('description', '//div[@class="property-detail"]/p/text()')
        item_loader.add_xpath('title', "//div[contains(@class,'col-lg-9')]/h3//text()[.!='Share This Property']")
        item_loader.add_value('rent_string', rent_string)

        images = [x for x in response.xpath("//a[@class='rsImg']/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)

        item_loader.add_xpath('images', '//div[@id="slider"]//img/@src')
        if room_count:
            item_loader.add_value('room_count', str(room_count))
        item_loader.add_value('latitude', str(lat))
        item_loader.add_value('longitude', str(lon))
        if bathrooms: 
            item_loader.add_value('bathroom_count', str(bathrooms))
        item_loader.add_value('landlord_name', 'The Umbrella Homes')
        item_loader.add_value('landlord_email', 'info@theumbrellahomes.co.uk')
        item_loader.add_value('landlord_phone', '02920 230 338 ')
        item_loader.add_value("external_source", "{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale))
        yield item_loader.load_item()