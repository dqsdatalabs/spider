# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
import scrapy
import re
import dateparser
from ..loaders import ListingLoader
from python_spiders.helper import format_date, remove_white_spaces

class ColdwellbankerSpider(scrapy.Spider):
    name = "coldwellbanker"
    allowed_domains = ["coldwellbanker.co.uk"]
    start_urls = (
        'http://www.coldwellbanker.co.uk/',
    )
    execution_type = 'testing'
    country = 'united_kingdom'
    locale ='en'
    thousand_separator=','
    scale_separator='.'

    def start_requests(self):
        start_urls = [
            {'url': 'https://www.coldwellbanker.co.uk/?id=3693&action=view&route=search&view=list&input=SE1&jengo_radius=200&jengo_property_for=2&jengo_property_type=8&jengo_category=1&jengo_min_beds=0&jengo_max_beds=9999&jengo_min_price=0&jengo_max_price=99999999999&jengo_order=2&pfor_complete=on&pfor_offer=on&trueSearch=&searchType=postcode&latitude=&longitude=#total-results-wrapper', 'property_type': 'apartment'},
            {'url': 'https://www.coldwellbanker.co.uk/?id=3693&action=view&route=search&view=list&input=SE1&jengo_radius=200&jengo_property_for=2&jengo_property_type=13&jengo_category=1&jengo_min_beds=0&jengo_max_beds=9999&jengo_min_price=0&jengo_max_price=99999999999&jengo_order=2&pfor_complete=on&pfor_offer=on&trueSearch=&searchType=postcode&latitude=&longitude=#total-results-wrapper', 'property_type': 'apartment'}
        ]
        for url in start_urls:
            yield scrapy.Request(
                url=url.get('url'),
                callback=self.parse, 
                meta={'property_type': url.get('property_type')},
                dont_filter=True
            )
            
    def parse(self, response, **kwargs):
        links = response.xpath("//div[@class='row actual-property']/div/a")
        for link in links: 
            url = response.urljoin(link.xpath('./@href').extract_first())
            yield scrapy.Request(url=url, callback=self.get_property_details, dont_filter=True, meta={'property_type': response.meta.get('property_type')})
        if response.xpath('//a[contains(text(), "Next")]/@href'):
            next_link = response.urljoin(response.xpath('//a[contains(text(), "Next")]/@href').extract_first())
            yield scrapy.Request(url=next_link, callback=self.parse, dont_filter=True, meta={'property_type': response.meta.get('property_type')})
    def get_property_details(self, response):
        # parse details of the property
        property_type = response.meta.get('property_type')
        external_link = response.url
        details_text = response.xpath('//section[@class="details-page"]//p[1]/text()').extract()
        address = str(details_text[0])
        try:
            room_count = int(response.xpath('//h6[contains(text(), "Bedrooms")]/span/text()').extract_first('').strip())
        except:
            room_count = ''
        if ', ,' not in address and room_count: 
            city = address.split(', ')[-2]
            zipcode = address.split(', ')[-1] 
            rent_string = details_text[1]  
            bathrooms = response.xpath('//h6[contains(text(), "Bathrooms")]/span/text()').extract_first('').strip() 
            try:
                lat_lon_text = re.search(r'\[\"transport\"\]\s=\s\'(.*?)\}', response.text).group(1)
                lat = re.search(r'\"latitude\"\:(.*?)\,', lat_lon_text).group(1).replace('\"', '')
                lon = re.search(r'\"longitude\"\:(.*?)\,', lat_lon_text).group(1).replace('\"', '') 
            except:
                lat = ''
                lon = ''
            available_date = response.xpath('//h6[contains(text(), "Available")]/span/text()').extract_first('').strip()
            date_parsed = dateparser.parse( available_date, date_formats=["%d %B %Y"] ) 
            try:
                date2 = date_parsed.strftime("%Y-%m-%d")
            except:
                try:
                    date2 = format_date(remove_white_spaces(available_date), '%d %B %Y')
                except:
                    date2 = ''
            descriptions_text = ''.join(response.xpath('//div[@id="description-tab"]//text()').extract())
            descriptions = [x.lower() for x in descriptions_text] 
            list_texts_upper = response.xpath('//div[@id="features"]//text()').extract()
            furnished = ''
            swimming_pool = ''
            parking = ''
            terrace = ''
            dishwasher = ''
            list_texts = [y.lower() for y in list_texts_upper]  
            for list_text in list_texts:  
                if 'furnished' in list_text or 'furnished' in descriptions:
                    furnished = True
                if 'swimming pool' in list_text or 'swimming pool' in descriptions:
                    swimming_pool = True
                if 'parking' in list_text or 'parking' in descriptions:
                    parking = True
                if 'terrace' in list_text or 'terrace' in descriptions:
                    terrace	= True
                if 'dishwasher' in list_text or 'dishwasher' in descriptions:
                    dishwasher = True
            square_meters = ''
            floor = ''
            for dec in response.xpath('//div[@id="description-tab"]//text()').extract():
                if 'sq ft' in dec.lower():
                    try:
                        square_meters_tx = re.search(r'\s([\d|,|\.]+)\ssq', dec, re.S | re.M | re.I).group(1)
                        square_meters = float(square_meters_tx)/10.764
                    except:
                        square_meters = ''
                
                if 'floor' in dec.lower():
                    try:
                        floor = re.search(r'\s(\d{2})th floor', dec.lower()).group(1)
                    except:
                        floor = ''
            item_loader = ListingLoader(response=response)
            item_loader.add_value('property_type', property_type)
            item_loader.add_value('external_link', external_link)
            item_loader.add_value("external_id", external_link.split("&pid=")[1])
            item_loader.add_value('address', address)
            item_loader.add_xpath('title', '//title/text()')
            item_loader.add_value('city', city)
            item_loader.add_value('zipcode', zipcode)
            item_loader.add_xpath('description', '//div[@id="description-tab"]//text()')
            item_loader.add_value('rent_string', rent_string)
            item_loader.add_xpath('images', '//div[contains(@class, "fotorama")]//img/@src')
            if str(square_meters).strip():
                item_loader.add_value('square_meters', square_meters)
            if str(floor).strip():
                item_loader.add_value('floor', floor)
            if lat:
                item_loader.add_value('latitude', str(lat))
            if lon:
                item_loader.add_value('longitude', str(lon))
            pet_exce = response.xpath('//h6[contains(text(), "Pet")]/span/text()').extract_first('')
            if furnished:
                item_loader.add_value('furnished', True)
            if swimming_pool:
                item_loader.add_value('swimming_pool', True)
            if parking:
                item_loader.add_value('parking', True)
            if terrace:
                item_loader.add_value('terrace', True)
            if dishwasher:
                item_loader.add_value('dishwasher', True)    
            if pet_exce and 'n/a' not in pet_exce.lower():
                item_loader.add_value('pets_allowed', True) 
            if date2:
                item_loader.add_value('available_date', date2)
            if room_count:
                item_loader.add_value('room_count', str(room_count))
            if bathrooms: 
                item_loader.add_value('bathroom_count', str(bathrooms))
            item_loader.add_value('landlord_name', 'Coldwell Banker')
            item_loader.add_value('landlord_email', 'info@coldwellbanker.co.uk')
            item_loader.add_value('landlord_phone', '0203 333 1906')
            item_loader.add_value("external_source", "{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale))
            yield item_loader.load_item()