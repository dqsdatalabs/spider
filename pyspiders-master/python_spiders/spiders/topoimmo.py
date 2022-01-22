# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
import scrapy
import re
import json
from ..loaders import ListingLoader
import dateparser
from python_spiders.helper import remove_unicode_char, extract_rent_currency, format_date

def extract_city_zipcode(_address):
    zip_city = _address.split(", ")[1]
    zipcode = zip_city.split(" ")[0]
    city = re.findall(r'[A-Za-z]+', zip_city)[0]
    return zipcode, city

class TopoimmoSpider(scrapy.Spider):
    name = 'topoimmo'
    allowed_domains = ['topo-immo']
    start_urls = ['https://topo-immo.be/te-huur']
    execution_type = 'testing'
    country = 'belgium'
    locale ='nl'
    thousand_separator=','
    scale_separator='.'
    external_source = "Topoimmo_PySpider_belgium_nl"

    def start_requests(self):
        url = "https://topo-immo.be/api/estates"
        yield scrapy.Request(
                url=url,
                callback=self.parse,
                dont_filter=True
        )
    
    def parse(self, response, **kwargs):
        datas = json.loads(response.text)
        for data in datas['data']:
            property_url = data['url']
            
            if data["availability"] == "Te huur" and data["isOption"] == False:
                sub_category = data["subcategory"]
                if 'halfopen' in sub_category.lower():
                    property_type = 'house'
                elif 'appartement' in sub_category.lower():
                    property_type = 'apartment'
                else:
                    continue
                yield scrapy.Request(
                    url=property_url,
                    callback=self.get_property_details,
                    meta={'property_type': property_type},
                    dont_filter=True
                )     

    def get_property_details(self, response):
        external_link = response.url
        external_id = response.xpath('//div[@class="estate-grid__right"]//p[contains(text(), "REF")]/text()').extract_first().replace('REF:', '').replace(' ', '')
        title = ''.join(response.xpath('//div[@class="description__info"]//h1/text()').extract())
        property_type = response.meta.get('property_type')
        rent_text = response.xpath('//div[@class="estate-grid__right"]//h3/text()').extract_first('')
        if '€' in rent_text:
           rent = True
        else:
           rent = '' 
        address_text = response.xpath('//p[@class="description__address"]/text()').extract_first('')
        if address_text:
            address = address_text
        else:
            address = ''
        zipcode, city = extract_city_zipcode(address)
        bathrooms_count = response.xpath('//div[contains(@class,"building-type__table-item")]//p[contains(.,"Badkamers")]//following-sibling::p//text()').extract_first('').strip()
        lat = response.xpath('//div[@id="google-map"]/@data-latitude').extract_first('').strip()
        lon = response.xpath('//div[@id="google-map"]/@data-longitude').extract_first('').strip()
        room_count_text = response.xpath('//div[@class="characteristics__table-item"]//p[contains(text(), "Slaapkamers")]/following-sibling::p/text()').extract_first('')
        if room_count_text and int(room_count_text) > 0:
            room_count_text = True 
        else:
            room_count_text = ''
        square_meters_text = response.xpath('//p[contains(text(), "Woonoppervlak")]/following-sibling::p/text()').extract_first('')
        images = []
        image_links = response.xpath('//div[@class="estate-carousel-navigation"]//img')
        for image_link in image_links:
            image_url = image_link.xpath('./@src').extract_first()
            if image_url not in images: 
                images.append(image_url)
        date2 = ''
        available_date = response.xpath('//p[contains(text(), "Vrij")]/following-sibling::p/text()').extract_first('').strip()
        if available_date: 
            date_parsed = dateparser.parse( available_date, date_formats=["%d %B %Y"] ) 
            try:
                date2 = date_parsed.strftime("%Y-%m-%d")
            except:
                pass
        terrace_text = response.xpath('//p[contains(text(), "Terras")]/following-sibling::p/text()').extract_first('').strip()
        pets = response.xpath('//p[contains(text(), "Huisdieren")]/following-sibling::p/text()').extract_first('').strip()
        parking = response.xpath('//div[contains(@class,"extra-info__table-item")]//p[contains(.,"Parkeerplaatsen")]//following-sibling::p//text()').get()
        if rent: 
            item_loader = ListingLoader(response=response)
            item_loader.add_value('external_id', str(external_id))
            item_loader.add_value('property_type', property_type)
            item_loader.add_value('external_link', external_link)
            item_loader.add_value('title', title)
            item_loader.add_value('address', address)
            item_loader.add_xpath('rent_string', '//div[@class="estate-grid__right"]//h3/text()')
            item_loader.add_xpath('description', '//div[@class="description__description"]//text()')
            item_loader.add_xpath('square_meters', '//p[contains(text(), "Woonoppervlak")]/following-sibling::p/text()')
            item_loader.add_value('images', images)
            if lat:
                item_loader.add_value('latitude', str(lat))
            if lon:
                item_loader.add_value('longitude', str(lon))
            if bathrooms_count:
                item_loader.add_value('bathroom_count', str(bathrooms_count)) 
            if date2:
                item_loader.add_value('available_date', date2)
            if 'ja' in terrace_text.lower():
                item_loader.add_value('terrace', True) 
            if 'ja' in pets.lower():
                item_loader.add_value('pets_allowed', True)
            if parking:
                item_loader.add_value("parking", True)
            item_loader.add_xpath('room_count', '//div[@class="characteristics__table-item"]//p[contains(text(), "Slaapkamers")]/following-sibling::p/text()')
            item_loader.add_value('landlord_name', 'Topo Immo')
            item_loader.add_value('landlord_email', 'info@topo-immo.be')
            item_loader.add_value('landlord_phone', '+32 54 33 39 33')
            item_loader.add_value('zipcode', zipcode)
            item_loader.add_value('city', city)
            item_loader.add_value('external_source', self.external_source)
            yield item_loader.load_item()



         