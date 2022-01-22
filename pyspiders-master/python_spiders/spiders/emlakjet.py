# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
import scrapy
import re
from ..loaders import ListingLoader
import json
from python_spiders.helper import string_found, remove_white_spaces

class EmlakjetSpider(scrapy.Spider):
    name = 'emlakjet'
    allowed_domains = ['emlakjet']
    start_urls = ['https://www.emlakjet.com/'] 
    execution_type = 'testing'
    country = 'turkey'
    locale ='tr'
    thousand_separator=','
    scale_separator='.'
     
    def start_requests(self):
        start_urls = [
            {'url': 'https://www.emlakjet.com/kiralik-konut/?gm_turu[]=2&gm_turu[]=204', 'property_type': 'house'},
            {'url':'https://www.emlakjet.com/kiralik-konut/?gm_turu[]=2&gm_turu[]=4&gm_turu[]=201&gm_turu[]=207&gm_turu[]=205&gm_turu[]=206&gm_turu[]=204&gm_turu[]=202&gm_turu[]=5&gm_turu[]=3','property_type': 'house'}
        ]
        for url in start_urls:
            yield scrapy.Request(
                url=url.get('url'),
                callback=self.parse, meta={'property_type': url.get('property_type')},
                dont_filter=True
            )
            
    def parse(self, response, **kwargs):        
        links = response.xpath('//a[@data-ej-label="link_item"]')
        for link in links: 
            url = response.urljoin(link.xpath('./@href').extract_first())
            yield scrapy.Request(url=url, callback=self.get_property_details, dont_filter=True, meta={'property_type': response.meta.get('property_type')})
        if response.xpath('//i[contains(text(), "navigate_next")]/../@href'):
            next_link = response.urljoin(response.xpath('//i[contains(text(), "navigate_next")]/../@href').extract_first())
            yield scrapy.Request(url=next_link, callback=self.parse, dont_filter=True, meta={'property_type': response.meta.get('property_type')})
    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        url=response.url
        if url and "satilik-" in url.lower():
            return 

        property_details_xpath = response.xpath('//script[@id="__NEXT_DATA__"]/text()').extract_first()
        try:
            property_details = json.loads(property_details_xpath)
        except:
            property_details = ''
        try:
            external_id = str(property_details['props']['initialProps']['pageProps']['pageResponse']['id'])
        except:
            external_id = ''
        if external_id and property_details:
            external_link = response.url
            images = property_details['props']['initialProps']['pageProps']['pageResponse']['images']
            title = property_details['props']['initialProps']['pageProps']['pageResponse']['title']
            rent_type_r = property_details['props']['initialProps']['pageProps']['pageResponse']['price']
            if 'GBP' in rent_type_r['currency']:
                rent_type = '£' 
            else:
                rent_type = rent_type_r['currency'] 
            if rent_type: 
                rent = str(rent_type_r['value']) + rent_type   
            else:
                rent = ''
            address = property_details['props']['initialProps']['pageProps']['pageResponse']['location']['city']['name']
            city= property_details['props']['initialProps']['pageProps']['pageResponse']['location']['city']['name']
            
            quickinfos = property_details['props']['initialProps']['pageProps']['pageResponse']['quickInfo']
            for quickinfo in quickinfos:
                if 'room_count' in quickinfo['key']:
                    room_count_list = quickinfo['value']
                elif 'gross_square' in quickinfo['key']:
                    square_meters = str(quickinfo['value'].replace('m2', ''))
            try:
                if '+' in room_count_list and 'Oda' not in room_count_list: 
                    room_count_value = room_count_list.split('+')
                    room_count = str(int(room_count_value[0]) + int(room_count_value[1]))
                    room_count = re.findall(r'\d+', room_count)[0]
                elif 'null' in room_count_list:
                    room_count = ''
                else:
                    room_count = ''
            except:
                room_count = ''
            lat = str(property_details['props']['initialProps']['pageProps']['pageResponse']['location']['coordinates']['lat'])
            lon = str(property_details['props']['initialProps']['pageProps']['pageResponse']['location']['coordinates']['lon'])
            description = ''.join(response.xpath('//div[contains(text(), "Açıklaması")]/following-sibling::div//p//text()').extract())
            property_type = response.meta.get('property_type')
            detail_texts = property_details['props']['initialProps']['pageProps']['pageResponse']['info']
            for details in detail_texts:
                if 'floor_count' in details['key']:
                    floor = details['value']
            landlord_phone = property_details['props']['initialProps']['pageProps']['pageResponse']['owner']['account']['phoneNumber']
            landlord_name = property_details['props']['initialProps']['pageProps']['pageResponse']['owner']['member']['name']
            bathroom_count = response.xpath("//div[contains(@class,'styles_tableColumn__')][contains(.,'Banyo Sayısı')]//following-sibling::div//text()").get()            
            furnished = response.xpath("//div[contains(@class,'styles_tableColumn__')][contains(.,'Eşya Durumu')]//following-sibling::div//text()[contains(.,'Eşyalı')]").get()  
            terrace = response.xpath("//div[contains(@class,'styles_feature__')][contains(.,'Teras')]/text()").get()
            balcony = response.xpath("//div[contains(@class,'styles_feature__')][contains(.,'Balkon')]/text()").get()

            if room_count and rent and remove_white_spaces(description):
                item_loader.add_value('property_type', property_type)
                item_loader.add_value('external_id', external_id)
                item_loader.add_value('external_link', external_link)
                item_loader.add_value('title', title)
                item_loader.add_value('address', address)
                item_loader.add_value('city', city)
                item_loader.add_value('description', description)
                item_loader.add_value('rent_string', rent)
                item_loader.add_value('images', images)
                item_loader.add_value('square_meters', square_meters)
                item_loader.add_value('floor', floor)
                if string_found(['otopark'], description):
                    item_loader.add_value('parking', True)
                if string_found(['balkon'], description):
                    item_loader.add_value('balcony', True)
                if string_found(['asansör'], description):
                    item_loader.add_value('elevator', True)
                if string_found(['terrasse', 'teras'], description):
                    item_loader.add_value('terrace', True)
                if lat: 
                    item_loader.add_value('latitude', lat)    
                if lon: 
                    item_loader.add_value('longitude', lon)
                if furnished:
                    item_loader.add_value("furnished", True)
                if balcony:
                    item_loader.add_value("balcony", True)
                if terrace:
                    item_loader.add_value("terrace", True)
                item_loader.add_value('room_count', room_count)
                if bathroom_count:
                    item_loader.add_value("bathroom_count", bathroom_count)
                item_loader.add_value('landlord_name', landlord_name)
                item_loader.add_value('landlord_email', 'info@emlakjet.com')
                item_loader.add_value('landlord_phone', landlord_phone)
                item_loader.add_value('external_source', 'Emlakjet_PySpider_turkey_tr')

                if not item_loader.get_collected_values("parking"):
                    if response.xpath("//div[contains(text(),'Otopark')]/text()").get(): item_loader.add_value("parking", True)
                
                if not item_loader.get_collected_values("bathroom_count"):
                    bathroom_count = response.xpath("//div[contains(text(),'Banyo Sayısı')]/following-sibling::div/text()").get()
                    if bathroom_count: 
                        item_loader.add_value("bathroom_count", "".join(filter(str.isnumeric, bathroom_count)))

                if not item_loader.get_collected_values("deposit"):
                    deposit = response.xpath("//div[contains(text(),'Depozito')]/following-sibling::div/text()").get()
                    if deposit: 
                        item_loader.add_value("deposit", "".join(filter(str.isnumeric, deposit)))

                yield item_loader.load_item()
